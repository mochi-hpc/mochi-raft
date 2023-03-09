#include <mpi.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <ssg.h>
#include <ssg-mpi.h>
#include <mochi-raft.h>
#include "memory-log.h"

struct myfsm {
    char* content;
};

static int myfsm_apply(struct raft_fsm *fsm,
                const struct raft_buffer* buf,
                void** result)
{
    struct myfsm* myfsm = (struct myfsm*)fsm->data;
    fprintf(stderr, "[test] Applying %s\n", (char*)buf->base);
    if(!myfsm->content) {
        myfsm->content = strdup((char*)buf->base);
    } else {
        size_t s = strlen(myfsm->content) + strlen((char*)buf->base) + 1;
        myfsm->content = realloc(myfsm->content, s);
        strcat(myfsm->content, (char*)buf->base);
    }
    return 0;
}

static int myfsm_snapshot(struct raft_fsm *fsm,
                          struct raft_buffer *bufs[],
                          unsigned *n_bufs)
{
    struct myfsm* myfsm = (struct myfsm*)fsm->data;
    *bufs = raft_malloc(sizeof(**bufs));
    *n_bufs = 1;
    (*bufs)[0].base = strdup(myfsm->content);
    (*bufs)[0].len = strlen(myfsm->content) + 1;
    return 0;
}

static int myfsm_restore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
    struct myfsm* myfsm = (struct myfsm*)fsm->data;
    free(myfsm->content);
    myfsm->content = strdup((char*)buf->base);
    return 0;
}

static void tracer_emit(struct raft_tracer *t,
                        const char *file,
                        int line,
                        const char *message)
{
    margo_instance_id mid = (margo_instance_id)t->impl;
    margo_trace(mid, "[trace] [%s:%d] %s", file, line, message);
}

int main(int argc, char** argv)
{
    /* Initialize MPI */
    MPI_Init(&argc, &argv);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Initialize Margo */
    margo_instance_id mid = margo_init("na+sm", MARGO_SERVER_MODE, 1, 1);
    assert(mid);

    margo_set_log_level(mid, MARGO_LOG_TRACE);

    /* Initialize SSG */
    int ret = ssg_init();
    assert(ret == SSG_SUCCESS);

    ssg_group_config_t config = {
        .swim_period_length_ms = 1000,
        .swim_suspect_timeout_periods = 5,
        .swim_subgroup_member_count = -1,
        .swim_disabled = 0,
        .ssg_credential = -1
    };

    ssg_group_id_t gid;
    ret = ssg_group_create_mpi(
            mid, "mygroup", MPI_COMM_WORLD,
            &config, NULL, NULL, &gid);
    assert(ret == SSG_SUCCESS);

    /* Get this process' address and ID */
    ssg_member_id_t self_id;
    ssg_get_self_id(mid, &self_id);
    hg_addr_t self_hg_addr = HG_ADDR_NULL;
    char self_addr[256] = {0};
    hg_size_t self_addr_size = 256;
    margo_addr_self(mid, &self_hg_addr);
    margo_addr_to_string(mid, self_addr, &self_addr_size, self_hg_addr);
    margo_addr_free(mid, self_hg_addr);

    /* Initialize the state machine */
    struct myfsm myfsm = {0};
    struct raft_fsm raft_fsm = {
        .version = 1,
        .data = &myfsm,
        .apply = myfsm_apply,
        .snapshot = myfsm_snapshot,
        .restore = myfsm_restore,
        .snapshot_finalize = NULL,
        .snapshot_async = NULL
    };

    /* Initialize the log */
    struct mraft_log* log = memory_log_create();

    /* Initialize raft_io backend */
    struct mraft_init_args mraft_init_args = {
        .mid  = mid,
        .pool = ABT_POOL_NULL,
        .id   = 42,
        .log  = log
    };
    struct raft_io raft_io;
    ret = mraft_init(&mraft_init_args, &raft_io);
    assert(ret == 0);

    /* Initialize RAFT */
    struct raft raft;
    ret = raft_init(&raft, &raft_io, &raft_fsm, self_id, self_addr);
    assert(ret == 0);

    /* Initialize RAFT tracer */
    struct raft_tracer tracer = {
        .impl = (void*)mid,
        .enabled = true,
        .emit = tracer_emit
    };
    raft.tracer = &tracer;

    /* Bootstrap RAFT from SSG members */
    struct raft_configuration conf = {0};
    raft_configuration_init(&conf);
    int group_size;
    ssg_get_group_size(gid, &group_size);
    for(unsigned i = 0; i < group_size; i++) {
        ssg_member_id_t member_id = 0;
        char* address = NULL;
        ssg_get_group_member_id_from_rank(gid, i, &member_id);
        ssg_get_group_member_addr_str(gid, member_id, &address);
        raft_configuration_add(&conf, member_id, address, RAFT_VOTER);
    }
    ret = raft_bootstrap(&raft,&conf);
    assert(ret == 0);
    raft_configuration_close(&conf);

    raft_set_pre_vote(&raft, true);

    /* Start RAFT */
    ret = raft_start(&raft);
    assert(ret == 0);

    /* Start sending to the state machine */
    srand(rank);
    for(unsigned i=0; i < 32; i++) {
        char c = 'A' + rank;
        char msg[5] = {c,c,c,c,'\0'};
        struct raft_buffer buf = {.base = msg, .len = 5};
        struct raft_apply apply = {0};
        fprintf(stderr, "[test] Sending %s\n", msg);
        ret = raft_apply(&raft, &apply, &buf, 1, NULL);
        assert(ret == 0);
        int delay_ms = random() % 500;
        margo_thread_sleep(mid, delay_ms);
    }

    /* Finalize RAFT */
    raft_close(&raft, NULL);

    /* Finalize raft_io backend */
    ret = mraft_finalize(&raft_io);
    assert(ret == 0);

    /* Finalize the log */
    memory_log_free(log);

    /* Finalize the state machine */
    free(myfsm.content);

    /* Finalize SSG */
    ret = ssg_group_destroy(gid);
    assert(ret == SSG_SUCCESS);

    ret = ssg_finalize();
    assert(ret == SSG_SUCCESS);

    /* Finalize Margo */
    margo_finalize(mid);

    /* Finalize MPI */
    MPI_Finalize();

    return 0;
}
