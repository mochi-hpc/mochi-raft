#include <mpi.h>
#include <abt.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <ssg.h>
#include <ssg-mpi.h>
#include <mochi-raft.h>
#include "memory-log.h"
#include "mochi-raft-test.h"

#define N_ENTRIES 16

#define margo_assert(__mid__, __expr__)                                      \
    do {                                                                     \
        if (!(__expr__)) {                                                   \
            margo_critical(__mid__,                                          \
                           "[test] assert failed at %s line %d: " #__expr__, \
                           __FILE__, __LINE__);                              \
            exit(-1);                                                        \
        }                                                                    \
    } while (0)

struct myfsm {
    char* content;
};

static int
myfsm_apply(struct raft_fsm* fsm, const struct raft_buffer* buf, void** result)
{
    struct myfsm* myfsm = (struct myfsm*)fsm->data;
    fprintf(stderr, "[test] Applying %s\n", (char*)buf->base);
    if (!myfsm->content) {
        myfsm->content = strdup((char*)buf->base);
    } else {
        size_t s       = strlen(myfsm->content) + strlen((char*)buf->base) + 1;
        myfsm->content = realloc(myfsm->content, s);
        strcat(myfsm->content, (char*)buf->base);
    }
    return 0;
}

static int myfsm_snapshot(struct raft_fsm*    fsm,
                          struct raft_buffer* bufs[],
                          unsigned*           n_bufs)
{
    struct myfsm* myfsm = (struct myfsm*)fsm->data;
    *bufs               = raft_malloc(sizeof(**bufs));
    *n_bufs             = 1;
    (*bufs)[0].base     = strdup(myfsm->content);
    (*bufs)[0].len      = strlen(myfsm->content) + 1;
    return 0;
}

static int myfsm_restore(struct raft_fsm* fsm, struct raft_buffer* buf)
{
    struct myfsm* myfsm = (struct myfsm*)fsm->data;
    free(myfsm->content);
    if (!buf || !buf->len) return 0;
    myfsm->content = strdup((char*)buf->base);
    return 0;
}

static void tracer_emit(struct raft_tracer* t,
                        const char*         file,
                        int                 line,
                        const char*         message)
{
    margo_instance_id mid = (margo_instance_id)t->impl;
    margo_debug(mid, "[craft] [%s:%d] %s", file, line, message);
}

static int check_log_values(struct mraft_log* log)
{
    struct raft_entry* entries = NULL;
    unsigned           n_entries;
    memory_log_get_entries(log, &entries, &n_entries);

    int N_PROCESSES = 1;
    int expected[N_PROCESSES][N_ENTRIES];

    for (unsigned i = 0; i < n_entries; i++) {
        size_t len = entries[i].buf.len;
        if (len != 5) continue;
        char* buf = (char*)entries[i].buf.base;
        char  rank;
        int   n;
        sscanf(buf, "%c%03d", &rank, &n);
        expected[rank - 'A'][n] = 1;
    }
    for (unsigned i = 0; i < N_PROCESSES; i++)
        for (unsigned j = 0; j < N_ENTRIES; j++)
            if (expected[i][j] == 0) return -1;

    fprintf(stderr, "[test] Log values recovered successfully\n");
    return 0;
}

struct error_recovery {
    struct raft*      raft;
    struct raft_io*   raft_io;
    margo_instance_id mid;
    raft_id           leader_id;
    ssg_member_id_t   self_id;
    ssg_member_id_t   transfer_to_id;
};

/* A ULT that kills and revives a process */
static void kill_and_revive_process_ult(void* arg)
{
    struct error_recovery* recovery       = (struct error_recovery*)arg;
    struct raft*           raft           = recovery->raft;
    struct raft_io*        raft_io        = recovery->raft_io;
    margo_instance_id      mid            = recovery->mid;
    uint64_t               leader_id      = recovery->leader_id;
    uint64_t               self_id        = recovery->self_id;
    ssg_member_id_t        transfer_to_id = recovery->transfer_to_id;

    /* Transfer leadership before killing process */
    if (self_id == leader_id) {
        fprintf(stderr, "[test] Transfering leader ship from %lu to %lu\n",
                leader_id, transfer_to_id);
        mraft_transfer(raft, transfer_to_id);
    }

    margo_thread_sleep(mid, 3000); /* Sleep for some time */
    mraft_io_simulate_dead(raft_io, true); /* Kill the process */
    fprintf(stderr, "[test] Process %lu has been disabled\n", self_id);

    margo_thread_sleep(mid, 2000);          /* Sleep for some time */
    mraft_io_simulate_dead(raft_io, false); /* Revive the process */
    fprintf(stderr, "[test] Process %lu has been re-enabled\n", self_id);
}

int main(int argc, char** argv)
{
    /* Initialize MPI */
    MPI_Init(&argc, &argv);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Initialize Margo */
    margo_instance_id mid = margo_init("na+sm", MARGO_SERVER_MODE, 1, 1);
    margo_assert(mid, mid);

    margo_set_log_level(mid, MARGO_LOG_TRACE);

    /* Initialize Argobots */
    ABT_init(0, NULL);

    /* Initialize SSG */
    int ret = ssg_init();
    margo_assert(mid, ret == SSG_SUCCESS);

    ssg_group_config_t config = {.swim_period_length_ms        = 1000,
                                 .swim_suspect_timeout_periods = 5,
                                 .swim_subgroup_member_count   = -1,
                                 .swim_disabled                = 1,
                                 .ssg_credential               = -1};

    ssg_group_id_t gid;
    ret = ssg_group_create_mpi(mid, "mygroup", MPI_COMM_WORLD, &config, NULL,
                               NULL, &gid);
    margo_assert(mid, ret == SSG_SUCCESS);

    /* Get this process' address and ID */
    ssg_member_id_t self_id;
    ssg_get_self_id(mid, &self_id);
    hg_addr_t self_hg_addr   = HG_ADDR_NULL;
    char      self_addr[256] = {0};
    hg_size_t self_addr_size = 256;
    margo_addr_self(mid, &self_hg_addr);
    margo_addr_to_string(mid, self_addr, &self_addr_size, self_hg_addr);
    margo_addr_free(mid, self_hg_addr);

    /* Initialize the state machine */
    struct myfsm    myfsm    = {0};
    struct raft_fsm raft_fsm = {.version           = 1,
                                .data              = &myfsm,
                                .apply             = myfsm_apply,
                                .snapshot          = myfsm_snapshot,
                                .restore           = myfsm_restore,
                                .snapshot_finalize = NULL,
                                .snapshot_async    = NULL};

    /* Initialize the log */
    struct mraft_log* log = memory_log_create();

    /* Initialize raft_io backend */
    struct mraft_io_init_args mraft_io_init_args
        = {.mid = mid, .pool = ABT_POOL_NULL, .id = 42, .log = log};
    struct raft_io raft_io;
    ret = mraft_io_init(&mraft_io_init_args, &raft_io);
    margo_assert(mid, ret == 0);

    /* Initialize RAFT */
    struct raft raft;
    ret = mraft_init(&raft, &raft_io, &raft_fsm, self_id, self_addr);
    margo_assert(mid, ret == 0);

    /* Initialize RAFT tracer */
    struct raft_tracer tracer
        = {.impl = (void*)mid, .enabled = true, .emit = tracer_emit};
    raft.tracer = &tracer;

    /* Bootstrap RAFT from SSG members */
    ret = mraft_bootstrap_from_ssg(&raft, gid);
    margo_assert(mid, ret == 0);
    fprintf(stderr, "============= Bootstrap done ============\n");
    /* Start RAFT */
    ret = mraft_start(&raft);
    margo_assert(mid, ret == 0);
    fprintf(stderr, "============= Start done ============\n");

    margo_thread_sleep(mid, 2000);

    fprintf(stderr, "============= Starting to work ============\n");
    raft_id     leader_id;
    const char* leader_addr;
    raft_leader(&raft, &leader_id, &leader_addr);
    margo_trace(mid, "[raft] Leader is %lu, state is %d\n", leader_id,
                raft_state(&raft));

    /* Start sending to the state machine */
    /* Only rank 0 sends commands */
    srand(rank + 1);
    if (rank == 0) {
        for (unsigned i = 0; i < N_ENTRIES; i++) {
            char c = 'A' + rank;
            char msg[5];
            sprintf(msg, "%c%03d", 'A' + rank, i);
            struct raft_buffer buf = {.base = msg, .len = 5};
            fprintf(stderr, "[test] Sending %s\n", msg);
            ret = mraft_apply(&raft, &buf, 1);
            margo_assert(mid, ret == 0);
            int delay_ms = random() % 500;
            margo_thread_sleep(mid, delay_ms);
        }
    }
    /* Only rank 1 gets disabled and enabled */
    if (rank == 1) {
        /* Create an execution stream and post a ULT to it */
        ABT_xstream xstream;
        ABT_xstream_create(ABT_SCHED_NULL, &xstream);
        ABT_pool pool;
        ABT_xstream_get_main_pools(xstream, 1, &pool);
        ABT_thread thread;

        /* Transfer leadership to process of rank 0 */
        ssg_member_id_t transfer_to_id;
        ssg_get_group_member_id_from_rank(gid, 0, &transfer_to_id);
        struct error_recovery recovery = {
            .raft           = &raft,
            .raft_io        = &raft_io,
            .mid            = mid,
            .leader_id      = leader_id,
            .self_id        = self_id,
            .transfer_to_id = transfer_to_id,
        };

        /* Post ULT to the execution stream */
        ABT_thread_create(pool, kill_and_revive_process_ult, &recovery,
                          ABT_THREAD_ATTR_NULL, &thread);

        /* Wait for the ULT to finish */
        ABT_thread_join(thread);
        ABT_thread_free(&thread);

        /* Finalize the execution stream */
        ABT_xstream_join(xstream);
        ABT_xstream_free(&xstream);
    }

    margo_thread_sleep(mid, 5000);
    MPI_Barrier(MPI_COMM_WORLD);

    /* Check that the process has the correct log despite being temporarily
     * disabled */
    if (rank == 1) {
        ret = check_log_values(log);
        margo_assert(mid, ret == 0);
    }

    /* Finalize RAFT */
    mraft_close(&raft);

    /* Finalize raft_io backend */
    ret = mraft_io_finalize(&raft_io);
    margo_assert(mid, ret == 0);

    /* Finalize the log */
    memory_log_free(log);

    /* Finalize the state machine */
    free(myfsm.content);

    /* Finalize SSG */
    ret = ssg_group_destroy(gid);
    margo_assert(mid, ret == SSG_SUCCESS);

    ret = ssg_finalize();
    margo_assert(mid, ret == SSG_SUCCESS);

    /* Finalize Argobots */
    ABT_finalize();

    /* Finalize Margo */
    margo_finalize(mid);

    /* Finalize MPI */
    MPI_Finalize();

    return 0;
}
