#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <ssg.h>
#include <mochi-raft.h>
#include <getopt.h>
#include "memory-log.h"

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

/**
 * @brief Parse the command line arguments and store them in the given pointers
 *
 * @param argc Number of arguments
 * @param argv Array of arguments
 * @param ssg_path Pointer to store the ssg path
 * @param join Pointer to store the join flag
 * @param raft_id Pointer to store the raft id
 * @return 0 on success, -1 on failure
 */
static int
parse_args(int argc, char** argv, char** ssg_path, bool* join, raft_id* raft_id)
{
    /* Define the long options */
    static const struct option long_options[]
        = {{"ssg-path", required_argument, NULL, 'p'},
           {"join", no_argument, NULL, 'j'},
           {"raft-id", required_argument, NULL, 'r'},
           {NULL, 0, NULL, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "p:jr:", long_options, NULL)) != -1) {
        switch (opt) {
        case 'p':
            *ssg_path = optarg;
            break;
        case 'j':
            *join = 1;
            break;
        case 'r':
            *raft_id = atoll(optarg);
            break;
        default:
            fprintf(stderr, "Unknown option '%d'\n", opt);
            return -1;
        }
    }
    if (*raft_id == 0) {
        fprintf(
            stderr,
            "Usage: %s --raft-id <raft-id> [--ssg-path ssg_path] [--join]\n",
            argv[0]);
        fprintf(stderr, "Please specify --raft-id option\n");
        return -1;
    }
    return 0;
}

struct membership_update_arg {
    struct raft*   raft;         /* The server's raft instance */
    raft_id        self_raft_id; /* The server's raft ID */
    char*          self_addr;    /* The server's address */
    ssg_group_id_t gid;          /* The SSG group ID */
};

/**
 * @brief The callback function to be invoked when a group membership update
 * occurs.
 *
 * @param group_data A user-defined pointer to the group data
 * @param member_id The id of the member that joined, left, or died
 * @param update_type The type of the membership update
 */
static void my_membership_update_cb(void*                    group_data,
                                    ssg_member_id_t          member_id,
                                    ssg_member_update_type_t update_type)
{
    fprintf(stderr, "[test] [debug] my_membership_update_cb\n");

    /* Get the raft structure and the self id from the group data */
    struct membership_update_arg* gd
        = (struct membership_update_arg*)group_data;
    struct raft*   raft         = gd->raft;
    raft_id        self_raft_id = gd->self_raft_id;
    char*          self_addr    = gd->self_addr;
    ssg_group_id_t gid          = gd->gid;

    /* Get the margo instance id from the raft structure */
    margo_instance_id mid = (margo_instance_id)raft->io->impl;
    int               ret;

    /* Get the current raft leader */
    raft_id     leader_id;
    const char* leader_addr;
    raft_leader(raft, &leader_id, &leader_addr);

    switch (update_type) {
    case SSG_MEMBER_JOINED:
        fprintf(stderr, "[test] [debug] member joined SSG group\n");
        /* The leader must add this new process to the raft cluster.
           He must get the joining process' raft ID */
        if (self_raft_id == leader_id) {
            /* Get the address of the joining server */
            fprintf(stderr, "[test] [debug] trying to get the joiner's addr\n");
            char* joiner_addr;
            ssg_get_group_member_addr_str(gid, member_id, &joiner_addr);

            /* Send RPC to ask for raft-id of joining server */
            fprintf(stderr,
                    "[test] [debug] trying to get the joiner's raft id\n");
            raft_id joiner_raft_id;
            ret = mraft_get_raft_id(raft, joiner_addr, &joiner_raft_id);
            margo_assert(mid, ret == 0);

            /* Add this process to the raft cluster */
            fprintf(stderr,
                    "[test] [debug] trying to add the joiner (id=%llu, "
                    "addr='%s') to the raft cluster\n",
                    joiner_raft_id, joiner_addr);
            ret = mraft_add(raft, joiner_raft_id, joiner_addr);
            margo_assert(mid, ret == 0);

            /* Change the role from raft_spare */
            fprintf(stderr, "[test] [debug] trying to change the role\n");
            ret = mraft_assign(raft, joiner_raft_id, RAFT_VOTER);
            margo_assert(mid, ret == 0);

            fprintf(stderr,
                    "[test] [debug] raft server(id=%llu, addr='%s') added\n",
                    joiner_raft_id, joiner_addr);
        }
        break;
    case SSG_MEMBER_LEFT:
        fprintf(stderr, "[test] [debug] member left SSG group\n");
        break;
    case SSG_MEMBER_DIED:
        fprintf(stderr, "[test] [debug] died and left SSG group\n");
        break;
    default:
        /* Unknown update type, ignore it */
        break;
    }
}

/**
 * @brief Create an load an ssg group into the file at `ssg_path`.
 * @param mid The margo instance ID
 * @param ssg_path The path to the group file
 * @param arg The argument to pass to the membership update callback function
 * for the ssg group
 * @return 0 upon success, -1 upon failure
 */
static int _create_ssg_group(margo_instance_id             mid,
                             const char*                   ssg_path,
                             struct membership_update_arg* arg)
{
    int            ret;
    ssg_group_id_t gid;

    /* Default configuration for SSG group */
    ssg_group_config_t config            = {.swim_period_length_ms        = 1000,
                                            .swim_suspect_timeout_periods = 5,
                                            .swim_subgroup_member_count   = -1,
                                            .swim_disabled                = 0,
                                            .ssg_credential               = -1};
    const char*        group_addr_strs[] = {arg->self_addr};

    /* We dont pass the callback yet, because we need to add the SSG group ID to
     * `arg` */
    ret = ssg_group_create(mid, "mygroup", group_addr_strs, 1, &config, NULL,
                           NULL, &gid);
    margo_assert(mid, ret == SSG_SUCCESS);

    /* Add callback now that we have the SSG group ID */
    arg->gid = gid;
    ssg_group_add_membership_update_callback(gid, my_membership_update_cb,
                                             (void*)arg);

    ret = ssg_group_id_store(ssg_path, gid, 1);
    margo_assert(mid, ret == SSG_SUCCESS);

    return 0;
}

/**
 * @brief Join the ssg group specified in the group file at ssg_path
 * @param mid The margo instance ID
 * @param ssg_path The path to the group file
 * @param arg The argument to pass to the membership update callback function
 * for the ssg group
 * @return 0 upon success, -1 upon failure
 */
static int _join_ssg_group(margo_instance_id             mid,
                           const char*                   ssg_path,
                           struct membership_update_arg* arg)
{
    int            ret;
    ssg_group_id_t gid;

    /* Load the group from the ssg file */
    int num_addrs = 1;
    ret           = ssg_group_id_load(ssg_path, &num_addrs, &gid);
    margo_assert(mid, ret == SSG_SUCCESS);

    /* Join the ssg group */
    arg->gid = gid;
    ret      = ssg_group_join(mid, gid, my_membership_update_cb, (void*)arg);
    margo_assert(mid, ret == SSG_SUCCESS);

    return 0;
}

static int check_log_values(raft_id self_raft_id, struct mraft_log* log)
{
    struct raft_entry* entries = NULL;
    unsigned           n_entries;
    memory_log_get_entries(log, &entries, &n_entries);

    // int N_PROCESSES = 1;
    // int expected[N_PROCESSES][N_ENTRIES];

    for (unsigned i = 0; i < n_entries; i++) {
        size_t len = entries[i].buf.len;
        if (len == 0) continue;
        char* buf = (char*)entries[i].buf.base;
        char  rank;
        int   n;
        printf("id=%llu, buf='%s'\n", self_raft_id, buf);
        // sscanf(buf, "%c%03d", &rank, &n);
        // expected[rank - 'A'][n] = 1;
    }
    // for (unsigned i = 0; i < N_PROCESSES; i++)
    //     for (unsigned j = 0; j < N_ENTRIES; j++)
    //         if (expected[i][j] == 0) return -1;

    fprintf(stderr, "[test] Log values recovered successfully\n");
    return 0;
}

int main(int argc, char** argv)
{
    /* Parse command line arguments */
    char* ssg_path = "group.ssg"; /* Default path to ssg group file */
    bool  join     = false;       /* Indicates whether process should create or
                                     join the ssg group */
    raft_id self_raft_id = 0; /* ID the server will use in the raft cluster */
    int     ret = parse_args(argc, argv, &ssg_path, &join, &self_raft_id);
    if (ret < 0) return -1;

    /* Initialize Margo */
    margo_instance_id mid = margo_init("tcp", MARGO_SERVER_MODE, 1, 1);
    margo_assert(mid, mid);

    margo_set_log_level(mid, MARGO_LOG_TRACE);

    /* Initialize SSG */
    ret = ssg_init();
    margo_assert(mid, ret == SSG_SUCCESS);

    /* Get this process' address */
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
    ret = mraft_init(&raft, &raft_io, &raft_fsm, self_raft_id, self_addr);
    margo_assert(mid, ret == 0);

    /* Initialize RAFT tracer */
    struct raft_tracer tracer
        = {.impl = (void*)mid, .enabled = true, .emit = tracer_emit};
    raft.tracer = &tracer;

    /* The creator of the group has to be the one to make the calls to
     * mraft_bootstrap */
    if (!join) {
        struct raft_configuration configuration;
        raft_configuration_init(&configuration);
        ret = raft_configuration_add(&configuration, self_raft_id, self_addr,
                                     RAFT_VOTER);
        margo_assert(mid, ret == 0);

        ret = mraft_bootstrap(&raft, &configuration);
        margo_assert(mid, ret == 0);

        raft_configuration_close(&configuration);
        fprintf(stderr, "============= Bootstrap done ============\n");
    }
    margo_thread_sleep(mid, 2000);

    /* Start RAFT. All processes need to call mraft_start */
    ret = mraft_start(&raft);
    margo_assert(mid, ret == 0);
    fprintf(stderr, "============= Start done ============\n");
    margo_thread_sleep(mid, 2000);

    /* Create or join the SSG group */
    struct membership_update_arg arg = {
        .raft         = &raft,
        .self_addr    = self_addr,
        .self_raft_id = self_raft_id,
        .gid          = 0, /* Not set yet */
    };
    if (join)
        _join_ssg_group(mid, ssg_path, &arg);
    else
        _create_ssg_group(mid, ssg_path, &arg);

    margo_thread_sleep(mid, 2000);

    fprintf(stderr, "============= Starting to work ============\n");
    raft_id     leader_id;
    const char* leader_addr;
    raft_leader(&raft, &leader_id, &leader_addr);
    margo_trace(mid, "[raft] Leader is %lu, state is %d\n", leader_id,
                raft_state(&raft));

    /* Start sending to the state machine */
    // srand(self_raft_id + 1);
    // if (self_raft_id == leader_id) {  /* Only leader sends to FSM */
    //     for (unsigned i = 0; i < 16; i++) {
    //         char msg[20 + 3 + 1 + 1]; /* 20 for self_raft_id (ULL), 3 for i
    //                                      (unsigned), 1 for '-' and 1 for '\0'
    //                                      */
    //         sprintf(msg, "%020llu-%03d", self_raft_id, i);
    //         struct raft_buffer buf = {.base = msg, .len = 25};
    //         fprintf(stderr, "[test] Sending %s\n", msg);
    //         ret = mraft_apply(&raft, &buf, 1);
    //         margo_assert(mid, ret == 0);
    //         int delay_ms = random() % 500;
    //         margo_thread_sleep(mid, delay_ms);
    //     }
    // }
    margo_thread_sleep(mid, 5000);

    /* Print all log values */
    check_log_values(self_raft_id, log);

    /* Finalize RAFT */
    mraft_close(&raft);

    /* Finalize raft_io backend */
    ret = mraft_io_finalize(&raft_io);
    margo_assert(mid, ret == 0);

    /* Finalize the log */
    memory_log_free(log);

    /* Finalize the state machine */
    free(myfsm.content);

    /* Leave the SSG group */
    ret = ssg_group_leave(arg.gid);
    margo_assert(mid, ret == SSG_SUCCESS);

    /* Finalize SSG */
    ret = ssg_group_destroy(arg.gid);
    margo_assert(mid, ret == SSG_SUCCESS);

    ret = ssg_finalize();
    margo_assert(mid, ret == SSG_SUCCESS);

    /* Finalize Margo */
    margo_finalize(mid);

    return 0;
}
