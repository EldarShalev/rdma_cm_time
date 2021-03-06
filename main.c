#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include "queue_internal.h"
#include <rdma/rdma_cma.h>
#include "threadpool.h"

static struct rdma_addrinfo hints;
static struct rdma_addrinfo *rai;
static struct rdma_event_channel *channel;

static char *port = "7471";
static char *dst_addr;
static char *src_addr;
static int timeout = 2000;
static int retries = 2;
threadpool thpool;
static int num_of_threads=1;
pthread_mutex_t tp_lock;


enum step {
    STEP_CREATE_ID,
    STEP_BIND,
    STEP_RESOLVE_ADDR,
    STEP_RESOLVE_ROUTE,
    STEP_CREATE_QP,
    STEP_CONNECT,
    STEP_DISCONNECT,
    STEP_DESTROY,
    STEP_CNT
};

static char *step_str[] = {
        "create id",
        "bind addr",
        "resolve addr",
        "resolve route",
        "create qp",
        "connect",
        "disconnect",
        "destroy"
};

struct node {
    struct rdma_cm_id *id;
    struct timeval times[STEP_CNT][2];
    int error;
    int retries;
};

struct list_head {
    struct list_head	*prev;
    struct list_head	*next;
    struct rdma_cm_id	*id;
};

struct work_list {
    pthread_mutex_t		lock;
    pthread_cond_t		cond;
    struct list_head	list;
};

#define INIT_LIST(x) ((x)->prev = (x)->next = (x))

static struct work_list req_work;
static struct work_list disc_work;
static struct node *nodes;


// Structs for each queue
static struct queue_s *resolve_route_queue;
static struct queue_s *qp_creation_queue;
static struct queue_s *connection_queue;
static struct queue_s *disconnection_queue;
pthread_mutex_t lock;



static struct timeval times[STEP_CNT][2];
static int connections = 10000;
static volatile int started[STEP_CNT];
static volatile int completed[STEP_CNT];
static struct ibv_qp_init_attr init_qp_attr;
static struct rdma_conn_param conn_param;

#define start_perf(n, s)	gettimeofday(&((n)->times[s][0]), NULL)
#define end_perf(n, s)		gettimeofday(&((n)->times[s][1]), NULL)
#define start_time(s)		gettimeofday(&times[s][0], NULL)
#define end_time(s)		gettimeofday(&times[s][1], NULL)

static inline void __list_delete(struct list_head *list)
{
    struct list_head *prev, *next;
    prev = list->prev;
    next = list->next;
    prev->next = next;
    next->prev = prev;
    INIT_LIST(list);
}

static inline int __list_empty(struct work_list *list)
{
    return list->list.next == &list->list;
}

static inline struct list_head *__list_remove_head(struct work_list *work_list)
{
    struct list_head *list_item;

    list_item = work_list->list.next;
    __list_delete(list_item);
    return list_item;
}
static inline struct list_head *list_remove_head(struct work_list *work_list)
{
    struct list_head *list_item;
    pthread_mutex_lock(&work_list->lock);
    list_item = __list_remove_head(work_list);
    pthread_mutex_unlock(&work_list->lock);
    return list_item;
}

static inline void list_add_tail(struct work_list *work_list, struct list_head *req)
{
    int empty;
    pthread_mutex_lock(&work_list->lock);
    empty = __list_empty(work_list);
    req->prev = work_list->list.prev;
    req->next = &work_list->list;
    req->prev->next = work_list->list.prev = req;
    pthread_mutex_unlock(&work_list->lock);
    if (empty)
        pthread_cond_signal(&work_list->cond);
}

static int zero_time(struct timeval *t)
{
    return !(t->tv_sec || t->tv_usec);
}

static float diff_us(struct timeval *end, struct timeval *start)
{
    return (end->tv_sec - start->tv_sec) * 1000000. + (end->tv_usec - start->tv_usec);
}

static void show_perf(void)
{
    int c, i;
    float us, max[STEP_CNT], min[STEP_CNT];

    for (i = 0; i < STEP_CNT; i++) {
        max[i] = 0;
        min[i] = 999999999.;
        for (c = 0; c < connections; c++) {
            if (!zero_time(&nodes[c].times[i][0]) &&
                !zero_time(&nodes[c].times[i][1])) {
                us = diff_us(&nodes[c].times[i][1], &nodes[c].times[i][0]);
                if (us > max[i])
                    max[i] = us;
                if (us < min[i])
                    min[i] = us;
            }
        }
    }

    printf("step              total ms     max ms     min us  us / conn\n");
    for (i = 0; i < STEP_CNT; i++) {
        if (i == STEP_BIND && !src_addr)
            continue;

        us = diff_us(&times[i][1], &times[i][0]);
        printf("%-13s: %11.2f%11.2f%11.2f%11.2f\n", step_str[i], us / 1000.,
               max[i] / 1000., min[i], us / connections);
    }
}

static void addr_handler(struct node *n)
{
    end_perf(n, STEP_RESOLVE_ADDR);
    completed[STEP_RESOLVE_ADDR]++;
}

static void route_handler(struct node *n)
{
    end_perf(n, STEP_RESOLVE_ROUTE);
    completed[STEP_RESOLVE_ROUTE]++;
}

static void conn_handler(struct node *n)
{
    end_perf(n, STEP_CONNECT);
    completed[STEP_CONNECT]++;
}

static void disc_handler(struct node *n)
{
    end_perf(n, STEP_DISCONNECT);
    completed[STEP_DISCONNECT]++;
}

static void __req_handler(struct rdma_cm_id *id)
{
    int ret;

    ret = rdma_create_qp(id, NULL, &init_qp_attr);
    if (ret) {
        perror("failure creating qp");
        goto err;
    }

    ret = rdma_accept(id, NULL);
    if (ret) {
        perror("failure accepting");
        goto err;
    }
    return;

    err:
    printf("failing connection request\n");
    rdma_reject(id, NULL, 0);
    rdma_destroy_id(id);
    return;
}


static void *req_handler_thread(void *arg)
{
    struct list_head *work;
    do {
        pthread_mutex_lock(&req_work.lock);
        if (__list_empty(&req_work))
            pthread_cond_wait(&req_work.cond, &req_work.lock);
        work = __list_remove_head(&req_work);
        pthread_mutex_unlock(&req_work.lock);
        __req_handler(work->id);
        free(work);
    } while (1);
    return NULL;
}

static void *disc_handler_thread(void *arg)
{
    struct list_head *work;
    do {
        pthread_mutex_lock(&disc_work.lock);
        if (__list_empty(&disc_work))
            pthread_cond_wait(&disc_work.cond, &disc_work.lock);
        work = __list_remove_head(&disc_work);
        pthread_mutex_unlock(&disc_work.lock);
        rdma_disconnect(work->id);
        rdma_destroy_id(work->id);
        free(work);
    } while (1);
    return NULL;
}

static void cma_handler(struct rdma_cm_id *id, struct rdma_cm_event *event)
{
    struct node *n = id->context;
    struct list_head *request;

    switch (event->event) {
        case RDMA_CM_EVENT_ADDR_RESOLVED:
            addr_handler(n);
            break;
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            route_handler(n);
            break;
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            request = malloc(sizeof *request);
            if (!request) {
                perror("out of memory accepting connect request");
                rdma_reject(id, NULL, 0);
                rdma_destroy_id(id);
            } else {
                INIT_LIST(request);
                request->id = id;
                list_add_tail(&req_work, request);
            }
            break;
        case RDMA_CM_EVENT_ESTABLISHED:
            if (n)
                conn_handler(n);
            break;
        case RDMA_CM_EVENT_ADDR_ERROR:
            if (n->retries--) {
                if (!rdma_resolve_addr(n->id, rai->ai_src_addr,
                                       rai->ai_dst_addr, timeout))
                    break;
            }
            printf("RDMA_CM_EVENT_ADDR_ERROR, error: %d\n", event->status);
            addr_handler(n);
            n->error = 1;
            break;
        case RDMA_CM_EVENT_ROUTE_ERROR:
            if (n->retries--) {
                if (!rdma_resolve_route(n->id, timeout))
                    break;
            }
            printf("RDMA_CM_EVENT_ROUTE_ERROR, error: %d\n", event->status);
            route_handler(n);
            n->error = 1;
            break;
        case RDMA_CM_EVENT_CONNECT_ERROR:
        case RDMA_CM_EVENT_UNREACHABLE:
        case RDMA_CM_EVENT_REJECTED:
            printf("event: %s, error: %d\n",
                   rdma_event_str(event->event), event->status);
            conn_handler(n);
            n->error = 1;
            break;
        case RDMA_CM_EVENT_DISCONNECTED:
            if (!n) {
                request = malloc(sizeof *request);
                if (!request) {
                    perror("out of memory queueing disconnect request, handling synchronously");
                    rdma_disconnect(id);
                    rdma_destroy_id(id);
                } else {
                    INIT_LIST(request);
                    request->id = id;
                    list_add_tail(&disc_work, request);
                }
            } else
                disc_handler(n);
            break;
        case RDMA_CM_EVENT_DEVICE_REMOVAL:
            /* Cleanup will occur after test completes. */
            break;
        default:
            break;
    }
    rdma_ack_cm_event(event);
}


static int alloc_nodes(void)
{
    int ret, i;
    if (pthread_mutex_init(&lock, NULL) != 0)
    {
        printf("\n mutex init has failed\n");
        return 1;
    }
    if (pthread_mutex_init(&tp_lock, NULL)!=0){
        printf("\n thread pool mutex init has failed\n");
        return 1;
    }

    nodes = calloc(sizeof *nodes, connections);
    if (!nodes)
        return -ENOMEM;


    // Creating all blocking queues
    resolve_route_queue=queue_create();
    qp_creation_queue=queue_create();
    connection_queue=queue_create();
    disconnection_queue=queue_create();

    printf("creating id\n");
    start_time(STEP_CREATE_ID);
    for (i = 0; i < connections; i++) {
        start_perf(&nodes[i], STEP_CREATE_ID);
        if (dst_addr) {
            ret = rdma_create_id(channel, &nodes[i].id, &nodes[i],
                                 hints.ai_port_space);
            if (ret)
                goto err;
        }
        end_perf(&nodes[i], STEP_CREATE_ID);
    }
    end_time(STEP_CREATE_ID);
    return 0;

    err:
    while (--i >= 0)
        rdma_destroy_id(nodes[i].id);
    free(nodes);
    return ret;
}


static void cleanup_nodes(void)
{
    printf("destroying id\n");
    pthread_mutex_destroy(&lock);
    pthread_mutex_destroy(&tp_lock);
    queue_destroy(resolve_route_queue);
    queue_destroy(qp_creation_queue);
    queue_destroy(connection_queue);
    queue_destroy(disconnection_queue);
    // Should be 0 threads working by now..
    printf("Working threads: %d\n", thpool_num_threads_working(thpool));
    thpool_destroy(thpool);

//    int i;
//    start_time(STEP_DESTROY);
//    for (i = 1; i < connections; i++) {
//        start_perf(&nodes[i], STEP_DESTROY);
//        if (nodes[i].id) {
//            printf("BUG: %d\n", nodes[i].id);
//           // rdma_destroy_id(nodes[i].id);
//        }
//        end_perf(&nodes[i], STEP_DESTROY);
//    }
//    // TODO bug in place [0](?)
//    //printf("Special BUG: %d\n", nodes[0].id);
//    //rdma_destroy_id(nodes[0].id);
//    end_time(STEP_DESTROY);

}

static void *process_events(void *arg)
{
    struct rdma_cm_event *event;
    int ret = 0;

    while (!ret) {
        ret = rdma_get_cm_event(channel, &event);
        if (!ret) {
            cma_handler(event->id, event);
        } else {
            perror("failure in rdma_get_cm_event in process_server_events");
            ret = errno;
        }
    }
    return NULL;
}

static void *resolving_addresses(void *arg) {
    printf("Thread \"resolving address\" has started \n");
    start_time(STEP_RESOLVE_ADDR);
    int ret = 0;
    int i=0;

    while (i < connections) {
        if (nodes[i].error)
            continue;
        nodes[i].retries = retries;
        start_perf(&nodes[i], STEP_RESOLVE_ADDR);

        //#printf("trying to resolve address for id %d\n" ,nodes[i].id);
        ret = rdma_resolve_addr(nodes[i].id, rai->ai_src_addr,
                                    rai->ai_dst_addr, timeout);
        if (ret) {
            perror("failure getting addr\n");
            nodes[i].error = 1;
            continue;
        }
        nodes[i].retries = retries;
        queue_put(resolve_route_queue,&nodes[i]);
        //#printf("address was put into resolve_route_queue! id: %d\n",nodes[i].id);
        started[STEP_RESOLVE_ADDR]++;
        i++;


    }
    // TODO - busy waiting..
    while (started[STEP_RESOLVE_ADDR] != completed[STEP_RESOLVE_ADDR]) sched_yield();
    end_time(STEP_RESOLVE_ADDR);
    return NULL;

}

static void *resolving_route(void *arg) {
    sleep(1);
    printf("Thread \"resolving route\" has started \n");
    start_time(STEP_RESOLVE_ROUTE);
    int ret = 0;
    int j=0;
    struct node *current= calloc(sizeof *nodes, 1);
    while (j < connections) {
        ret=queue_get_wait(resolve_route_queue,(void **)&current);
        if (ret) {
            perror("failure get from resolve route queue\n");
            nodes[j].error = 1;
            continue;
        }
        //#printf("resolve_address was dequeue from resolve_route_queue!\n");
        nodes[j]=*current;
        if (nodes[j].error)
            continue;
        nodes[j].retries = retries;
        start_perf(&nodes[j], STEP_RESOLVE_ROUTE);
        //$printf("trying to resolve route for id %d\n", nodes[j].id);

        ret = rdma_resolve_route(nodes[j].id, timeout);
        if (ret) {
            perror("failure resolving route");
            nodes[j].error = 1;
            continue;
        }
        queue_put(qp_creation_queue,&nodes[j]);
        started[STEP_RESOLVE_ROUTE]++;
        j++;

    }
    // TODO - sched_yield() can cause busy waiting..
    while (started[STEP_RESOLVE_ROUTE] != completed[STEP_RESOLVE_ROUTE]) sched_yield();
    end_time(STEP_RESOLVE_ROUTE);
    // TODO free here
    //free(current);
    return NULL;

}

static void *qp_creation(void *arg) {

    sleep(1);
    start_time(STEP_CREATE_QP);
    printf("Thread \"qp creation\" has started \n");
    int ret = 0;
    int j=0;
    struct node *current= calloc(sizeof *nodes, 1);
    while (j < connections) {
        ret=queue_get_wait(qp_creation_queue,(void **)&current);
        if (ret) {
            perror("failure get element from qp creation queue\n");
            nodes[j].error = 1;
            continue;
        }
        //#printf("route was dequeue from qp_creation queue!\n");
        nodes[j]=*current;
        if (nodes[j].error)
            continue;
        start_perf(&nodes[j], STEP_CREATE_QP);
        //$printf("trying to create_qp for id %d\n", nodes[j].id);
        ret = rdma_create_qp(nodes[j].id, NULL, &init_qp_attr);
        if (ret) {
            perror("failure creating qp");
            nodes[j].error = 1;
            continue;
        }
        queue_put(connection_queue,&nodes[j]);
        j++;
        end_perf(&nodes[j], STEP_CREATE_QP);

    }

    end_time(STEP_CREATE_QP);
    //TODO free current...
    //free(current);
    return NULL;
}

static void *connection(void *arg) {
    sleep(1);
    printf("Thread \"connection\" has started \n");
    start_time(STEP_CONNECT);
    int ret = 0;
    int j=0;
    struct node *current= calloc(sizeof *nodes, 1);
    while (j < connections) {
        ret=queue_get_wait(connection_queue,(void **)&current);
        if (ret) {
            perror("failure get element from connection_queue\n");
            nodes[j].error = 1;
            continue;
        }
        //#printf("qp was dequeue from connection_queue!\n");
        nodes[j]=*current;
        if (nodes[j].error)
            continue;
        start_perf(&nodes[j], STEP_CONNECT);
        //#printf("trying to connect for id %d\n", nodes[j].id);
        ret = rdma_connect(nodes[j].id, &conn_param);
        if (ret) {
            perror("failure connecting");
            nodes[j].error = 1;
            continue;
        }
        started[STEP_CONNECT]++;


        queue_put(disconnection_queue,&nodes[j]);
        j++;

    }
    // TODO - busy waiting..
    while (started[STEP_CONNECT] != completed[STEP_CONNECT]){
        sched_yield();
    }
    end_time(STEP_CONNECT);
    // TODO free here
    //free(current);
    return NULL;
}

static void *disconnection(void *arg) {
    //#printf("Thread #%u working on task1\n", (int)pthread_self());
    int ret = 0;
    int j=0;
    struct node *current= calloc(sizeof *nodes, 1);
    ret=queue_get_wait(disconnection_queue,(void **)&current);
    if (ret) {
        perror("failure get element from disconnection_queue\n");
        nodes[j].error = 1;
        return NULL;
    }
    //#printf("connection was dequeue from disconnection_queue!\n");
    pthread_mutex_lock(&tp_lock);
    nodes[j]=*current;

    if (nodes[j].error)
        return NULL;
    start_perf(&nodes[j], STEP_DISCONNECT);
    //#printf("trying to disconnect for id: %d\n", nodes[j].id);
    rdma_disconnect(nodes[j].id);
    rdma_destroy_qp(nodes[j].id);
    //#printf("trying to destroy id: %d\n", nodes[j].id);
    rdma_destroy_id(nodes[j].id);
    pthread_mutex_unlock(&tp_lock);
    started[STEP_DISCONNECT]++;
    //j++;



//    // TODO - busy waiting..
//    while (started[STEP_DISCONNECT] != completed[STEP_DISCONNECT]) {
//        sched_yield();
//    }
//    end_time(STEP_DISCONNECT);
////    // TODO free here
//    //free(current);
    //return NULL;

}


int get_rdma_addr(char *src, char *dst, char *port,
                  struct rdma_addrinfo *hints, struct rdma_addrinfo **rai)
{
    struct rdma_addrinfo rai_hints, *res;
    int ret;

    if (hints->ai_flags & RAI_PASSIVE)
        return rdma_getaddrinfo(src, port, hints, rai);

    rai_hints = *hints;
    if (src) {
        rai_hints.ai_flags |= RAI_PASSIVE;
        ret = rdma_getaddrinfo(src, NULL, &rai_hints, &res);
        if (ret)
            return ret;

        rai_hints.ai_src_addr = res->ai_src_addr;
        rai_hints.ai_src_len = res->ai_src_len;
        rai_hints.ai_flags &= ~RAI_PASSIVE;
    }

    ret = rdma_getaddrinfo(dst, port, &rai_hints, rai);
    if (src)
        rdma_freeaddrinfo(res);

    return ret;
}

static int run_server(void)
{
    pthread_t req_thread, disc_thread;
    struct rdma_cm_id *listen_id;
    int ret;

    INIT_LIST(&req_work.list);
    INIT_LIST(&disc_work.list);
    ret = pthread_mutex_init(&req_work.lock, NULL);
    if (ret) {
        perror("initializing mutex for req work");
        return ret;
    }

    ret = pthread_mutex_init(&disc_work.lock, NULL);
    if (ret) {
        perror("initializing mutex for disc work");
        return ret;
    }

    ret = pthread_cond_init(&req_work.cond, NULL);
    if (ret) {
        perror("initializing cond for req work");
        return ret;
    }

    ret = pthread_cond_init(&disc_work.cond, NULL);
    if (ret) {
        perror("initializing cond for disc work");
        return ret;
    }
    ret = pthread_create(&req_thread, NULL, req_handler_thread, NULL);
    if (ret) {
        perror("failed to create req handler thread");
        return ret;
    }

    ret = pthread_create(&disc_thread, NULL, disc_handler_thread, NULL);
    if (ret) {
        perror("failed to create disconnect handler thread");
        return ret;
    }

    ret = rdma_create_id(channel, &listen_id, NULL, hints.ai_port_space);
    if (ret) {
        perror("listen request failed");
        return ret;
    }


    ret = get_rdma_addr(src_addr, dst_addr, port, &hints, &rai);
    if (ret) {
        perror("getrdmaaddr error");
        goto out;
    }

    ret = rdma_bind_addr(listen_id, rai->ai_src_addr);
    if (ret) {
        perror("bind address failed");
        goto out;
    }

    ret = rdma_listen(listen_id, 0);
    if (ret) {
        perror("failure trying to listen");
        goto out;
    }

    process_events(NULL);
    out:
    rdma_destroy_id(listen_id);
    return ret;
}




int Run_Threads(){
    pthread_t event_thread;
    pthread_t binding_thread;
    pthread_t resolving_addr_thread;
    pthread_t resolving_route_thread;
    pthread_t create_qp_thread;
    pthread_t connecting_thread;
    pthread_t disconnecting_thread;

    int i, ret;

    ret = get_rdma_addr(src_addr, dst_addr, port, &hints, &rai);
    if (ret) {
        perror("getaddrinfo error");
        return ret;
    }

    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;
    conn_param.retry_count = retries;
    conn_param.private_data = rai->ai_connect;
    conn_param.private_data_len = rai->ai_connect_len;


    ret = pthread_create(&event_thread, NULL, process_events, NULL);
    if (ret) {
        perror("failure creating event thread");
        return ret;
    }
    // TODO Add thread for binding
    if (src_addr) {
        printf("binding source address\n");
        start_time(STEP_BIND);
        for (i = 0; i < connections; i++) {
            start_perf(&nodes[i], STEP_BIND);
            ret = rdma_bind_addr(nodes[i].id, rai->ai_src_addr);
            if (ret) {
                perror("failure bind addr");
                nodes[i].error = 1;
                continue;
            }
            end_perf(&nodes[i], STEP_BIND);
        }
        end_time(STEP_BIND);
    }

    ret=pthread_create(&resolving_addr_thread,NULL,resolving_addresses,NULL);
    if (ret) {
        perror("failure resolving address");
        return ret;
    }


    ret=pthread_create(&resolving_route_thread,NULL,resolving_route,NULL);
    if (ret) {
        perror("failure resolving route thread");
        return ret;
    }



    ret=pthread_create(&create_qp_thread,NULL,qp_creation,NULL);
    if (ret) {
        perror("failure creating qp thread");
        return ret;
    }



    ret=pthread_create(&connecting_thread,NULL,connection,NULL);
    if (ret) {
        perror("failure creating connection thread");
        return ret;
    }


    pthread_join(resolving_addr_thread,NULL);
    printf("Thread \"resolve_address has\" has finished\n");
    pthread_join(resolving_route_thread,NULL);
    printf("Thread \"resolve_route\" has finished\n");
    pthread_join(create_qp_thread,NULL);
    printf("Thread \"create qp\" has finished\n");
    pthread_join(connecting_thread,NULL);
    printf("Thread \"connection\" has finished\n");


    // Disconnection part with threadpool
    start_time(STEP_DISCONNECT);
    int j;
    for (j=0;j<connections;j++){
        thpool_add_work(thpool,(void*)&disconnection,NULL);
    }

    thpool_wait(thpool);
    printf("Thread \"disconnection\" has finished\n");
    end_time(STEP_DISCONNECT);



//    if (ret) {
//        perror("failure creating disconnecting thread");
//        return ret;
//    }
    //pthread_join(disconnecting_thread,NULL);
    //printf("Thread \"disconnection\" has finished\n");

    return 1;

}

int main(int argc, char **argv) {
    int op, ret;

    hints.ai_port_space = RDMA_PS_TCP;
    hints.ai_qp_type = IBV_QPT_RC;
    while ((op = getopt(argc, argv, "s:b:c:p:r:t:n:")) != -1) {
        switch (op) {
            case 's':
                dst_addr = optarg;
                break;
            case 'b':
                src_addr = optarg;
                break;
            case 'c':
                connections = atoi(optarg);
                break;
            case 'p':
                port = optarg;
                break;
            case 'r':
                retries = atoi(optarg);
                break;
            case 't':
                timeout = atoi(optarg);
                break;
            case 'n':
                num_of_threads=atoi(optarg);
                break;

            default:
                printf("usage: %s\n", argv[0]);
                printf("\t[-s server_address]\n");
                printf("\t[-b bind_address]\n");
                printf("\t[-c connections]\n");
                printf("\t[-p port_number]\n");
                printf("\t[-r num of retries]\n");
                printf("\t[-t timeout_ms]\n");
                printf("\t[-n num_of_threads]\n");
                exit(1);
        }
    }

    init_qp_attr.cap.max_send_wr = 1;
    init_qp_attr.cap.max_recv_wr = 1;
    init_qp_attr.cap.max_send_sge = 1;
    init_qp_attr.cap.max_recv_sge = 1;
    init_qp_attr.qp_type = IBV_QPT_RC;
    channel = rdma_create_event_channel();
    if (!channel) {
        printf("failed to create event channel\n");
        exit(1);
    }

    if (num_of_threads){
        thpool = thpool_init(num_of_threads);
    }

    if (dst_addr) {
        alloc_nodes();
        ret=Run_Threads();
    } else {
        hints.ai_flags |= RAI_PASSIVE;
        ret = run_server();
    }


    cleanup_nodes();
    rdma_destroy_event_channel(channel);
    if (rai)
        rdma_freeaddrinfo(rai);

    show_perf();
    free(nodes);

    return 0;
}