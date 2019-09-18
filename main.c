/*
 * Copyright (c) 2013 Intel Corporation.  All rights reserved.
 *
 * This software is available to you under the OpenIB.org BSD license
 * below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AWV
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
#include <netinet/tcp.h>
#include "ibv_helper.h"
#include <rdma/rdma_cma.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <limits.h>

#include "queue_internal.h"
#include "threadpool.h"


static struct rdma_addrinfo hints;
static struct rdma_addrinfo *rai;
static struct rdma_event_channel *channel;
static char *port = "7471";
static char *dst_addr;
static char *client_src_addr;
static char *server_src_addr;
static int timeout = 2000;
static int retries = 2;
///////////////////
threadpool thpool;
static int num_of_threads = 1;
static int disconnection = 1;
static int cq = 0;
static int pd = 0;
static int eqp = 0;
pthread_mutex_t tp_lock;
struct ibv_pd *pd_external_client;
struct ibv_pd *pd_external_server;
struct ibv_cq *cq_external_client;
struct ibv_cq *cq_external_server;


enum step {
    STEP_CREATE_ID,
    STEP_BIND,
    STEP_RESOLVE_ADDR,
    STEP_RESOLVE_ROUTE,
    STEP_CREATE_QP,
    STEP_CONNECT,
    STEP_REQ,
    STEP_DISCONNECT,
    STEP_DESTROY,
    STEP_CNT
};

static const char *step_str[] = {
        "create id",
        "bind addr",
        "resolve addr",
        "resolve route",
        "create qp",
        "connect",
        "request time",
        "disconnect",
        "destroy"
};

struct qp_external_attr {
    struct ibv_cq *cq;
    struct ibv_pd *pd;
    struct ibv_qp *qp;        /* DCI (client) or DCT (server) */
    enum ibv_mtu mtu;
    struct rdma_cm_id *cm_id;    /* connection on client side,*/
    /* listener on service side. */
    struct sockaddr_storage sin;
    __be16 port;
    uint8_t is_global;
    uint8_t sgid_index;

};

struct node {
    struct rdma_cm_id *id;
    struct timeval times[STEP_CNT][2];
    int error;
    int retries;
};

struct list_head_cm {
    struct list_head_cm *prev;
    struct list_head_cm *next;
    struct rdma_cm_id *id;
};

struct work_list {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    struct list_head_cm list;
};

#define INIT_LIST(x) ((x)->prev = (x)->next = (x))

static struct work_list req_work;
static struct work_list disc_work;
static struct node *nodes;
static struct qp_external_attr *qp_external;
static struct timeval times[STEP_CNT][2];
static int connections = 100;
static volatile int started[STEP_CNT];
static volatile int completed[STEP_CNT];
static struct ibv_qp_init_attr init_qp_attr;
static struct rdma_conn_param conn_param;

#define start_perf(n, s)    gettimeofday(&((n)->times[s][0]), NULL)
#define end_perf(n, s)        gettimeofday(&((n)->times[s][1]), NULL)
#define start_time(s)        gettimeofday(&times[s][0], NULL)
#define end_time(s)        gettimeofday(&times[s][1], NULL)

static inline void __list_delete(struct list_head_cm *list) {
    struct list_head_cm *prev, *next;
    prev = list->prev;
    next = list->next;
    prev->next = next;
    next->prev = prev;
    INIT_LIST(list);
}

static inline int __list_empty(struct work_list *list) {
    return list->list.next == &list->list;
}

static inline struct list_head_cm *__list_remove_head(struct work_list *work_list) {
    struct list_head_cm *list_item;

    list_item = work_list->list.next;
    __list_delete(list_item);
    return list_item;
}

static inline void list_add_tail_cm(struct work_list *work_list, struct list_head_cm *req) {
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

static int zero_time(struct timeval *t) {
    return !(t->tv_sec || t->tv_usec);
}

static float diff_us(struct timeval *end, struct timeval *start) {
    return (end->tv_sec - start->tv_sec) * 1000000. + (end->tv_usec - start->tv_usec);
}

static void show_perf(void) {
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
        if (i == STEP_BIND && !client_src_addr)
            continue;

        us = diff_us(&times[i][1], &times[i][0]);
        printf("%-13s: %11.2f%11.2f%11.2f%11.2f\n", step_str[i], us / 1000.,
               max[i] / 1000., min[i], us / connections);
    }
}

static void addr_handler(struct node *n) {
    end_perf(n, STEP_RESOLVE_ADDR);
    completed[STEP_RESOLVE_ADDR]++;
}

static void route_handler(struct node *n) {
    end_perf(n, STEP_RESOLVE_ROUTE);
    completed[STEP_RESOLVE_ROUTE]++;
}

static void req_handler(struct node *n) {
    end_perf(n, STEP_REQ);
    completed[STEP_REQ]++;
}

static void conn_handler(struct node *n) {
    end_perf(n, STEP_CONNECT);
    completed[STEP_CONNECT]++;
}

static void disc_handler(struct node *n) {
    end_perf(n, STEP_DISCONNECT);
    completed[STEP_DISCONNECT]++;
}

static void __req_handler(struct rdma_cm_id *id) {
    int ret;
    if (pd) {
        ret = rdma_create_qp(id, pd_external_server, &init_qp_attr);
    } else {
        ret = rdma_create_qp(id, NULL, &init_qp_attr);
    }
    //printf("id is %d , recv cq is %d, send cq is %d \n", id,&(init_qp_attr.recv_cq),&(init_qp_attr.send_cq));
    if (ret) {
        perror("failure creating qp");
        goto err1;
    }

    ret = rdma_accept(id, NULL);
    if (ret) {
        perror("failure accepting");
        goto err2;
    }
    return;
    err2:
    rdma_destroy_qp(id);
    err1:
    printf("failing connection request\n");
    rdma_reject(id, NULL, 0);
    rdma_destroy_id(id);
    return;
}

static void *req_handler_thread(void *arg) {
    struct list_head_cm *work;
    do {
        pthread_mutex_lock(&req_work.lock);
        //printf("CONSUMER thread id = %d\n", pthread_self());
        if (__list_empty(&req_work))
            pthread_cond_wait(&req_work.cond, &req_work.lock);
        work = __list_remove_head(&req_work);
        pthread_mutex_unlock(&req_work.lock);
        __req_handler(work->id);
        free(work);
    } while (1);
    return NULL;
}

static void *disc_handler_thread(void *arg) {
    struct list_head_cm *work;
    do {
        pthread_mutex_lock(&disc_work.lock);
        if (__list_empty(&disc_work))
            pthread_cond_wait(&disc_work.cond, &disc_work.lock);
        work = __list_remove_head(&disc_work);
        pthread_mutex_unlock(&disc_work.lock);
        rdma_disconnect(work->id);
        rdma_destroy_qp(work->id);
        rdma_destroy_id(work->id);
        free(work);
    } while (1);
    return NULL;
}

static void *cma_handler(struct rdma_cm_id *id, struct rdma_cm_event *event) {
    struct node *n = id->context;
    struct list_head_cm *request;

    switch (event->event) {
        case RDMA_CM_EVENT_ADDR_RESOLVED:
            addr_handler(n);
            break;
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            route_handler(n);
            break;
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            started[STEP_REQ]++;
            //printf("PRODUCER thread id = %d\n", pthread_self());
            request = malloc(sizeof *request);
            if (!request) {
                perror("out of memory accepting connect request");
                rdma_reject(id, NULL, 0);
                rdma_destroy_id(id);
            } else {
                INIT_LIST(request);
                request->id = id;
                list_add_tail_cm(&req_work, request);
            }
            if (n) {
                req_handler(n);
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
                    rdma_destroy_qp(id);
                    rdma_destroy_id(id);
                } else {
                    INIT_LIST(request);
                    request->id = id;
                    list_add_tail_cm(&disc_work, request);
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

static int alloc_nodes(void) {
    int ret, i;
    if (pthread_mutex_init(&tp_lock, NULL) != 0) {
        printf("\n thread pool mutex init has failed\n");
        return 1;
    }
    nodes = calloc(sizeof *nodes, connections);
    if (!nodes)
        return -ENOMEM;

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

static void cleanup_nodes(void) {
    printf("Working threads: %d\n", thpool_num_threads_working(thpool));
    thpool_destroy(thpool);
    int i;
    pthread_mutex_destroy(&tp_lock);
    printf("destroying id\n");
    start_time(STEP_DESTROY);
    for (i = 0; i < connections; i++) {
        start_perf(&nodes[i], STEP_DESTROY);
        if (nodes[i].id)
            rdma_destroy_id(nodes[i].id);
        end_perf(&nodes[i], STEP_DESTROY);
    }
    end_time(STEP_DESTROY);
    if (pd) {
        free(pd_external_client);
        free(pd_external_server);
    }
    if (cq) {
        free(cq_external_client);
        free(cq_external_server);
    }
}

static void *process_events(void *arg) {
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

int get_rdma_addr(char *src, char *dst, char *port,
                  struct rdma_addrinfo *hints, struct rdma_addrinfo **rai) {
    struct rdma_addrinfo rai_hints, *res;
    int ret;


    if (hints->ai_flags & RAI_PASSIVE)
        return rdma_getaddrinfo(src, port, hints, rai);

    if (src) {
        rai_hints.ai_flags |= RAI_PASSIVE;
        ret = rdma_getaddrinfo(src, NULL, &rai_hints, &res);
        if (ret)
            return ret;

        rai_hints.ai_src_addr = res->ai_src_addr;
        rai_hints.ai_src_len = res->ai_src_len;
        rai_hints.ai_flags &= ~RAI_PASSIVE;
    }

    rai_hints = *hints;


    ret = rdma_getaddrinfo(dst, port, &rai_hints, rai);
    if (src)
        rdma_freeaddrinfo(res);

    return ret;
}

static int run_server(void) {
    pthread_t req_thread;
    pthread_t disc_thread;
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


    // Threadpool of Consumers
    if (num_of_threads > 1) {
        int j;
        for (j = 0; j < num_of_threads; j++) {
            thpool_add_work(thpool, (void *) req_handler_thread, NULL);
        }
    } else {
        ret = pthread_create(&req_thread, NULL, req_handler_thread, NULL);
        if (ret) {
            perror("failed to create req handler thread");
            return ret;
        }
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

    ret = get_rdma_addr(server_src_addr, dst_addr, port, &hints, &rai);
    if (ret) {
        printf("getrdmaaddr error: %s\n", gai_strerror(ret));
        goto out;
    }

    ret = rdma_bind_addr(listen_id, rai->ai_src_addr);
    if (ret) {
        perror("bind address failed");
        goto out;
    }

    // External CQ Flag
    if (cq) {
        cq_external_server = ibv_create_cq(listen_id->verbs, 100, NULL, NULL, 0);
        init_qp_attr.recv_cq = cq_external_server;
        init_qp_attr.send_cq = cq_external_server;

    }
    // External PD flag
    if (pd) {
        pd_external_server = ibv_alloc_pd(listen_id->verbs);
        if (!pd_external_server) {
            fprintf(stderr, "Error, ibv_alloc_pd() failed\n");
            return -1;
        }

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

static int run_client(void) {
    pthread_t event_thread;
    int i, ret;

    ret = get_rdma_addr(client_src_addr, dst_addr, port, &hints, &rai);
    if (ret) {
        printf("getaddrinfo error: %s\n", gai_strerror(ret));
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

    if (client_src_addr) {
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

    printf("resolving address\n");
    start_time(STEP_RESOLVE_ADDR);
    for (i = 0; i < connections; i++) {
        if (nodes[i].error)
            continue;
        nodes[i].retries = retries;
        start_perf(&nodes[i], STEP_RESOLVE_ADDR);
        ret = rdma_resolve_addr(nodes[i].id, rai->ai_src_addr,
                                rai->ai_dst_addr, timeout);
        if (ret) {
            perror("failure getting addr");
            nodes[i].error = 1;
            continue;
        }
        started[STEP_RESOLVE_ADDR]++;
    }
    while (started[STEP_RESOLVE_ADDR] != completed[STEP_RESOLVE_ADDR]) sched_yield();
    end_time(STEP_RESOLVE_ADDR);

    // CQ Flag
    if (cq) {
        cq_external_client = ibv_create_cq(nodes[0].id->verbs, 100, NULL, NULL, 0);
        init_qp_attr.recv_cq = cq_external_client;
        init_qp_attr.send_cq = cq_external_client;
    }

    // External PD flag
    if (pd) {
        pd_external_client = ibv_alloc_pd(nodes[0].id->verbs);
        if (!pd_external_client) {
            fprintf(stderr, "Error, ibv_alloc_pd() failed\n");
            return -1;
        }

    }
    printf("resolving route\n");
    start_time(STEP_RESOLVE_ROUTE);
    for (i = 0; i < connections; i++) {
        if (nodes[i].error)
            continue;
        nodes[i].retries = retries;
        start_perf(&nodes[i], STEP_RESOLVE_ROUTE);
        ret = rdma_resolve_route(nodes[i].id, timeout);
        if (ret) {
            perror("failure resolving route");
            nodes[i].error = 1;
            continue;
        }
        started[STEP_RESOLVE_ROUTE]++;
    }
    while (started[STEP_RESOLVE_ROUTE] != completed[STEP_RESOLVE_ROUTE]) sched_yield();
    end_time(STEP_RESOLVE_ROUTE);

    printf("creating qp\n");
    start_time(STEP_CREATE_QP);
    for (i = 0; i < connections; i++) {
        if (nodes[i].error)
            continue;
        start_perf(&nodes[i], STEP_CREATE_QP);
        if (pd) {
            ret = rdma_create_qp(nodes[i].id, pd_external_client, &init_qp_attr);
        } else {
            ret = rdma_create_qp(nodes[i].id, NULL, &init_qp_attr);
        }
        if (ret) {
            perror("failure creating qp");
            nodes[i].error = 1;
            continue;
        }
        end_perf(&nodes[i], STEP_CREATE_QP);
    }
    end_time(STEP_CREATE_QP);

    printf("connecting\n");
    start_time(STEP_REQ);
    start_time(STEP_CONNECT);
    for (i = 0; i < connections; i++) {
        if (nodes[i].error)
            continue;

        start_perf(&nodes[i], STEP_REQ);
        start_perf(&nodes[i], STEP_CONNECT);
        ret = rdma_connect(nodes[i].id, &conn_param);
        if (ret) {
            perror("failure connecting");
            nodes[i].error = 1;
            continue;
        }
        started[STEP_CONNECT]++;
    }
    end_time(STEP_REQ);
    while (started[STEP_CONNECT] != completed[STEP_CONNECT]) sched_yield();
    end_time(STEP_CONNECT);

    if (disconnection) {
        printf("disconnecting\n");
        start_time(STEP_DISCONNECT);
        for (i = 0; i < connections; i++) {
            if (nodes[i].error)
                continue;
            start_perf(&nodes[i], STEP_DISCONNECT);
            rdma_disconnect(nodes[i].id);
            rdma_destroy_qp(nodes[i].id);
            started[STEP_DISCONNECT]++;
        }
        while (started[STEP_DISCONNECT] != completed[STEP_DISCONNECT]) sched_yield();
        end_time(STEP_DISCONNECT);
    }
    return ret;
}

static struct option longopts[] = {
        {"cq",  no_argument, NULL, 0},
        {"pd",  no_argument, NULL, 0},
        {"eqp", no_argument, NULL, 0},
        {NULL, 0,            NULL, 0}
};


static int eqp_run_server() {

    struct ibv_port_attr port_attr;
    int ret;
    char str[INET_ADDRSTRLEN];
    struct addrinfo *res;

    ret = getaddrinfo(server_src_addr, NULL, NULL, &res);
    if (ret) {
        printf("getaddrinfo failed (%s) - invalid hostname or IP address\n", gai_strerror(ret));
        return ret;
    }

    if (res->ai_family == PF_INET)
        memcpy(&qp_external->sin, res->ai_addr, sizeof(struct sockaddr_in));
    else if (res->ai_family == PF_INET6)
        memcpy(&qp_external->sin, res->ai_addr, sizeof(struct sockaddr_in6));
    else
        ret = -1;


    ret = rdma_create_id(channel, &qp_external->cm_id, NULL, hints.ai_port_space);
    if (ret) {
        perror("create rdma cm id request failed (for external QP)");
        return ret;
    }
    // Resolve sin family
    if (qp_external->sin.ss_family == AF_INET) {
        ((struct sockaddr_in *) &qp_external->sin)->sin_port = qp_external->port;
        inet_ntop(AF_INET, &(((struct sockaddr_in *) &qp_external->sin)->sin_addr), str, INET_ADDRSTRLEN);
    } else {
        ((struct sockaddr_in6 *) &qp_external->sin)->sin6_port = qp_external->port;
        inet_ntop(AF_INET6, &(((struct sockaddr_in6 *) &qp_external->sin)->sin6_addr), str, INET_ADDRSTRLEN);
    }

    // Binding
    ret = rdma_bind_addr(qp_external->cm_id, (struct sockaddr *) &qp_external->sin);
    if (ret) {
        perror("bind address failed (for external QP)");
        goto out;
    }


    // Check if we could bind
    if (qp_external->cm_id->verbs == NULL) {
        perror("Can't extract context from id (for external QP)");
        goto out;
    }

    // Query port
    if (ibv_query_port(qp_external->cm_id->verbs, qp_external->cm_id->port_num, &port_attr)) {
        perror("ibv_query_port faild (for external QP)");
        goto out;
    }

    // Setting mtu
    qp_external->mtu = port_attr.active_mtu;

    if (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET) {
        qp_external->is_global = 1;
        qp_external->sgid_index = ibv_find_sgid_type(qp_external->cm_id->verbs, qp_external->cm_id->port_num,
                                                     IBV_GID_TYPE_ROCE_V2, qp_external->sin.ss_family);
    }


    out:
    rdma_destroy_id(qp_external->cm_id);
    return ret;


}

int main(int argc, char **argv) {
    int op, ret;
    int option_index = 0;

    while ((op = getopt_long(argc, argv, "s:b:c:p:r:t:n:d:l:cq:pd:eqp:", longopts, &option_index)) != -1) {
        switch (op) {
            case 's':
                dst_addr = optarg;
                break;
            case 'b':
                client_src_addr = optarg;
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
                num_of_threads = atoi(optarg);
                break;
            case 'd':
                disconnection = atoi(optarg);
                break;
            case 'l':
                server_src_addr = optarg;
                break;
            case 0:
                switch (option_index) {
                    case 0:
                        cq = 1;
                        break;
                    case 1:
                        pd = 1;
                        break;
                    case 2:
                        eqp = 1;
                        break;
                }
                break;

            default:
                printf("usage: %s\n", argv[0]);
                printf("\t[-s which server address to connect (client side)]\n");
                printf("\t[-b bind_address (the src address of client side) ]\n");
                printf("\t[-c connections (client side)]\n");
                printf("\t[-p port_number]\n");
                printf("\t[-r retries]\n");
                printf("\t[-t timeout_ms]\n");
                printf("\t[-n num_of_threads(server side)]\n");
                printf("\t[-d include disconnect test (0|1) (default is 1)]\n");
                printf("\t[-l listening to ip (server side) \n");
                printf("\t[--cq create CQ before connect (default is 0)]\n");
                printf("\t[--pd create PD before connect (default is 0)]\n");
                printf("\t[--eqp create external QP before connect (default is 0)]\n");
                exit(1);
        }
    }


    // Init parameters
    hints.ai_port_space = RDMA_PS_TCP;
    hints.ai_qp_type = IBV_QPT_RC;

    init_qp_attr.cap.max_send_wr = 1;
    init_qp_attr.cap.max_recv_wr = 1;
    init_qp_attr.cap.max_send_sge = 1;
    init_qp_attr.cap.max_recv_sge = 1;
    init_qp_attr.qp_type = IBV_QPT_RC;


    // Create channel
    channel = rdma_create_event_channel();
    if (!channel) {
        exit(1);
    }

    // Create thredapool for server side handling events
    if (num_of_threads) {
        thpool = thpool_init(num_of_threads);
    }

    // External QP flag
    if (eqp) {
        // Need more attributes for external QP
        if (dst_addr) { // Client

        } else { //Server
            hints.ai_flags |= RAI_PASSIVE;
            qp_external = calloc(1, sizeof *qp_external);
            qp_external->sin.ss_family = PF_INET;
            qp_external->port = htobe16(7174);
            ret = eqp_run_server();
        }
    } else { // Not external QP
        if (dst_addr) {
            alloc_nodes();
            ret = run_client();
        } else {
            hints.ai_flags |= RAI_PASSIVE;
            ret = run_server();
        }
    }

    cleanup_nodes();
    rdma_destroy_event_channel(channel);
    if (rai)
        rdma_freeaddrinfo(rai);

    show_perf();
    free(nodes);
    return ret;
}
