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
static int timeout = 200;
static int retries = 7;
threadpool thpool;
static int num_of_threads = 1;
static int disconnection = 1;
static int cq = 0;
static int pd = 0;
static int eqp = 0;
static int debug = 0;
static int modify_eqp = 0;
static volatile int qpn_counter = 1;
pthread_mutex_t tp_lock;
struct ibv_pd *pd_external_client;
struct ibv_pd *pd_external_server;
struct ibv_cq *cq_external_client;
struct ibv_cq *cq_external_server;

#define DEBUG_LOG if (debug) printf


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
    STEP_CNT,
};

const char *getEnumStep(enum step enumstep) {
    switch (enumstep) {
        case STEP_CREATE_ID:
            return "STEP_CREATE_ID";
        case STEP_BIND:
            return "STEP_BIND";
        case STEP_RESOLVE_ADDR:
            return "STEP_RESOLVE_ADDR";
        case STEP_RESOLVE_ROUTE:
            return "STEP_RESOLVE_ROUTE";
        case STEP_CREATE_QP:
            return "STEP_CREATE_QP";
        case STEP_CONNECT:
            return "STEP_CONNECT";
        case STEP_REQ:
            return "STEP_REQ";
        case STEP_DISCONNECT:
            return "STEP_DISCONNECT";
        case STEP_DESTROY:
            return "STEP_DESTROY";
        default:
            return "Not Found Step";

    }
}

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
    struct rdma_cm_id *cm_id;    /* connection on client side, listener on server side*/
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

void swap(float *a, float *b) {
    float t = *a;
    *a = *b;
    *b = t;
}

int partition(float arr[], int low, int high) {
    float pivot = arr[high];    // pivot
    int i = (low - 1);

    for (int j = low; j <= high - 1; j++) {
        if (arr[j] < pivot) {
            i++;
            swap(&arr[i], &arr[j]);
        }
    }
    swap(&arr[i + 1], &arr[high]);
    return (i + 1);
}

void quickSort(float arr[], int low, int high) {
    if (low < high) {
        int pi = partition(arr, low, high);
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}

static void show_perf(void) {
    float medians[STEP_CNT][connections];
    int c, i;
    float us, max[STEP_CNT], min[STEP_CNT];
    for (i = 0; i < STEP_CNT; i++) {
        max[i] = 0;
        min[i] = 999999999.;
        for (c = 0; c < connections; c++) {
            if (!zero_time(&nodes[c].times[i][0]) &&
                !zero_time(&nodes[c].times[i][1])) {
                us = diff_us(&nodes[c].times[i][1], &nodes[c].times[i][0]);
                medians[i][c] = (float) (us / 1000.);
                if (us > max[i])
                    max[i] = us;
                if (us < min[i])
                    min[i] = us;
            }
        }
    }


    for (i = 0; i < STEP_CNT; i++) {
        DEBUG_LOG("Sort has started for %s\n", getEnumStep(i));
        quickSort(medians[i], 0, connections - 1);
    }

    printf("step\t\ttotal ms\t\tmax ms\t\tmin us\t\tus / conn\t\tmedian(ms)\n");
    for (i = 0; i < STEP_CNT; i++) {
        if (i == STEP_BIND && !client_src_addr)
            continue;

        us = diff_us(&times[i][1], &times[i][0]);
        printf("%-13s:\t%11.2f\t%11.2f\t%11.2f\t%11.2f\t\t%11.2f\n", step_str[i], us / 1000.,
               max[i] / 1000., min[i], us / connections, medians[i][connections / 2]);
    }
}

static void addr_handler(struct node *n) {
    end_perf(n, STEP_RESOLVE_ADDR);
    completed[STEP_RESOLVE_ADDR]++;
}

static void route_handler(struct node *n) {
    // end_perf(n, STEP_RESOLVE_ROUTE);
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


static void _eqp_req_handler(struct rdma_cm_id *id) {
    pthread_mutex_lock(&tp_lock);
    int ret;

    conn_param.qp_num = qpn_counter;
    qpn_counter++;
    DEBUG_LOG("QPN number is %d, thread id = %d\n", conn_param.qp_num, pthread_self());
    if (qpn_counter % 1000 == 0) {
        DEBUG_LOG("Sleeping..\n");
        usleep(100);
        sched_yield();
    }
    if (num_of_threads > 1) {
        usleep(200);
    }
    pthread_mutex_unlock(&tp_lock);
    ret = rdma_accept(id, &conn_param);
    if (ret) {
        perror("failure accepting");
    }
}


static void __req_handler(struct rdma_cm_id *id) {
    int ret;
    if (pd) {
        ret = rdma_create_qp(id, pd_external_server, &init_qp_attr);
    } else {
        ret = rdma_create_qp(id, NULL, &init_qp_attr);
    }
    if (ret) {
        perror("failure creating qp");
        goto err1;
    }
    DEBUG_LOG("QPN number is %d, thread id = %d\n", id->qp->qp_num, pthread_self());
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

static void *req_handler_thread(void (*f)(void *)) {
    struct list_head_cm *work;
    do {
        pthread_mutex_lock(&req_work.lock);
        if (__list_empty(&req_work))
            pthread_cond_wait(&req_work.cond, &req_work.lock);
        work = __list_remove_head(&req_work);
        pthread_mutex_unlock(&req_work.lock);
        (*f)(work->id);
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
        if (work->id->qp)
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
            //DEBUG_LOG("Connection Established!\n");
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
            if (n) {
                conn_handler(n);
                n->error = 1;
            }
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
        case RDMA_CM_EVENT_CONNECT_RESPONSE:
            if (n)
                conn_handler(n);
            break;
        default:
            break;
    }
    rdma_ack_cm_event(event);
}

static int alloc_nodes(void) {
    int ret, i;

    nodes = calloc(sizeof *nodes, connections);
    if (!nodes)
        return -ENOMEM;
    if (eqp) {
        qp_external = calloc(sizeof *qp_external, connections);
        if (!qp_external)
            return -ENOMEM;
    }

    printf("creating id\n");
    start_time(STEP_CREATE_ID);
    for (i = 0; i < connections; i++) {
        start_perf(&nodes[i], STEP_CREATE_ID);
        if (dst_addr) {
            ret = rdma_create_id(channel, &nodes[i].id, &nodes[i], hints.ai_port_space);
            if (ret)
                goto err;
            if (eqp) {
                qp_external[i].cm_id = nodes[i].id;
            }
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
    DEBUG_LOG("Working threads: %d\n", thpool_num_threads_working(thpool));
    thpool_destroy(thpool);
    int i;
    pthread_mutex_destroy(&tp_lock);
    printf("destroying id\n");
    start_time(STEP_DESTROY);
    for (i = 0; i < connections; i++) {
        start_perf(&nodes[i], STEP_DESTROY);
        if (nodes[i].id)
            rdma_destroy_id(nodes[i].id);
        if (eqp) {
            rdma_destroy_id(qp_external[i].cm_id);
        }
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
            perror("failure in rdma_get_cm_event in process_server_events\n");
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

    if (pthread_mutex_init(&tp_lock, NULL) != 0) {
        printf("\n thread pool mutex init has failed\n");
        return 1;
    }
    ret = pthread_mutex_init(&req_work.lock, NULL);
    if (ret) {
        perror("initializing mutex for req work\n");
        return ret;
    }

    ret = pthread_mutex_init(&disc_work.lock, NULL);
    if (ret) {
        perror("initializing mutex for disc work\n");
        return ret;
    }

    ret = pthread_cond_init(&req_work.cond, NULL);
    if (ret) {
        perror("initializing cond for req work\n");
        return ret;
    }

    ret = pthread_cond_init(&disc_work.cond, NULL);
    if (ret) {
        perror("initializing cond for disc work\n");
        return ret;
    }


    // Threadpool of Consumers
    if (num_of_threads > 1) {
        int j;
        for (j = 0; j < num_of_threads; j++) {
            thpool_add_work(thpool, (void *) req_handler_thread, __req_handler);
        }
    } else {
        ret = pthread_create(&req_thread, NULL, (void *) req_handler_thread, __req_handler);
        if (ret) {
            perror("failed to create req handler thread\n");
            return ret;
        }
    }


    ret = pthread_create(&disc_thread, NULL, disc_handler_thread, NULL);
    if (ret) {
        perror("failed to create disconnect handler thread\n");
        return ret;
    }

    ret = rdma_create_id(channel, &listen_id, NULL, hints.ai_port_space);
    if (ret) {
        perror("listen request failed\n");
        return ret;
    }

    ret = get_rdma_addr(server_src_addr, dst_addr, port, &hints, &rai);
    if (ret) {
        printf("getrdmaaddr error: %s\n", gai_strerror(ret));
        goto out;
    }

    ret = rdma_bind_addr(listen_id, rai->ai_src_addr);
    if (ret) {
        perror("bind address failed\n");
        goto out;
    }

    // External CQ Flag
    if (cq) {
        cq_external_server = ibv_create_cq(listen_id->verbs, 100, NULL, NULL, 0);
        if (!cq_external_server) {
            perror("ibv_create_cq failed \n");
            ret = errno;
            goto out;
        }
        init_qp_attr.recv_cq = cq_external_server;
        init_qp_attr.send_cq = cq_external_server;
        DEBUG_LOG("Created cq %p\n", cq_external_server);
    }
    // External PD flag
    if (pd) {
        pd_external_server = ibv_alloc_pd(listen_id->verbs);
        if (!pd_external_server) {
            fprintf(stderr, "Error, ibv_alloc_pd() failed\n");
            goto out;
        }
        DEBUG_LOG("Created pd %p\n", pd_external_server);

    }

    ret = rdma_listen(listen_id, 0);
    if (ret) {
        perror("failure trying to listen\n");
        goto out;
    }

    DEBUG_LOG("Waiting for client to connect..\n");
    process_events(NULL);
    out:
    rdma_destroy_id(listen_id);
    return ret;
}

static void resolve_route_thread(int f) {
    int num_of_resolves;
    int iteration = f;
    if (connections > num_of_threads) {
        num_of_resolves = connections / num_of_threads;
    } else {
        num_of_resolves = connections;
    }
    int j = iteration * num_of_resolves;

    int ret;
    for (; j < iteration * num_of_resolves + num_of_resolves; j++) {
        printf("working on node number %d with thread id = %d\n", j, pthread_self());
        if (nodes[j].error)
            continue;
        nodes[j].retries = retries;
        start_perf(&nodes[j], STEP_RESOLVE_ROUTE);
        ret = rdma_resolve_route(nodes[j].id, timeout);
        if (ret) {
            perror("failure resolving route");
            nodes[j].error = 1;
            continue;
        }
        end_perf(&nodes[j], STEP_RESOLVE_ROUTE);
        pthread_mutex_lock(&tp_lock);
        started[STEP_RESOLVE_ROUTE]++;
        pthread_mutex_unlock(&tp_lock);
    }


}

static int run_client(void) {
    pthread_t event_thread;
    int i, ret;

    if (pthread_mutex_init(&tp_lock, NULL) != 0) {
        printf("\n thread pool mutex init has failed\n");
        return 1;
    }
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

    // External CQ Flag
    if (cq) {
        cq_external_client = ibv_create_cq(nodes[0].id->verbs, 100, NULL, NULL, 0);
        if (!cq_external_client) {
            perror("ibv_create_cq failed (for external QP)\n");
            ret = errno;
            return ret;
        }
        DEBUG_LOG("Created cq %p\n", cq_external_client);
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
        DEBUG_LOG("Created pd %p\n", pd_external_client);
    }

    // External QP flag
    if (eqp) {
        qp_external->cq = cq_external_client;
        qp_external->pd = pd_external_client;

    }

    if (num_of_threads > 1) {
        printf("resolving route with %d threads \n", num_of_threads);
        start_time(STEP_RESOLVE_ROUTE);
        int j;
        for (j = 0; j < num_of_threads; j++) {
            thpool_add_work(thpool, resolve_route_thread, j);
        }
    } else {
        printf("resolving route - single thread\n");
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
            end_perf(&nodes[i], STEP_RESOLVE_ROUTE);
            started[STEP_RESOLVE_ROUTE]++;
        }
    }

    while (connections != completed[STEP_RESOLVE_ROUTE]) sched_yield();
    end_time(STEP_RESOLVE_ROUTE);
    if (!eqp) {
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
    }

    printf("connecting\n");
    start_time(STEP_REQ);
    start_time(STEP_CONNECT);
    if (eqp) {
        struct rdma_conn_param conn_param_test;
        memset(&conn_param_test, 0, sizeof(conn_param_test));
        conn_param_test.responder_resources = 1;
        conn_param_test.initiator_depth = 1;
        conn_param_test.retry_count = 7;
        conn_param_test.rnr_retry_count = 7;
        for (i = 0; i < connections; i++) {
            if (nodes[i].error)
                continue;
            start_perf(&nodes[i], STEP_REQ);
            start_perf(&nodes[i], STEP_CONNECT);
            // Fictitious  qp number
            conn_param_test.qp_num = i + 1;
            ret = rdma_connect(qp_external[i].cm_id, &conn_param_test);
            if (ret) {
                perror("failure connecting");
                nodes[i].error = 1;
                continue;
            }
            started[STEP_CONNECT]++;
        }
        end_time(STEP_REQ);
    } else {
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
    }
    while (started[STEP_CONNECT] != completed[STEP_CONNECT]) sched_yield();
    if (!eqp) {
        end_time(STEP_CONNECT);
    }

    if (eqp) {
        for (i = 0; i < connections; i++) {
            ret = rdma_establish(qp_external[i].cm_id);
            if (ret) {
                perror("rdma_establish");
                return ret;
            }

        }
        // Connection time include rdma_connect & rdma_establish
        end_time(STEP_CONNECT);
        // In order to avoid race between rdma_establish to rdma_disconnect
        sleepmilli(connections / 2);
    }
    if (disconnection) {
        printf("disconnecting\n");
        start_time(STEP_DISCONNECT);
        for (i = 0; i < connections; i++) {
            if (nodes[i].error)
                continue;
            start_perf(&nodes[i], STEP_DISCONNECT);
            rdma_disconnect(nodes[i].id);
            if (!eqp) {
                rdma_destroy_qp(nodes[i].id);
            }
            started[STEP_DISCONNECT]++;
        }
        while (started[STEP_DISCONNECT] != completed[STEP_DISCONNECT]) sched_yield();
        end_time(STEP_DISCONNECT);
    }
    return ret;
}

int init_eqp_requests() {
    pthread_t req_thread;
    pthread_t disc_thread;
    int ret;
    INIT_LIST(&req_work.list);
    INIT_LIST(&disc_work.list);
    if (pthread_mutex_init(&tp_lock, NULL) != 0) {
        printf("\n thread pool mutex init has failed\n");
        return 1;
    }
    memset(&conn_param, 0, sizeof(conn_param));
    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;
    conn_param.retry_count = 7;
    conn_param.rnr_retry_count = 7;
    conn_param.qp_num = qpn_counter;

    ret = pthread_mutex_init(&req_work.lock, NULL);
    if (ret) {
        perror("initializing mutex for req work\n");
        return ret;
    }

    ret = pthread_mutex_init(&disc_work.lock, NULL);
    if (ret) {
        perror("initializing mutex for disc work\n");
        return ret;
    }

    ret = pthread_cond_init(&req_work.cond, NULL);
    if (ret) {
        perror("initializing cond for req work\n");
        return ret;
    }

    ret = pthread_cond_init(&disc_work.cond, NULL);
    if (ret) {
        perror("initializing cond for disc work\n");
        return ret;
    }


    // Threadpool of Consumers
    if (num_of_threads > 1) {
        int j;
        for (j = 0; j < num_of_threads; j++) {
            thpool_add_work(thpool, (void *) req_handler_thread, _eqp_req_handler);
        }
    } else {
        ret = pthread_create(&req_thread, NULL, (void *) req_handler_thread, _eqp_req_handler);
        if (ret) {
            perror("failed to create req handler thread\n");
            return ret;
        }
    }


    ret = pthread_create(&disc_thread, NULL, disc_handler_thread, NULL);
    if (ret) {
        perror("failed to create disconnect handler thread\n");
        return ret;
    }
    return ret;

}

int init_eqp_parameters() {
    int ret;
    struct ibv_port_attr port_attr;
    char str[INET_ADDRSTRLEN];

    hints.ai_flags |= RAI_PASSIVE;
    qp_external = calloc(1, sizeof *qp_external);
    qp_external->sin.ss_family = PF_INET;
    qp_external->port = htobe16(port);


    ret = get_rdma_addr(server_src_addr, dst_addr, port, &hints, &rai);
    if (ret) {
        printf("getrdmaaddr error: %s\n", gai_strerror(ret));
        goto out;
    }

    ret = rdma_create_id(channel, &qp_external->cm_id, NULL, hints.ai_port_space);
    if (ret) {
        perror("create rdma cm id request failed (for external QP)\n");
        ret = errno;
        return ret;
    }

    ret = rdma_bind_addr(qp_external->cm_id, rai->ai_src_addr);
    if (ret) {
        perror("bind address failed (for external QP)\n");
        ret = errno;
        goto out;
    }
    DEBUG_LOG("binding succeeded \n");

    if (qp_external->cm_id->verbs == NULL) {
        perror("Can't extract context from id (for external QP)\n");
        ret = errno;
        goto out;
    }

    DEBUG_LOG("Created context %p\n", qp_external->cm_id->verbs);


    if (ibv_query_port(qp_external->cm_id->verbs, qp_external->cm_id->port_num, &port_attr)) {
        perror("ibv_query_port faild (for external QP)\n");
        ret = errno;
        goto out;
    }

    qp_external->mtu = port_attr.active_mtu;

    if (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET) {
        qp_external->is_global = 1;
        qp_external->sgid_index = ibv_find_sgid_type(qp_external->cm_id->verbs, qp_external->cm_id->port_num,
                                                     IBV_GID_TYPE_ROCE_V2, qp_external->sin.ss_family);
    }
    return ret;

    out:
    rdma_destroy_id(qp_external->cm_id);
    return ret;

}

int init_eqp_cq_pd() {
    int ret;
    qp_external->pd = ibv_alloc_pd(qp_external->cm_id->verbs);
    if (!qp_external->pd) {
        perror("ibv_alloc_pd failed (for external QP)\n");
        ret = errno;
        goto out;
    }

    DEBUG_LOG("Created pd %p\n", qp_external->pd);

    qp_external->cq = ibv_create_cq(qp_external->cm_id->verbs, 100, NULL, NULL, 0);
    if (!qp_external->cq) {
        perror("ibv_create_cq failed (for external QP)\n");
        ret = errno;
        goto out;
    }
    DEBUG_LOG("Created cq %p\n", qp_external->cq);
    init_qp_attr.recv_cq = qp_external->cq;
    init_qp_attr.send_cq = qp_external->cq;
    return ret;
    out:
    rdma_destroy_id(qp_external->cm_id);
    return ret;

}


static int eqp_run_server() {

    int ret;
    DEBUG_LOG("Init requests & disconnect thread for external qp\n");
    ret = init_eqp_requests();
    if (ret) {
        perror("Cannot init request for external QP");
        ret = errno;
        return ret;
    }

    DEBUG_LOG("Init parameters for connection\n");
    ret = init_eqp_parameters();
    if (ret) {
        perror("Cannot init parameters for external QP");
        ret = errno;
        return ret;
    }


    DEBUG_LOG("Init CQ & PD for connection\n");
    ret = init_eqp_cq_pd();
    if (ret) {
        perror("Cannot init CQ & PD for external QP");
        ret = errno;
        return ret;
    }

    ret = rdma_listen(qp_external->cm_id, 0);
    if (ret) {
        perror("listening failed (for external QP)\n");
        ret = errno;
        return ret;
    }
    DEBUG_LOG("listening succeeded \n");
    DEBUG_LOG("Waiting for client to connect..\n");
    process_events(NULL);


    return ret;

}


static struct option longopts[] = {
        {"cq",    no_argument, NULL, 0},
        {"pd",    no_argument, NULL, 0},
        {"eqp",   no_argument, NULL, 0},
        {"DEBUG", no_argument, NULL, 0},
        {NULL, 0,              NULL, 0}
};

int main(int argc, char **argv) {
    int op, ret;
    int option_index = 0;

    while ((op = getopt_long(argc, argv, "s:b:c:p:r:t:n:d:l:cq:pd:eqp:DEBUG:", longopts, &option_index)) != -1) {
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
                        cq++;
                        break;
                    case 1:
                        pd++;
                        break;
                    case 2:
                        eqp++;
                        break;
                    case 3:
                        debug++;
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
                printf("\t[--DEBUG for more info (default is 0)]\n");
                exit(1);
        }
    }


    // Init parameters
    hints.ai_port_space = RDMA_PS_TCP;
    hints.ai_qp_type = IBV_QPT_RC;

    // QP attributes
    init_qp_attr.cap.max_send_wr = 1;
    init_qp_attr.cap.max_recv_wr = 1;
    init_qp_attr.cap.max_send_sge = 1;
    init_qp_attr.cap.max_recv_sge = 1;
    init_qp_attr.qp_type = IBV_QPT_RC;


    // Create channel
    channel = rdma_create_event_channel();
    if (!channel) {
        perror("Failed to create channel\n");
        exit(1);
    }
    // Create thredapool for server side handling events
    if (num_of_threads) {
        thpool = thpool_init(num_of_threads);
    }
    DEBUG_LOG("Threadpool was created with %d threads\n", num_of_threads);

    if (dst_addr) {
        alloc_nodes();
        ret = run_client(); // both for external qp and reguler flow
    } else {
        if (eqp) {
            ret = eqp_run_server();
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
