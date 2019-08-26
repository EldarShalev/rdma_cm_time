//
// Created by eldarsh on 8/26/19.
//

#ifndef RDMA_CM_TIME_QUEUE_INTERNAL_H
#define RDMA_CM_TIME_QUEUE_INTERNAL_H

#endif //RDMA_CM_TIME_QUEUE_INTERNAL_H
#include <stdio.h>
#include <stdlib.h>

#include "queue.h"

/**
 * ATTENTION:
 * these functions are internal and should not be used directly.
 * they may _not_ lock properly, expecting the caller to do so
 */

/**
 * locks the queue
 * returns 0 on success, else not usable
 */
int8_t queue_lock_internal(queue_t *q);

/**
 * unlocks the queue
 * returns 0 on success, else not usable
 */
int8_t queue_unlock_internal(queue_t *q);

/**
  * adds an element to the queue.
  * when action is NULL the function returns with an error code.
  * queue _has_ to be locked.
  *
  * q - the queue
  * el - the element
  * action - specifies what should be executed if max_elements is reached.
  *
  * returns < 0 => error, 0 okay
  */
int8_t queue_put_internal(queue_t *q, void *el, int (*action)(pthread_cond_t *, pthread_mutex_t *));

/**
  * gets the first element in the queue.
  * when action is NULL the function returns with an error code.
  * queue _has_ to be locked.
  *
  * q - the queue
  * e - element pointer
  * action - specifies what should be executed if there are no elements in the queue
  * cmp - comparator function, NULL will create an error
  * cmpel - element with which should be compared
  *
  * returns < 0 => error, 0 okay
  */
int8_t queue_get_internal(queue_t *q, void **e, int (*action)(pthread_cond_t *, pthread_mutex_t *), int (*cmp)(void *, void *), void *cmpel);

/**
  * destroys a queue.
  * queue will be locked.
  *
  * q - the queue
  * fd - should element data be freed? 0 => No, Otherwise => Yes
  * ff - function to release the memory, NULL => free()
  */
int8_t queue_destroy_internal(queue_t *q, uint8_t fd, void (*ff)(void *));

/**
  * flushes a queue.
  * deletes all elements in the queue.
  * queue _has_ to be locked.
  *
  * q - the queue
  * fd - should element data be freed? 0 => No, Otherwise => Yes
  * ff - function to release the memory, NULL => free()
  */
int8_t queue_flush_internal(queue_t *q, uint8_t fd, void (*ff)(void *));