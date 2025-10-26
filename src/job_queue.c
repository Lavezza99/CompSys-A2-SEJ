#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include "job_queue.h"

int job_queue_init(struct job_queue *job_queue, int capacity) {
  if (capacity <= 0) return -1;

  job_queue->buffer = malloc(sizeof(void*) * capacity);
  if (!job_queue->buffer) return -1;

  job_queue->capacity = capacity;
  job_queue->size = 0;
  job_queue->front = 0;
  job_queue->rear = 0;
  job_queue->destroyed = 0;

  if (pthread_mutex_init(&job_queue->mutex, NULL) != 0) return -1;
  if (pthread_cond_init(&job_queue->not_full, NULL) != 0) return -1;
  if (pthread_cond_init(&job_queue->not_empty, NULL) != 0) return -1;

  return 0;
}

int job_queue_destroy(struct job_queue *job_queue) {
  pthread_mutex_lock(&job_queue->mutex);

  // Wait until queue is empty
  while (job_queue->size > 0) {
    pthread_cond_wait(&job_queue->not_empty, &job_queue->mutex);
  }
  
  job_queue->destroyed = 1;
  pthread_cond_broadcast(&job_queue->not_empty);
  
  pthread_mutex_unlock(&job_queue->mutex);

  pthread_mutex_destroy(&job_queue->mutex);
  pthread_cond_destroy(&job_queue->not_full);
  pthread_cond_destroy(&job_queue->not_empty);

  free(job_queue->buffer);
  return 0;
}

int job_queue_push(struct job_queue *job_queue, void *data) {
  pthread_mutex_lock(&job_queue->mutex);

  while (job_queue->size == job_queue->capacity && !job_queue->destroyed) {
    pthread_cond_wait(&job_queue->not_full, &job_queue->mutex);
  }

  if (job_queue->destroyed) {
    pthread_mutex_unlock(&job_queue->mutex);
    return -1;
  }

  job_queue->buffer[job_queue->rear] = data;
  job_queue->rear = (job_queue->rear + 1) % job_queue->capacity;
  job_queue->size++;

  pthread_cond_signal(&job_queue->not_empty);
  pthread_mutex_unlock(&job_queue->mutex);
  return 0;
}

int job_queue_pop(struct job_queue *job_queue, void **data) {
  pthread_mutex_lock(&job_queue->mutex);

  while (job_queue->size == 0 && !job_queue->destroyed) {
    pthread_cond_wait(&job_queue->not_empty, &job_queue->mutex);
  }

  if (job_queue->size == 0 && job_queue->destroyed) {
    pthread_mutex_unlock(&job_queue->mutex);
    return -1;
  }

  *data = job_queue->buffer[job_queue->front];
  job_queue->front = (job_queue->front + 1) % job_queue->capacity;
  job_queue->size--;

  pthread_cond_signal(&job_queue->not_full);
  
  // Signal destroy if queue is now empty
  if (job_queue->size == 0) {
    pthread_cond_signal(&job_queue->not_empty);
  }
  
  pthread_mutex_unlock(&job_queue->mutex);
  return 0;
}