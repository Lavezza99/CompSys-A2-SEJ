// Setting _DEFAULT_SOURCE is necessary to activate visibility of
// certain header file contents on GNU/Linux systems.
#define _DEFAULT_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fts.h>

#include "job_queue.h"
#include <err.h>
#include "histogram.h"

typedef struct {
  struct job_queue *jq;
  int (*global_hist)[8];           // pointer to the 8-bin global histogram
  pthread_mutex_t *merge_lock;
} worker_arg_t;

static void* worker(void *arg) {
  worker_arg_t *wa = (worker_arg_t*)arg;

  for (;;) {
    char *path = NULL;
    if (job_queue_pop(wa->jq, (void**)&path) != 0) {
      break; // queue destroyed & empty => exit
    }

    FILE *f = fopen(path, "rb");
    if (!f) {
      fprintf(stderr, "fhistogram-mt: cannot open %s\n", path);
      free(path);
      continue;
    }

    int local[8] = {0};
    unsigned char buf[1<<15];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
      for (size_t i = 0; i < n; ++i) {
        update_histogram(local, buf[i]);  // <-- use skeleton helper
      }
    }
    fclose(f);

    assert(pthread_mutex_lock(wa->merge_lock) == 0);
    merge_histogram(local, *wa->global_hist);            // <-- use helper
    assert(pthread_mutex_unlock(wa->merge_lock) == 0);

    free(path);
  }
  return NULL;
}

int main(int argc, char * const *argv) {
  if (argc < 2) {
    err(1, "usage: paths...");
    exit(1);
  }

  int num_threads = 1;
  char * const *paths = &argv[1];

  if (argc > 3 && strcmp(argv[1], "-n") == 0) {
    num_threads = atoi(argv[2]);
    if (num_threads < 1) {
      err(1, "invalid thread count: %s", argv[2]);
    }
    paths = (char * const *)&argv[3];
  } else {
    paths = (char * const *)&argv[1];
  }

  struct job_queue jq;
  if (job_queue_init(&jq, 64) != 0) {
    err(1, "job_queue_init() failed");
  }

  int global_hist[8] = {0};                             // 8-bin histogram per skeleton
  pthread_mutex_t merge_lock = PTHREAD_MUTEX_INITIALIZER;

  pthread_t *threads = calloc(num_threads, sizeof(pthread_t));
  if (!threads) err(1, "calloc threads failed");

  worker_arg_t warg = { .jq = &jq, .global_hist = &global_hist, .merge_lock = &merge_lock };
  for (int i = 0; i < num_threads; i++) {
    if (pthread_create(&threads[i], NULL, &worker, &warg) != 0) {
      err(1, "pthread_create() failed");
    }
  }

  int fts_options = FTS_LOGICAL | FTS_NOCHDIR;
  FTS *ftsp;
  if ((ftsp = fts_open(paths, fts_options, NULL)) == NULL) {
    err(1, "fts_open() failed");
    return -1;
  }

  FTSENT *p;
  while ((p = fts_read(ftsp)) != NULL) {
    switch (p->fts_info) {
      case FTS_D: break;
      case FTS_F:
        if (job_queue_push(&jq, strdup(p->fts_path)) != 0) {
          err(1, "job_queue_push() failed");
        }
        break;
      default: break;
    }
  }
  fts_close(ftsp);

  job_queue_destroy(&jq);
  for (int i = 0; i < num_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      err(1, "pthread_join() failed");
    }
  }
  free(threads);

  print_histogram(global_hist);                         // <-- use helper
  move_lines(9);                                        // keep skeleton call
  return 0;
}


