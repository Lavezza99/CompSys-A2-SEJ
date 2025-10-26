// Setting _DEFAULT_SOURCE is necessary to activate visibility of
// certain header file contents on GNU/Linux systems.
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fts.h>

// err.h contains various nonstandard BSD extensions, but they are
// very handy.
#include <err.h>

#include <pthread.h>

#include "job_queue.h"

static pthread_mutex_t stdout_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
  struct job_queue *jq;
  const char *needle;
} worker_arg_t;

// Function to search a file for the needle (similar to fauxgrep_file but for threads)
void search_file(const char *needle, const char *path) {
    FILE *f = fopen(path, "r");
    if (f == NULL) {
        pthread_mutex_lock(&stdout_mutex);
        warn("failed to open %s", path);
        pthread_mutex_unlock(&stdout_mutex);
        return;
    }

    char *line = NULL;
    size_t linelen = 0;
    int lineno = 1;

    while (getline(&line, &linelen, f) != -1) {
        if (strstr(line, needle) != NULL) {
            pthread_mutex_lock(&stdout_mutex);
            printf("%s:%d: %s", path, lineno, line);
            pthread_mutex_unlock(&stdout_mutex);
        }
        lineno++;
    }

    free(line);
    fclose(f);
}

static void* worker(void *arg) {
  worker_arg_t *wa = (worker_arg_t*)arg;

  for (;;) {
    char *path = NULL;
    if (job_queue_pop(wa->jq, (void**)&path) != 0) {
      // queue destroyed & empty or error => time to exit
      break;
    }

    // Search the file for the needle
    search_file(wa->needle, path);
        
    // Free the duplicated string
    free(path);
  }
  return NULL;
}

int main(int argc, char * const *argv) {
  if (argc < 2) {
    err(1, "usage: [-n INT] STRING paths...");
    exit(1);
  }

  int num_threads = 1;
  char const *needle = argv[1];
  char * const *paths = &argv[2];

  if (argc > 3 && strcmp(argv[1], "-n") == 0) {
    num_threads = atoi(argv[2]);
    if (num_threads < 1) {
      err(1, "invalid thread count: %s", argv[2]);
    }
    needle = argv[3];
    paths = (char * const *)&argv[4];
  } else {
    needle = argv[1];
    paths = (char * const *)&argv[2];
  }

  // ==== Initialise the job queue and worker threads ====
  struct job_queue jq;
  if (job_queue_init(&jq, 64) != 0) {
    err(1, "job_queue_init() failed");
  }

  pthread_t *threads = calloc(num_threads, sizeof(pthread_t));
  if (!threads) err(1, "calloc threads failed");

  worker_arg_t warg = { .jq = &jq, .needle = needle };
  for (int i = 0; i < num_threads; i++) {
    if (pthread_create(&threads[i], NULL, &worker, &warg) != 0) {
      err(1, "pthread_create() failed");
    }
  }

  // FTS_LOGICAL = follow symbolic links
  // FTS_NOCHDIR = do not change the working directory of the process
  int fts_options = FTS_LOGICAL | FTS_NOCHDIR;

  FTS *ftsp;
  if ((ftsp = fts_open(paths, fts_options, NULL)) == NULL) {
    err(1, "fts_open() failed");
    return -1;
  }

  FTSENT *p;
  int file_count = 0;
  while ((p = fts_read(ftsp)) != NULL) {
    switch (p->fts_info) {
    case FTS_D:
      break;
    case FTS_F:
      // ==== Enqueue the file path as a job ====
      if (job_queue_push(&jq, strdup(p->fts_path)) != 0) {
        err(1, "job_queue_push() failed");
      }
      break;
    default:
      break;
    }
  }

  fts_close(ftsp);

  // ==== Shut down queue and join threads ====
  job_queue_destroy(&jq); // blocks until queue empty, then wakes workers
  for (int i = 0; i < num_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      err(1, "pthread_join() failed");
    }
  }
  free(threads);
  return 0;
}

