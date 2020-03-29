#ifndef job_scheduler_h_
#define job_scheduler_h_

#include "./header.h"

#define NUMBER_OF_THREADS 5

struct job {
    void   (*function)(void* argument);
    void*  argument;
    int *barrier;
};

struct job *initialize_job(void (*function)(void*), void *, int *);
void free_job(struct job **);
void *thread_function(void *);

class job_scheduler {
    public:
        int number_of_threads;
        int jobs;
        bool stop;
        pthread_t *thread_pool;
        pthread_mutex_t queue_mutex;
        pthread_cond_t queue_empty;
        pthread_cond_t queue_not_empty;
        pthread_cond_t barrier;
        std::queue<job *> queue;

        job_scheduler(int);
        ~job_scheduler();
        void create_threads();
        void barrier_job_scheduler();
        void dynamic_barrier_job_scheduler(int *);
        void stop_job_scheduler();
        void schedule_job_scheduler(void (*function)(void*), void *, int *);
        void execute_job();
};

#endif