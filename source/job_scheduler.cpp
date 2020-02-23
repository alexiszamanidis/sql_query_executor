#include "../header/job_scheduler.h"

struct job *initialize_job(void (*function)(void*), void *argument, int *barrier) {
    struct job *new_job = (struct job *)malloc(sizeof(struct job));
    error_handler(new_job == NULL,"malloc failed");

    (*barrier)++;
    new_job->function = function;
    new_job->argument = argument;
    new_job->barrier = barrier;
    
    return new_job;
}

void free_job(struct job **job) {
    if( job == NULL )
        return;
    (*(*job)->barrier)--;
    if( (*job)->argument != NULL )
        free((*job)->argument);
    free(*job);
}

void *thread_function(void *job_scheduler_argument) {
    extern job_scheduler *job_scheduler_;

    error_handler(job_scheduler_ == NULL,"job scheduler is NULL");

    while( true ) {
        error_handler(pthread_mutex_lock(&job_scheduler_->queue_mutex) != 0,"pthread_mutex_lock failed");
        while( (job_scheduler_->queue.size() == 0) && (job_scheduler_->stop == false) )
            pthread_cond_wait(&job_scheduler_->queue_not_empty,&job_scheduler_->queue_mutex);
        if( job_scheduler_->stop == true ) {
            error_handler(pthread_mutex_unlock(&job_scheduler_->queue_mutex) != 0,"pthread_mutex_unlock failed");
            pthread_exit(0);
        }
        else
            job_scheduler_->execute_job();
    }
}

job_scheduler::job_scheduler(int number_of_threads){
    error_handler(number_of_threads < 1,"number of threads is less than 1");

    this->number_of_threads = number_of_threads;
    this->jobs = 0;
    this->stop = false;
    this->thread_pool = (pthread_t *)malloc(sizeof(pthread_t)*number_of_threads);
    error_handler(this->thread_pool == NULL,"malloc failed");
    error_handler(pthread_mutex_init(&this->queue_mutex, NULL) != 0,"pthread_mutex_init failed");
    error_handler(pthread_cond_init(&this->queue_empty, NULL) != 0,"pthread_cond_init failed");
    error_handler(pthread_cond_init(&this->queue_not_empty, NULL) != 0,"pthread_cond_init failed");
    error_handler(pthread_cond_init(&this->barrier, NULL) != 0,"pthread_cond_init failed");
}

job_scheduler::~job_scheduler() {
    if( this->thread_pool != NULL )
        free(this->thread_pool);
    pthread_mutex_destroy(&this->queue_mutex);
    pthread_cond_destroy(&this->queue_empty);
    pthread_cond_destroy(&this->queue_not_empty);
    pthread_cond_destroy(&this->barrier);
}

void job_scheduler::create_threads() {
    int return_value;
    for( int i = 0 ; i < this->number_of_threads ; i++ ) {
        return_value = pthread_create(&(this->thread_pool[i]),0,thread_function,NULL);
        error_handler(return_value != 0,"pthread_create failed");
    }
}

void job_scheduler::barrier_job_scheduler() {
    error_handler(pthread_mutex_lock(&this->queue_mutex) != 0,"pthread_mutex_lock failed");
    while( this->jobs > 0 )
        pthread_cond_wait(&this->queue_empty,&this->queue_mutex);
    error_handler(pthread_mutex_unlock(&this->queue_mutex) != 0,"pthread_mutex_unlock failed");
}

void job_scheduler::dynamic_barrier_job_scheduler(int *barrier) {
    while( true ) {
        error_handler(pthread_mutex_lock(&this->queue_mutex) != 0,"pthread_mutex_lock failed");
        if( (this->queue.size() != 0) && ((*barrier) != 0) )
            this->execute_job();
        else if( (this->queue.size() == 0) && ((*barrier) != 0) ) {
            while( (this->queue.size() == 0) && ((*barrier) != 0) )
                pthread_cond_wait(&this->barrier,&this->queue_mutex);
            error_handler(pthread_mutex_unlock(&this->queue_mutex) != 0,"pthread_mutex_unlock failed");
        }
        else if( (this->queue.size() == 0) && ((*barrier) == 0) ) {
            error_handler(pthread_mutex_unlock(&this->queue_mutex) != 0,"pthread_mutex_unlock failed");
            break;
        }
        else
            error_handler(pthread_mutex_unlock(&this->queue_mutex) != 0,"pthread_mutex_unlock failed");
    }
}

void job_scheduler::stop_job_scheduler() {
    this->stop = true;
    error_handler(pthread_cond_broadcast(&this->queue_not_empty) != 0,"pthread_cond_broadcast failed");
    for( int i = 0 ; i < this->number_of_threads ; i++ )
        error_handler(pthread_join(this->thread_pool[i],0) != 0,"pthread_join failed");
}

void job_scheduler::schedule_job_scheduler(void (*function)(void*), void *argument, int *barrier) {
    error_handler(pthread_mutex_lock(&this->queue_mutex) != 0,"pthread_mutex_lock failed");
    struct job *new_job = initialize_job(function,argument,barrier);
    this->queue.push(new_job);
    this->jobs++;
    error_handler(pthread_cond_signal(&this->queue_not_empty) != 0,"pthread_cond_signal failed");
    error_handler(pthread_cond_signal(&this->barrier) != 0,"pthread_cond_signal failed");
    error_handler(pthread_mutex_unlock(&this->queue_mutex) != 0,"pthread_mutex_unlock failed");
}

void job_scheduler::execute_job() {
    struct job *job = NULL;
    void (*function)(void*), *argument;
    int *barrier;

    job = this->queue.front();
    this->queue.pop();
    error_handler(pthread_mutex_unlock(&this->queue_mutex) != 0,"pthread_mutex_unlock failed");

    function = job->function;
    argument = job->argument;
    // execute the function
    function(argument);

    error_handler(pthread_mutex_lock(&this->queue_mutex) != 0,"pthread_mutex_lock failed");
    barrier = job->barrier;
    free_job(&job);
    this->jobs--;
    if( this->jobs == 0 )
        error_handler(pthread_cond_signal(&this->queue_empty) != 0,"pthread_cond_signal failed");
    if( (*barrier) == 0 )
        error_handler(pthread_cond_signal(&this->barrier) != 0,"pthread_cond_signal failed");
    error_handler(pthread_mutex_unlock(&this->queue_mutex) != 0,"pthread_mutex_unlock failed");
}