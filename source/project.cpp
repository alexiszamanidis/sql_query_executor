#include "../header/header.h"
#include "../header/relation.h"
#include "../header/results.h"
#include "../header/sql_query.h"
#include "../header/file_array.h"
#include "../thread_pool/header/job_scheduler.h"
#include "../header/intermidiate_results.h"

struct job_scheduler *job_scheduler = initialize_job_scheduler(3);

int main(int argc, char **argv) {
    struct timespec begin, end;
    double time;

    file_array *file_array_ = new file_array();

    clock_gettime(CLOCK_MONOTONIC, &begin);
    read_queries(file_array_);
    clock_gettime(CLOCK_MONOTONIC, &end);

    time_spent(time,begin,end);
    printf("Execution time = %f\n",time);

    barrier_job_scheduler(job_scheduler);
    free_job_scheduler(job_scheduler);
    delete file_array_;

    return SUCCESS;
}