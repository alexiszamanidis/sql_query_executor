#include "../header/header.h"
#include "../header/relation.h"
#include "../header/results.h"
#include "../header/sql_query.h"
#include "../header/file_array.h"
#include "../header/job_scheduler.h"
#include "../header/intermidiate_results.h"

job_scheduler *job_scheduler_ = new job_scheduler(2);

struct test {
    int x;
    int y;
};

void test_function(void *argument) {
    struct test *test = (struct test *)argument;
    printf("x=%d, y=%d\n", test->x, test->y);
}

void test_function_2(void *argument) {
    printf("no argument function\n");
}

int main(int argc, char **argv) {
    struct timespec begin, end;
    double time_spent;

    file_array *file_array_ = new file_array();

    clock_gettime(CLOCK_MONOTONIC, &begin);
    read_queries(file_array_);
    clock_gettime(CLOCK_MONOTONIC, &end);

    time_spent = (end.tv_sec - begin.tv_sec);
    time_spent = time_spent + (end.tv_nsec-begin.tv_nsec)/1000000000.0;
    printf("Execution time = %f\n",time_spent);

    job_scheduler_->barrier_job_scheduler();
    job_scheduler_->stop_job_scheduler();
    delete job_scheduler_;
    delete file_array_;

    return SUCCESS;
}