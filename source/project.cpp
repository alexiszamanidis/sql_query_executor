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
    int barrier = 0;

    file_array *file_array_ = new file_array();

    read_queries(file_array_);

    job_scheduler_->dynamic_barrier_job_scheduler(&barrier);
    job_scheduler_->stop_job_scheduler();
    delete job_scheduler_;
    delete file_array_;

    return SUCCESS;
}