#include "../header/header.h"
#include "../header/relation.h"
#include "../header/results.h"
#include "../header/sql_query.h"
#include "../header/file_array.h"
#include "../header/job_scheduler.h"

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

    for( int i = 0 ; i < 10 ; i ++) {
        struct test *test = (struct test *)malloc(sizeof(struct test));
        error_handler(test == NULL,"malloc failed");
        test->x = i;
        test->y = i;
        job_scheduler_->schedule_job_scheduler(test_function,test,&barrier);
        job_scheduler_->schedule_job_scheduler(test_function_2,NULL,&barrier);
    }
    
    job_scheduler_->dynamic_barrier_job_scheduler(&barrier);
    job_scheduler_->stop_job_scheduler();
    delete job_scheduler_;

    return SUCCESS;
}