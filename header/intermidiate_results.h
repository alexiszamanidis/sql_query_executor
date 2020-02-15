#ifndef intermidiate_results_h_
#define intermidiate_results_h_

#include "./header.h"
#include "./file_array.h"
#include "./sql_query.h"
#include "./job_scheduler.h"

#define RESULTS_ROWS 15
#define RESULTS_COLUMNS 5

struct execute_query_arguments {
    file_array *file_array_;
    sql_query *sql_query_;
    int64_t **results;
    int result_index;
};

void execute_query(void *);
void read_queries(file_array *);
int64_t **allocate_and_initialize_2d_array(int , int , int );
void print_2d_array_results(int64_t **, int , int );
void print_2d_array(int64_t **, int , int );
void free_2d_array(int64_t ***, int );
void inform_results_with_null(int , int64_t **, int );

#endif