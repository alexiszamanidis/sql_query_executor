#ifndef intermidiate_results_h_
#define intermidiate_results_h_

#include "./header.h"
#include "./file_array.h"
#include "./sql_query.h"
#include "../thread_pool/header/job_scheduler.h"
#include "./relation.h"
#include "./utilities.h"

#define RESULTS_ROWS 15
#define RESULTS_COLUMNS 5
#define RESERVE_SIZE 10

struct execute_query_arguments {
    file_array *file_array_;
    sql_query *sql_query_;
    int64_t **results;
    int result_index;
};

class intermidiate_content {
    public:
    int64_t file_index;
    int64_t predicate_relation;
    std::vector<int64_t> row_ids;

    intermidiate_content(int64_t , int64_t , int );
    ~intermidiate_content();
};

class intermidiate_result {
    public:
    std::vector<intermidiate_content *> content;
    int sorted_relations[2];                            // if we join 0.1=2.3, then we will have sorted_relations = |0,2|
    int sorted_relation_columns[2];                     //                              and sorted relation columns |1,3|

    intermidiate_result();
    intermidiate_result(std::vector<int> );
    ~intermidiate_result();
    void inform_intermidiate_result_sort_fields(std::vector<int> );
};

class intermidiate_results {
    public:
    std::vector<intermidiate_result *> results;

    intermidiate_results();
    ~intermidiate_results();
    void print_intermidiate_results();
};

struct projection_sum_results_arguments {
    file_array *file_array_;
    int64_t file_index;
    intermidiate_results *intermidiate_results_;
    int *intermidiate_result_index;
    int64_t **results;
    int result_index;
    int64_t column;
    uint result_column;
};

int *search_intermidiate_results(intermidiate_results *, int64_t );
void flip_predicate(std::vector<int> &);
bool join(file_array *, intermidiate_results *, std::vector<int> , std::vector<int> );
relation *create_relation_from_intermidiate_results_for_join(struct file *, intermidiate_results *, int *, int );
void calculate_join_partition(relation *);
results *sort_join_calculation(relation *, relation *, intermidiate_results *, std::vector<int> );
bool none_relation_in_mid_results(struct file_array *, intermidiate_results *, std::vector<int> , std::vector<int> );
bool both_relations_in_mid_results(struct file_array *, intermidiate_results *, std::vector<int> , std::vector<int> , int *, int *);
bool only_one_relation_in_mid_results(struct file_array *, intermidiate_results *, std::vector<int> , std::vector<int> , int *);
bool filter(file_array *, intermidiate_results *, std::vector<int> , std::vector<int> );
void projection_sum_results_job(void *argument);
void projection_sum_results(file_array *, intermidiate_results *, sql_query *, int64_t **, int );
void execute_query(void *);
void read_queries(file_array *);
void print_2d_array_results(int64_t **, int , int );
void inform_results_with_null(int , int64_t **, int );

#endif
