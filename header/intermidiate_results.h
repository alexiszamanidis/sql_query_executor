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

class intermidiate_content {
    public:
    int64_t file_index;
    int64_t predicate_relation;
    std::vector<int64_t> row_ids;

    intermidiate_content(uint64_t , uint64_t );
    ~intermidiate_content();
};

class intermidiate_result {
    public:
    std::vector<intermidiate_content *> content;
    int sorted_relations[2];                            // if we join 0.1=2.3, then we will have sorted_relations = |0,2|
    int sorted_relation_columns[2];                     //                              and sorted relation columns |1,3|

    intermidiate_result();
    intermidiate_result(int *, int *);
    ~intermidiate_result();
};

class intermidiate_results {
    public:
    std::vector<intermidiate_result *> results;

    intermidiate_results();
    ~intermidiate_results();
    void print_intermidiate_results();
};

int *search_intermidiate_results(intermidiate_results *, int64_t );
void flip_predicate(std::vector<int> );
bool join(file_array *, intermidiate_results *, std::vector<int> , std::vector<int> );
bool filter(file_array *, intermidiate_results *, std::vector<int> , std::vector<int> );
void execute_query(void *);
void read_queries(file_array *);
int64_t **allocate_and_initialize_2d_array(int , int , int );
void print_2d_array_results(int64_t **, int , int );
void print_2d_array(int64_t **, int , int );
void free_2d_array(int64_t ***, int );
void inform_results_with_null(int , int64_t **, int );

#endif