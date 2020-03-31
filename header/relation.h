#ifndef relation_h_
#define relation_h_

#include "./header.h"
#include "./results.h"
#include "./file_array.h"
#include "../thread_pool/header/job_scheduler.h"
#include "./utilities.h"

#define BUCKET_SIZE 256
#define CACHE_SIZE 64*1024
#define DUPLICATES CACHE_SIZE/sizeof(struct tuple)
#define START_BYTE 6
#define MODULO 0
#define BREAK_HISTOGRAMS 2

struct tuple {
    uint64_t row_id;
    uint64_t value;
};

struct histogram_indexing {
    int size;
    int indexes[BUCKET_SIZE];
};

struct sort_node {
    uint64_t start;
    uint64_t end;
    int byte;
};

struct join_partition {
    uint64_t *histogram;
    struct histogram_indexing histogram_indexes;
    uint64_t *prefix_sum;
};

class relation {
    public:
        tuple *tuples;
        uint64_t num_tuples;
        struct join_partition *join_partition;
    public:
        relation();
        relation(int );
        ~relation();
        void free_join_partition();
        void relation_initialize_random(int);
        void relation_initialize_with_dataset(char *);
        void create_relation_from_file(struct file *, int );
        void relation_print();
        unsigned char binary_mask(uint64_t , int );
        uint64_t *create_histogram(uint64_t , uint64_t , int ,struct histogram_indexing *);
        uint64_t *create_prefix_sum(uint64_t *, uint64_t );
        int partition(int , int );
        void quick_sort(int , int );
        void fill_new_relation(relation *, uint64_t *, uint64_t , uint64_t , int );
        uint64_t get_tuple_value(uint64_t );
        uint64_t get_tuple_row_id(uint64_t );
        uint64_t get_number_of_tuples();
        void get_range(uint64_t , uint64_t *);
};

struct sort_iterative_arguments {
    relation *R;
};

struct quick_sort_arguments {
    relation *R;
    uint64_t start;
    uint64_t end;
};

struct histogram_arguments {
    relation *R;
    uint64_t start;
    uint64_t end;
    int byte;
    int64_t *histogram;
};

struct parallel_join_arguments {
    struct relation * R;
    struct relation * S;
    results * list;
    int start_R;
    int end_R;
    int start_S;
    int end_S;
};

void parallel_join(relation *, relation *, results *);
inline int compare_tuples(const void *, const void *);
void quick_sort_job(void *);
uint64_t * sum_histograms(int64_t **, int , int , struct histogram_indexing *);
void create_histogram_for_multithread(void *);
void break_histogram_to_jobs(relation *, uint64_t , uint64_t , int ,struct histogram_indexing *, int64_t **, int *);
uint64_t *create_histogram_multithread(relation *, uint64_t ,uint64_t , int , struct histogram_indexing *);
void sort_iterative(void *);
void get_range_multithread(relation *, int64_t , int64_t *, int64_t );
void parallel_join_multithread(void *);
void fix_thread_list_results_links(results **, results *, int );
void break_join_to_jobs(relation **, relation **, results *, struct join_partition *, struct join_partition *);

#endif