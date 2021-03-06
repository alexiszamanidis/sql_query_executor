#ifndef results_h_
#define results_h_

#define NUMBER_OF_TUPLES_IN_BUCKET ((1024*1024)-2*(sizeof(int))-sizeof(struct bucket *))/sizeof(struct row_key_tuple)

#include "./header.h"

struct row_key_tuple {
    uint64_t row_key_1;
    uint64_t row_key_2;
};

struct bucket {
    struct row_key_tuple *tuples;
    struct bucket *next_bucket;
    int current_size;
    int max_size;
};

class results {
    public:
        struct bucket *head;
        struct bucket *tail;
        int64_t number_of_buckets;
        int64_t total_size;
    public:
        results();
        ~results();
        void insert_tuple(uint64_t , uint64_t );
        void results_print();
        struct bucket *results_initialize_bucket();
};

results **initialize_2d_results(int );
void print_2d_results(results **, int );
void free_2d_results(results **, int );

#endif