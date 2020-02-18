#ifndef relation_h_
#define relation_h_

#include "./header.h"
#include "../header/results.h"
#include "../header/file_array.h"
#include "../header/intermidiate_results.h"

#define BUCKET_SIZE 256
#define CACHE_SIZE 64*1024
#define DUPLICATES CACHE_SIZE/sizeof(struct tuple)

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

class relation {
    private:
        tuple *tuples;
        uint64_t num_tuples;
    public:
        relation();
        ~relation();
        void relation_initialize_random(int);
        void create_relation_from_file(struct file *, int );
        void create_relation_from_intermidiate_results_for_join(struct file *, intermidiate_results *, int , int );
        void relation_print();
        void relation_initialize_with_dataset(char *);
        unsigned char binary_mask(uint64_t , int );
        uint64_t *create_histogram(uint64_t , uint64_t , int ,struct histogram_indexing *);
        uint64_t *create_prefix_sum(uint64_t *, uint64_t );
        void swap_tuples(int , int );
        int partition(int , int );
        void quick_sort(int , int );
        void fill_new_relation(relation *, uint64_t *, uint64_t , uint64_t , int );
        void sort_iterative();
        uint64_t get_tuple_value(uint64_t );
        uint64_t get_tuple_row_id(uint64_t );
        uint64_t get_number_of_tuples();
        void get_range(uint64_t , uint64_t *);
};

void parallel_join(relation *, relation *, results *);

#endif