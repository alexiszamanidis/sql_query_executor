#ifndef relation_h_
#define relation_h_

#include "./header.h"

struct tuple {
    uint64_t row_id;
    uint64_t value;
};

class relation {
    private:
        tuple *tuples;
        int64_t num_tuples;
    public:
        relation();
        ~relation();
        void relation_initialize_random(int);
        void relation_print();
        void relation_initialize_with_dataset(char *);
};

#endif