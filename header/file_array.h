#ifndef file_array_h_
#define file_array_h_

#include "./header.h"
#include "./utilities.h"

#define BOOLEAN_SIZE 50000000

class statistics {
    public:
        int64_t min_value;
        int64_t max_value;
        uint64_t number_of_rows;
        int64_t distinct_values;
    public:
        statistics(int64_t , int64_t , uint64_t , int64_t );
        ~statistics();
};

struct file {
    char *name;
    uint64_t number_of_rows;
    uint64_t number_of_columns;
    int64_t **array;
    statistics *statistics_;
};

class file_array {
    public:
        std::vector<struct file *> files;
    public:
        file_array();
        ~file_array();
        struct file *initialize_file(char *, uint64_t, uint64_t);
        void print_file_array();
};

#endif