#ifndef file_array_h_
#define file_array_h_

#include "./header.h"

struct file {
    char *name;
    uint64_t number_of_rows;
    uint64_t number_of_columns;
    int64_t *array;
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