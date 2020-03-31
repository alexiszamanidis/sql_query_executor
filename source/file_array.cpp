#include "../header/file_array.h"

statistics::statistics(int64_t min_value, int64_t max_value, uint64_t number_of_rows, int64_t distinct_values) {
    this->min_value = min_value;
    this->max_value = max_value;
    this->number_of_rows = number_of_rows;
    this->distinct_values = distinct_values;
}

statistics::~statistics() {}

file_array::file_array() {
    uint64_t number_of_rows, number_of_columns, size, *value, *bollean_array;
    int64_t min_value, max_value, distinct_values, fd;
    char* filename = NULL;
    size_t length = 0;
    struct stat sb;

    while( getline(&filename, &length, stdin) != -1 ) {
        filename[strlen(filename)-1] = '\0';
        if( strcmp(filename,"Done") == 0 )
            break;

        fd = open(filename, O_RDONLY);
        error_handler(fd == -1, "open failed");
        error_handler( fstat(fd,&sb) == -1, "fstat failed");

        value = (uint64_t *)mmap(NULL,sb.st_size,PROT_READ|PROT_EXEC,MAP_SHARED, fd,0);

        number_of_rows = value[0];
        number_of_columns = value[1];
        // create new file node
        struct file *new_file = initialize_file(filename,number_of_rows,number_of_columns);
        // get the values from binary file and fix the file array
        for( uint j = 0 ; j < number_of_columns ; j++ ) {
            min_value = value[j*number_of_rows+2];
            max_value = value[j*number_of_rows+2];
            for( uint i = 0 ; i < number_of_rows ; i++ ) {
                new_file->array[i][j] = value[j*number_of_rows+i+2];
                // calculate min and max
                if( min_value > new_file->array[i][j] )
                    min_value = new_file->array[i][j];
                if( max_value < new_file->array[i][j] )
                    max_value = new_file->array[i][j];
            }
            // calculate the size of boolean array
            size = max_value - min_value + 1;
            if( size > BOOLEAN_SIZE )
                size = BOOLEAN_SIZE;
            // allocate boolean array
            bollean_array = my_calloc(uint64_t,size);
            error_handler(bollean_array == NULL, "calloc failed");
            distinct_values = 0;
            // find distinct values
            for( uint64_t k = 0; k < number_of_rows; k++ ) {
                if( bollean_array[max_value-new_file->array[k][j]] == 0 ) {
                    bollean_array[max_value-new_file->array[k][j]] = 1;
                    distinct_values++;
                }
            }
            statistics statistics_(min_value, max_value, number_of_rows, distinct_values);
            new_file->statistics_[j] = statistics_;
            free_pointer(&bollean_array);
        }
        this->files.push_back(new_file);
    }
    free_pointer(&filename);
}

file_array::~file_array() {
    struct file *file;
    for( uint i = 0 ; i < this->files.size() ; i++) {
        file = this->files[i];
        free_pointer(&file->name);
        free_2d_array(&file->array);
        free_pointer(&file->statistics_);
        free_pointer(&file);
    }
}

struct file *file_array::initialize_file(char *file, uint64_t number_of_rows, uint64_t number_of_columns) {
    struct file *new_file = my_malloc(struct file,1);
    error_handler(new_file == NULL, "malloc failed");
    new_file->name = my_malloc(char,strlen(file)+1);
    error_handler(new_file->name == NULL, "malloc failed");
    new_file->array = allocate_2d_array(number_of_rows,number_of_columns);
    new_file->statistics_ = my_malloc(statistics,number_of_columns);

    strcpy(new_file->name,file);
    new_file->number_of_rows = number_of_rows;
    new_file->number_of_columns = number_of_columns;

    return new_file;
}

void file_array::print_file_array() {
    struct file *file;
    std::cout << "files: " << std::endl;
    for( uint i = 0 ; i < this->files.size() ; i++) {
        file = this->files[i];
        std::cout << "filename = " << file->name << ", number of rows = "<< file->number_of_rows <<", number of columns = " << file->number_of_columns << std::endl;
        for( uint l = 0 ; l < file->number_of_columns ; l++ )
            std::cout << "statistics[" << l << "]: " << file->statistics_[l].min_value << " " << file->statistics_[l].max_value << " " << file->statistics_[l].number_of_rows << " " << file->statistics_[l].distinct_values << " " << std::endl;
        for( uint k = 0 ; k < file->number_of_rows ; k++ ) {
            for( uint l = 0 ; l < file->number_of_columns ; l++ )
                std::cout << file->array[k][l] << " ";
            std::cout << std::endl;
        }
    }
}