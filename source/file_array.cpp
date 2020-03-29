#include "../header/file_array.h"

file_array::file_array() {
    uint64_t number_of_rows, number_of_columns;
    char* filename=NULL;
    size_t length=0;

    while( getline(&filename, &length, stdin) != -1 ) {
        filename[strlen(filename)-1] = '\0';
        if( strcmp(filename,"Done") == 0 )
            break;

        int fd = open(filename, O_RDONLY);
        error_handler(fd == -1, "open failed");

        struct stat sb;
        error_handler( fstat(fd,&sb) == -1, "fstat failed");

        uint64_t *value = (uint64_t *)mmap(NULL,sb.st_size,PROT_READ|PROT_EXEC,MAP_SHARED, fd,0);

        number_of_rows = value[0];
        number_of_columns = value[1];

        // create new file node
        struct file *new_file = initialize_file(filename,number_of_rows,number_of_columns);
        // get the values from binary file and fix the file array
        for( uint i = 2 ; i <sb.st_size/sizeof *value ; i++)
            new_file->array[i-2] = value[i];

        this->files.push_back(new_file);
    }
    free_pointer(&filename);
}

file_array::~file_array() {
    struct file *file;
    for( uint i = 0 ; i < this->files.size() ; i++) {
        file = this->files[i];
        free_pointer(&file->name);
        free_pointer(&file->array);
        free_pointer(&file);
    }
}

struct file *file_array::initialize_file(char *file, uint64_t number_of_rows, uint64_t number_of_columns) {
    struct file *new_file = my_malloc(struct file,1);
    error_handler(new_file == NULL, "malloc failed");
    new_file->name = my_malloc(char,strlen(file)+1);
    error_handler(new_file->name == NULL, "malloc failed");
    new_file->array = my_malloc(int64_t,number_of_rows * number_of_columns);
    error_handler(new_file->array == NULL, "malloc failed");
    
    strcpy(new_file->name,file);
    new_file->number_of_rows = number_of_rows;
    new_file->number_of_columns = number_of_columns;

    return new_file;
}

void file_array::print_file_array() {
    struct file *file;
    printf("files: \n");
    for( uint i = 0 ; i < this->files.size() ; i++) {
        file = this->files[i];
        printf("filename = %s, number of rows = %ld, number of columns = %ld\n", file->name,file->number_of_rows,file->number_of_columns);
        for( uint k = 0 ; k < file->number_of_rows ; k++ ) {
            for( uint l = 0 ; l < file->number_of_columns ; l++ )
                printf("%ld ",file->array[ l*file->number_of_columns + k ]);
            printf("\n");
        }
    }
}