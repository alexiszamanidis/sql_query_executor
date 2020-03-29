#include "../header/utilities.h"

int64_t **allocate_and_initialize_2d_array(int rows, int columns, int initialize_number) {
    int64_t **array = my_malloc(int64_t *,rows);
    error_handler(array == NULL,"malloc failed");
    for( int i=0 ; i < rows ; i++ ) {
        array[i] = my_malloc(int64_t,columns);
        error_handler(array[i] == NULL,"malloc failed");
    }

    for ( int i = 0 ; i <  rows ; i++ )
        for ( int j = 0 ; j < columns ; j++ )
            array[i][j] = initialize_number;

    return array;
}

void free_2d_array(int64_t ***array, int rows) {
    for( int i = 0 ; i < rows; i++ )
        free_pointer(&(*array)[i]);
    free_pointer(&*array);
}

void print_2d_array(int64_t **array, int rows, int columns) {
    for( int i = 0 ; i < rows ; i ++) {
        for( int j = 0 ; j < columns ; j++)
            std::cout << array[i][j] << " " << std::endl;
        std::cout << std::endl;
    }
}