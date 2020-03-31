#include "../header/utilities.h"

int64_t **allocate_2d_array(int64_t rows, int64_t columns) {
    // allocate the rows
    int64_t **array = my_malloc(int64_t *,rows);
    error_handler(array == NULL, "malloc failed");
    // allocate all array elements => contiguous allocation
    int64_t *allocate_all_array_elements = my_malloc(int64_t,rows * columns);
    error_handler(array == NULL, "malloc failed");
    // fix array rows
    for (int i = 0; i < rows; i++)
        array[i] = &(allocate_all_array_elements[i * columns]);
    return array;
}

int64_t **allocate_and_initialize_2d_array(int rows, int columns, int initialize_number) {
    int64_t **array = allocate_2d_array(rows,columns);

    for ( int i = 0 ; i <  rows ; i++ )
        for ( int j = 0 ; j < columns ; j++ )
            array[i][j] = initialize_number;

    return array;
}

void print_2d_array(int64_t **array, int rows, int columns) {
    for( int i = 0 ; i < rows ; i ++) {
        for( int j = 0 ; j < columns ; j++)
            std::cout << array[i][j] << " " << std::endl;
        std::cout << std::endl;
    }
}

void free_2d_array(int64_t ***array) {
    if( *array == NULL ) return;
    free(&((*array)[0][0]));
    free(*array);
}