#include "../header/intermidiate_results.h"

void execute_query(void *argument) {
    struct execute_query_arguments *execute_query_arguments = (struct execute_query_arguments *)argument;
    std::cout << execute_query_arguments->result_index << std::endl;

    delete execute_query_arguments->sql_query_;
}

void read_queries(file_array *file_array) {
    char* query = NULL;
    size_t length = 0;
    int result_index, job_barrier = 0;
    bool stop = false;
    int64_t **results = allocate_and_initialize_2d_array(RESULTS_ROWS,RESULTS_COLUMNS,-1);
    extern job_scheduler *job_scheduler_;

    while( true ) {
        result_index = 0;
         while( getline(&query, &length, stdin) != -1 ) {
            query[strlen(query)-1]='\0';
            if( strcmp(query,"F")==0 )
                break;
            else if( strcmp(query,"Done")==0 ) {
                stop = true;
                break;
            }
            sql_query *sql_query_ = new sql_query(query);

            struct execute_query_arguments *execute_query_arguments = (struct execute_query_arguments *)malloc(sizeof(struct execute_query_arguments));
            error_handler(execute_query_arguments == NULL,"malloc failed");
            *execute_query_arguments = (struct execute_query_arguments){ .file_array_ = file_array, .sql_query_ = sql_query_, .results = results, .result_index = result_index};
            job_scheduler_->schedule_job_scheduler(execute_query,execute_query_arguments,&job_barrier);

            result_index++;
        }
        // wait until the whole batch ends
        job_scheduler_->dynamic_barrier_job_scheduler(&job_barrier);
        // print batch results
        print_2d_array_results(results,result_index,RESULTS_COLUMNS);
        // if all batches were executed, leave
        if( stop == true )
            break;
    }
    free(query);
    free_2d_array(&results,RESULTS_ROWS);
}


int64_t **allocate_and_initialize_2d_array(int rows, int columns, int initialize_number) {
    int64_t **array = (int64_t **)malloc(rows*sizeof(int64_t *)); 
    error_handler(array == NULL,"malloc failed");
    for( int i=0 ; i < rows ; i++ ) {
        array[i] = (int64_t *)malloc(columns*sizeof(int64_t));
        error_handler(array[i] == NULL,"malloc failed");
    }

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

void print_2d_array_results(int64_t **array, int rows, int columns) {
    for( int i = 0 ; i < rows ; i ++) {
        for( int j = 0 ; j < columns ; j++) {
            if( array[i][j] == -1 )
                break;
            else if( array[i][j] == -2 )
                std::cout << "NULL ";
            else
                std::cout << array[i][j] << " ";
            array[i][j] = -1;
        }
        std::cout << std::endl;
    }
}

void free_2d_array(int64_t ***array, int rows) {
    for( int i = 0 ; i < rows; i++ )
        free((*array)[i]);
    free(*array);
}

void inform_results_with_null(int number_of_nulls, int64_t **results, int result_index) {
    for( int i = 0 ; i < number_of_nulls ; i++)
        results[result_index][i] = -2;
}