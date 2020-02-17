#include "../header/intermidiate_results.h"

intermidiate_filter::intermidiate_filter(uint64_t file_index, uint64_t predicate_relation) {
    this->file_index = file_index;
    this->predicate_relation = predicate_relation;
}

intermidiate_filter::~intermidiate_filter() {}

intermidiate_join::intermidiate_join(uint64_t *file_index, uint64_t *predicate_relation) {
    this->file_index[0] = file_index[0];
    this->file_index[1] = file_index[1];
    this->predicate_relation[0] = predicate_relation[0];
    this->predicate_relation[1] = predicate_relation[1];
    this->sorted_relations[0] = -1;
    this->sorted_relations[1] = -1;
    this->sorted_relation_columns[0] = -1;
    this->sorted_relation_columns[1] = -1;
}

intermidiate_join::~intermidiate_join() {}

intermidiate_results::intermidiate_results() {}

intermidiate_results::~intermidiate_results() {
    intermidiate_filter *intermidiate_filter_results = NULL;
    uint size = this->filter_results.size();
    for( uint i = 0 ; i < size ; i++ ) {
        intermidiate_filter_results = this->filter_results[0];
        this->filter_results.erase(this->filter_results.begin());
        delete intermidiate_filter_results;
    }
}

bool join(file_array *file_array, intermidiate_results *intermidiate_results_, std::vector<int> relations, std::vector<int> predicate) {
    return true;
}

uint search_intermidiate_results_filters(intermidiate_results *intermidiate_results_, int64_t predicate_relation) {
    for( uint i = 0 ; i < intermidiate_results_->filter_results.size() ; i++ )
        if( intermidiate_results_->filter_results[i]->predicate_relation == predicate_relation )
            return i;
    return -1;
}

bool filter(file_array *file_array, intermidiate_results *intermidiate_results_, std::vector<int> relations, std::vector<int> predicate) {
    intermidiate_filter *filter_results = new intermidiate_filter(relations[predicate[ROW_A]],predicate[ROW_A]), *intermidiate_filter_results = NULL;

    int64_t file_index = relations[predicate[ROW_A]], predicate_relation = predicate[ROW_A];
    int64_t column_a = predicate[COLUMN_A], filter_number = predicate[ROW_B], column_b = predicate[COLUMN_B];
    int64_t operator_ = predicate[OPERATOR];
    int filter_index = search_intermidiate_results_filters(intermidiate_results_,predicate_relation);

    struct file *file = file_array->files[file_index];

    // if the relation does not exist in intermidiate results
    if( filter_index == -1 ) {
        if( operator_ == EQUAL ) {
            for( uint i = 0 ; i < file->number_of_rows ; i++ )
                if( column_b == -1 && file->array[column_a*file->number_of_rows + i] == filter_number )
                    filter_results->row_ids.push_back(i);
                else if( column_b != -1 && file->array[column_a*file->number_of_rows + i] == file->array[column_b*file->number_of_rows + i] )
                    filter_results->row_ids.push_back(i);
        }
        else if( operator_ == GREATER ) {
            for( uint i = 0 ; i < file->number_of_rows ; i++ )
                if( column_b == -1 && file->array[column_a*file->number_of_rows + i] > filter_number )
                    filter_results->row_ids.push_back(i);
                else if( column_b != -1 && file->array[column_a*file->number_of_rows + i] > file->array[column_b*file->number_of_rows + i] )
                    filter_results->row_ids.push_back(i);
        }
        else {	// LESS
            for( uint i = 0 ; i < file->number_of_rows ; i++ )
                if( column_b == -1 && file->array[column_a*file->number_of_rows + i] < filter_number )
                    filter_results->row_ids.push_back(i);
                else if( column_b != -1 && file->array[column_a*file->number_of_rows + i] < file->array[column_b*file->number_of_rows + i] )
                    filter_results->row_ids.push_back(i);
        }
    }
    // if the relation exists in intermidiate results
    else {
        intermidiate_filter_results = intermidiate_results_->filter_results[filter_index];

        if( operator_ == EQUAL ) {
            for( uint i = 0 ; i < intermidiate_filter_results->row_ids.size() ; i++ )
                if( column_b == -1 && file->array[column_a*file->number_of_rows+intermidiate_filter_results->row_ids[i]] == filter_number )
                    filter_results->row_ids.push_back(intermidiate_filter_results->row_ids[i]);
                else if( column_b != -1 && file->array[column_a*file->number_of_rows+intermidiate_filter_results->row_ids[i]] == file->array[column_b*file->number_of_rows+intermidiate_filter_results->row_ids[i]])
                    filter_results->row_ids.push_back(intermidiate_filter_results->row_ids[i]);
        }
        else if( operator_ == GREATER ) {
            for( uint i = 0 ; i < intermidiate_filter_results->row_ids.size() ; i++ )
                if( column_b == -1 && file->array[column_a*file->number_of_rows+intermidiate_filter_results->row_ids[i]] > filter_number )
                    filter_results->row_ids.push_back(intermidiate_filter_results->row_ids[i]);
                else if( column_b != -1 && file->array[column_a*file->number_of_rows+intermidiate_filter_results->row_ids[i]] > file->array[column_b*file->number_of_rows+intermidiate_filter_results->row_ids[i]])
                    filter_results->row_ids.push_back(intermidiate_filter_results->row_ids[i]);
        }
        else {	// LESS
            for( uint i = 0 ; i < intermidiate_filter_results->row_ids.size() ; i++ )
                if( column_b == -1 && file->array[column_a*file->number_of_rows+intermidiate_filter_results->row_ids[i]] < filter_number )
                    filter_results->row_ids.push_back(intermidiate_filter_results->row_ids[i]);
                else if( column_b != -1 && file->array[column_a*file->number_of_rows+intermidiate_filter_results->row_ids[i]] < file->array[column_b*file->number_of_rows+intermidiate_filter_results->row_ids[i]])
                    filter_results->row_ids.push_back(intermidiate_filter_results->row_ids[i]);
        }
        intermidiate_results_->filter_results.erase(intermidiate_results_->filter_results.begin()+filter_index);
        delete intermidiate_filter_results;
    }

    if( filter_results->row_ids.size() == 0 )
        return false;
    else {
        intermidiate_results_->filter_results.push_back(filter_results);
        return true;
    }
}

void execute_query(void *argument) {
    struct execute_query_arguments *execute_query_arguments = (struct execute_query_arguments *)argument;
    intermidiate_results *intermidiate_results_ = new intermidiate_results();
    bool return_value;

    // execute filters
    for( uint i = 0 ; i < execute_query_arguments->sql_query_->filters.size() ; i++ ) {
        return_value = filter(execute_query_arguments->file_array_,intermidiate_results_,execute_query_arguments->sql_query_->relations,execute_query_arguments->sql_query_->filters[i]);
        if( return_value == false ) {
            inform_results_with_null(execute_query_arguments->sql_query_->projections.size(),execute_query_arguments->results, execute_query_arguments->result_index);
            delete intermidiate_results_;
            delete execute_query_arguments->sql_query_;
        }
    }

    std::cout << intermidiate_results_->filter_results[0]->row_ids.size() << std::endl;

    // execute joins
    for( uint i = 0 ; i < execute_query_arguments->sql_query_->joins.size() ; i++ ) {
        return_value = join(execute_query_arguments->file_array_,intermidiate_results_,execute_query_arguments->sql_query_->relations,execute_query_arguments->sql_query_->joins[i]);
        if( return_value == false ) {
            inform_results_with_null(execute_query_arguments->sql_query_->projections.size(),execute_query_arguments->results, execute_query_arguments->result_index);
            delete intermidiate_results_;
            delete execute_query_arguments->sql_query_;
        }
    }

    delete intermidiate_results_;
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