#include "../header/intermidiate_results.h"

intermidiate_content::intermidiate_content(int64_t file_index, int64_t predicate_relation, int reserve_size) {
    this->file_index = file_index;
    this->predicate_relation = predicate_relation;
    this->row_ids.reserve(reserve_size);
}

intermidiate_content::~intermidiate_content() {}

intermidiate_result::intermidiate_result() {
    this->sorted_relations[0] = -1;
    this->sorted_relations[1] = -1;
    this->sorted_relation_columns[0] = -1;
    this->sorted_relation_columns[1] = -1;
}

intermidiate_result::intermidiate_result(std::vector<int> predicate) {
    this->content.reserve(RESERVE_SIZE);
    this->sorted_relations[0] = predicate[RELATION_A];
    this->sorted_relations[1] = predicate[RELATION_B];
    this->sorted_relation_columns[0] = predicate[COLUMN_A];
    this->sorted_relation_columns[1] = predicate[COLUMN_B];
}

void intermidiate_result::inform_intermidiate_result_sort_fields(std::vector<int> predicate) {
    this->content.reserve(RESERVE_SIZE);
    this->sorted_relations[0] = predicate[RELATION_A];
    this->sorted_relations[1] = predicate[RELATION_B];
    this->sorted_relation_columns[0] = predicate[COLUMN_A];
    this->sorted_relation_columns[1] = predicate[COLUMN_B];
}

intermidiate_result::~intermidiate_result() {
    intermidiate_content *intermidiate_content_ = NULL;
    int content_size = this->content.size();

    for( int j = 0 ; j < content_size ; j++ ) {
        intermidiate_content_ = this->content[0];
        this->content.erase(this->content.begin());
        delete intermidiate_content_;
    }
}

intermidiate_results::intermidiate_results() {
    this->results.reserve(RESERVE_SIZE);
}

intermidiate_results::~intermidiate_results() {
    intermidiate_result *intermidiate_result_ = NULL;
    int result_size = (int)this->results.size();

    for( int i = 0 ; i < result_size ; i++ ) {
        intermidiate_result_ = this->results[0];
        this->results.erase(this->results.begin());
        delete intermidiate_result_;
    }
}

void intermidiate_results::print_intermidiate_results() {
    std::cout << "Intermidiate Results:" << std::endl;
    for( int i = 0 ; i < (int)this->results.size() ; i++ ) {
        std::cout << "Intermidiate Result[" << i << "]" << std::endl;
        std::cout << "sorted_relations " << this->results[i]->sorted_relations[0] << " " << this->results[i]->sorted_relations[1] << std::endl;
        std::cout << "sorted_relation_columns " << this->results[i]->sorted_relation_columns[0] << " " << this->results[i]->sorted_relation_columns[0] << std::endl;
        for( int j = 0 ; j < (int)this->results[i]->content.size() ; j++ ) {
            std::cout << "file_index:" << this->results[i]->content[j]->file_index << ", predicate_relation:" << this->results[i]->content[j]->predicate_relation;
            std::cout << ", row_ids:" << this->results[i]->content[j]->row_ids.size() << std::endl;
        /*
            for( int k = 0 ; k < (int)this->results[i]->content[j]->row_ids.size() ; k++ )
                std::cout << this->results[i]->content[j]->row_ids[k] << " ";
            std::cout << std::endl;
        */
        }
    }
}

int *search_intermidiate_results(intermidiate_results *intermidiate_results_, int64_t predicate_relation) {
    int *intermidiate_indexes = NULL;
    for( int i = 0 ; i < (int)intermidiate_results_->results.size() ; i++ ) {
        for( int j = 0 ; j < (int)intermidiate_results_->results[i]->content.size() ; j++ ) {
            if( intermidiate_results_->results[i]->content[j]->predicate_relation == predicate_relation ) {
                intermidiate_indexes = (int *)malloc(2*sizeof(int));
                intermidiate_indexes[0] = i;
                intermidiate_indexes[1] = j;
                return intermidiate_indexes;
            }
        }
    }
    return intermidiate_indexes;
}

void flip_predicate(std::vector<int> &predicate) {
    int temp;
    temp = predicate[RELATION_A];
    predicate[RELATION_A] = predicate[RELATION_B];
    predicate[RELATION_B] = temp;
    temp = predicate[COLUMN_A];
    predicate[COLUMN_A] = predicate[COLUMN_B];
    predicate[COLUMN_B] = temp;
}

bool join(file_array *file_array, intermidiate_results *intermidiate_results_, std::vector<int> relations, std::vector<int> predicate) {
    int *intermidiate_result_index_a = search_intermidiate_results(intermidiate_results_,predicate[RELATION_A]);
    int *intermidiate_result_index_b = search_intermidiate_results(intermidiate_results_,predicate[RELATION_B]);
    bool return_value = false;

    // if none of the relations are in mid results
    if( (intermidiate_result_index_a == NULL) && (intermidiate_result_index_b == NULL) )
        return_value = none_relation_in_mid_results(file_array, intermidiate_results_, predicate, relations);
    // if only the first relation is in mid results
    else if ( (intermidiate_result_index_a != NULL) && (intermidiate_result_index_b == NULL) )
        return_value = only_one_relation_in_mid_results(file_array,intermidiate_results_,predicate,relations,intermidiate_result_index_a);
    // if only the second relation is in mid results
    else if ( (intermidiate_result_index_a == NULL) && (intermidiate_result_index_b != NULL) ){
        flip_predicate(predicate);
        return_value = only_one_relation_in_mid_results(file_array,intermidiate_results_,predicate,relations,intermidiate_result_index_b);;
    }
    // if both relations are in mid results
    else
        return_value = both_relations_in_mid_results(file_array,intermidiate_results_,predicate,relations,intermidiate_result_index_a,intermidiate_result_index_b);

    if( intermidiate_result_index_a != NULL )
        free(intermidiate_result_index_a);
    if( intermidiate_result_index_b != NULL )
        free(intermidiate_result_index_b);

    return return_value;
}

relation *create_relation_from_intermidiate_results_for_join(struct file *file, intermidiate_results *intermidiate_results_, int *intermidiate_result_index, int column) {
    intermidiate_content *intermidiate_content_results = intermidiate_results_->results[intermidiate_result_index[0]]->content[intermidiate_result_index[1]];
    relation *R = new relation(intermidiate_content_results->row_ids.size());

    for( uint64_t i = 0 ; i < intermidiate_content_results->row_ids.size() ; i++ ) {
        R->tuples[i].row_id = i;
        R->tuples[i].value = file->array[column*file->number_of_rows+intermidiate_content_results->row_ids[i]];
    }

    return R;
}

results *sort_join_calculation(relation *R, relation *S, intermidiate_results *intermidiate_results_, std::vector<int> predicate) {
    extern job_scheduler *job_scheduler_;
    bool flag_r = false, flag_s = false;
    int job_barrier = 0;
    results *results_ = new results();

    for( int i = 0 ; i < (int)intermidiate_results_->results.size() ; i++ ) {
        for( int j = 0 ; j < 2 ; j++ ) {
            if((intermidiate_results_->results[i]->sorted_relations[j] == predicate[RELATION_A]) && (intermidiate_results_->results[i]->sorted_relation_columns[j] == predicate[COLUMN_A])) {
                flag_r = true;
                break;
            }
        }
    }

    for( int i = 0 ; i < (int)intermidiate_results_->results.size() ; i++ ) {
        for( int j = 0 ; j < 2 ; j++ ) {
            if((intermidiate_results_->results[i]->sorted_relations[j] == predicate[RELATION_B]) && (intermidiate_results_->results[i]->sorted_relation_columns[j] == predicate[COLUMN_B])) {
                flag_s = true;
                break;
            }
        }
    }

    if( flag_r == false ) {
        struct sort_iterative_arguments *sort_iterative_arguments_r = (struct sort_iterative_arguments *)malloc(sizeof(struct sort_iterative_arguments));
        error_handler(sort_iterative_arguments_r == NULL,"malloc failed");
        *sort_iterative_arguments_r = (struct sort_iterative_arguments){ .R = R };
        job_scheduler_->schedule_job_scheduler(sort_iterative,sort_iterative_arguments_r,&job_barrier);
    //    sort_iterative(sort_iterative_arguments_r);
    //    free(sort_iterative_arguments_r);
    }

    if( flag_s == false ) {
        struct sort_iterative_arguments *sort_iterative_arguments_s = (struct sort_iterative_arguments *)malloc(sizeof(struct sort_iterative_arguments));
        error_handler(sort_iterative_arguments_s == NULL,"malloc failed");
        *sort_iterative_arguments_s = (struct sort_iterative_arguments){ .R = S };
        job_scheduler_->schedule_job_scheduler(sort_iterative,sort_iterative_arguments_s,&job_barrier);
    //    sort_iterative(sort_iterative_arguments_s);
    //    free(sort_iterative_arguments_s);
    }

    job_scheduler_->dynamic_barrier_job_scheduler(&job_barrier);

    parallel_join(R,S,results_);

    return results_;
}

bool none_relation_in_mid_results(struct file_array *file_array, intermidiate_results *intermidiate_results_, std::vector<int> predicate, std::vector<int> relations) {
    intermidiate_result *intermidiate_result_ = NULL;
    intermidiate_content *new_intermidiate_result_a = NULL, *new_intermidiate_result_b = NULL;
    relation *R = new relation(), *S = new relation();
    int64_t file_index_a = relations[predicate[RELATION_A]], file_index_b = relations[predicate[RELATION_B]];
    int64_t predicate_relation_a = predicate[RELATION_A], predicate_relation_b = predicate[RELATION_B];
    int64_t column_a = predicate[COLUMN_A], column_b = predicate[COLUMN_B];
    struct file *file_a = file_array->files[file_index_a], *file_b = file_array->files[file_index_b];
    results *results_ = NULL;

    // initialize structures for join and do it
    R->create_relation_from_file(file_a, column_a);
    S->create_relation_from_file(file_b, column_b);
    results_ = sort_join_calculation(R,S,intermidiate_results_,predicate);

    // if there was no results then free structures and return false
    if( results_->total_size == 0 ) {
        delete R;
        delete S;
        delete results_;
        return false;
    }

    new_intermidiate_result_a = new intermidiate_content(file_index_a,predicate_relation_a,results_->total_size);
    new_intermidiate_result_b = new intermidiate_content(file_index_b,predicate_relation_b,results_->total_size);

    struct bucket *temp_bucket = results_->head;
    for( int i = 0 ; i < results_->number_of_buckets ; i++) {
        for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
            new_intermidiate_result_a->row_ids.push_back(temp_bucket->tuples[j].row_key_1);
            new_intermidiate_result_b->row_ids.push_back(temp_bucket->tuples[j].row_key_2);
        }
        temp_bucket = temp_bucket->next_bucket;
    }

    intermidiate_result_ = new intermidiate_result(predicate);
    intermidiate_result_->content.push_back(new_intermidiate_result_a);
    intermidiate_result_->content.push_back(new_intermidiate_result_b);
    intermidiate_results_->results.push_back(intermidiate_result_);

    // free structures that were used for the parallel join
    delete R;
    delete S;
    delete results_;
    return true;
}

bool both_relations_in_mid_results(struct file_array *file_array, intermidiate_results *intermidiate_results_, std::vector<int> predicate, std::vector<int> relations, int *intermidiate_result_index_a, int *intermidiate_result_index_b) {
    int64_t file_index_a = relations[predicate[RELATION_A]], file_index_b = relations[predicate[RELATION_B]];
    int64_t predicate_relation_a = predicate[RELATION_A], predicate_relation_b = predicate[RELATION_B];
    int64_t column_a = predicate[COLUMN_A], column_b = predicate[COLUMN_B];
    struct file *file_a = file_array->files[file_index_a], *file_b = file_array->files[file_index_b];
    intermidiate_content *new_intermidiate_result_a = NULL, *new_intermidiate_result_b = NULL;
    relation *R = NULL, *S = NULL;
    results *results_ = NULL;
    
    intermidiate_content *intermidiate_content_a = intermidiate_results_->results[intermidiate_result_index_a[0]]->content[intermidiate_result_index_a[1]];
    intermidiate_content *intermidiate_content_b = intermidiate_results_->results[intermidiate_result_index_b[0]]->content[intermidiate_result_index_b[1]];

    R = create_relation_from_intermidiate_results_for_join(file_a,intermidiate_results_,intermidiate_result_index_a,column_a);
    S = create_relation_from_intermidiate_results_for_join(file_b,intermidiate_results_,intermidiate_result_index_b,column_b);

    // if they are already joined
    if( intermidiate_result_index_a[0] == intermidiate_result_index_b[0] ) {
        std::vector<int64_t> indexes;
        // calculate the indexes when R.value = S.value
        for( uint i = 0 ; i < R->num_tuples ; i++ )
            if(  R->tuples[i].value == S->tuples[i].value )
                indexes.push_back(R->tuples[i].row_id);

        if( indexes.size() == 0 ) {
            delete R;
            delete S;
            return false;
        }

        new_intermidiate_result_a = new intermidiate_content(file_index_a,predicate_relation_a,indexes.size());
        new_intermidiate_result_b = new intermidiate_content(file_index_b,predicate_relation_b,indexes.size());

        for( uint i = 0 ; i < indexes.size() ; i++ ) {
            int index = indexes[i];
            int value_a = intermidiate_content_a->row_ids[index];
            int value_b = intermidiate_content_b->row_ids[index];
            new_intermidiate_result_a->row_ids.push_back(value_a);
            new_intermidiate_result_b->row_ids.push_back(value_b);
        }

        // delete old relations and push new relations
        intermidiate_result *intermidiate_result_ = intermidiate_results_->results[intermidiate_result_index_a[0]];
        intermidiate_result_->content.erase(intermidiate_result_->content.begin()+intermidiate_result_index_a[1]);
        delete intermidiate_content_a;
        intermidiate_result_->content.insert(intermidiate_result_->content.begin()+intermidiate_result_index_a[1], new_intermidiate_result_a);
        intermidiate_result_->content.erase(intermidiate_result_->content.begin()+intermidiate_result_index_b[1]);
        delete intermidiate_content_b;
        intermidiate_result_->content.insert(intermidiate_result_->content.begin()+intermidiate_result_index_b[1], new_intermidiate_result_b);
    
        // synchronize old joined relations
        for( int i = 0 ; i < (int)intermidiate_result_->content.size() ; i++ ) {
            intermidiate_content *temp_intermidiate_content_ = intermidiate_result_->content[i];
            if( (i == intermidiate_result_index_a[1]) || (i == intermidiate_result_index_b[1]) )
                continue;
            else {
                intermidiate_content *new_intermidiate_result_c = new intermidiate_content(temp_intermidiate_content_->file_index,temp_intermidiate_content_->predicate_relation,indexes.size());
                for( uint j = 0 ; j < indexes.size() ; j++ ) {
                    int index = indexes[j];
                    int value = temp_intermidiate_content_->row_ids[index];
                    new_intermidiate_result_c->row_ids.push_back(value);
                }
                intermidiate_result_->content.erase(intermidiate_result_->content.begin()+i);
                delete temp_intermidiate_content_;
                intermidiate_result_->content.insert(intermidiate_result_->content.begin()+i, new_intermidiate_result_c);
            }
        }
    }
    else {
        results_ = sort_join_calculation(R,S,intermidiate_results_,predicate);
        
        // if there was no results then free structures and return false
        if( results_->total_size == 0 ) {
            delete R;
            delete S;
            delete results_;
            return false;
        }

        intermidiate_result *new_intermidiate_result = new intermidiate_result(predicate);

        intermidiate_result *intermidiate_result_a = intermidiate_results_->results[intermidiate_result_index_a[0]];

        // synchronize old joined relations from first intermidiate result
        for( int i = 0 ; i < (int)intermidiate_result_a->content.size() ; i++ ) {
            intermidiate_content *temp_intermidiate_content_ = intermidiate_result_a->content[i];
            struct bucket *temp_bucket = results_->head;
            intermidiate_content *new_intermidiate_result_c = new intermidiate_content(temp_intermidiate_content_->file_index,temp_intermidiate_content_->predicate_relation,results_->total_size);
            for( int k = 0 ; k < results_->number_of_buckets ; k++) {
                for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
                    int index = temp_bucket->tuples[j].row_key_1;
                    int value = temp_intermidiate_content_->row_ids[index];
                    new_intermidiate_result_c->row_ids.push_back(value);
                }
                temp_bucket = temp_bucket->next_bucket;
            }
            new_intermidiate_result->content.push_back(new_intermidiate_result_c);
        }

        intermidiate_result *intermidiate_result_b = intermidiate_results_->results[intermidiate_result_index_b[0]];

        // synchronize old joined relations from second intermidiate result
        for( int i = 0 ; i < (int)intermidiate_result_b->content.size() ; i++ ) {
            intermidiate_content *temp_intermidiate_content_ = intermidiate_result_b->content[i];
            struct bucket *temp_bucket = results_->head;
            intermidiate_content *new_intermidiate_result_c = new intermidiate_content(temp_intermidiate_content_->file_index,temp_intermidiate_content_->predicate_relation,results_->total_size);
            for( int k = 0 ; k < results_->number_of_buckets ; k++) {
                for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
                    int index = temp_bucket->tuples[j].row_key_2;
                    int value = temp_intermidiate_content_->row_ids[index];
                    new_intermidiate_result_c->row_ids.push_back(value);
                }
                temp_bucket = temp_bucket->next_bucket;
            }
            new_intermidiate_result->content.push_back(new_intermidiate_result_c);
        }

        intermidiate_results_->results.push_back(new_intermidiate_result);

        // remove old intermidiate results
        if( intermidiate_result_index_a[0] > intermidiate_result_index_b[0] ) {
            intermidiate_results_->results.erase(intermidiate_results_->results.begin()+intermidiate_result_index_a[0]);
            delete intermidiate_result_a;
            intermidiate_results_->results.erase(intermidiate_results_->results.begin()+intermidiate_result_index_b[0]);
            delete intermidiate_result_b;
        }
        else {
            intermidiate_results_->results.erase(intermidiate_results_->results.begin()+intermidiate_result_index_b[0]);
            delete intermidiate_result_b;
            intermidiate_results_->results.erase(intermidiate_results_->results.begin()+intermidiate_result_index_a[0]);
            delete intermidiate_result_a;
        }
    }

    if( results_ != NULL )
        delete results_;
    delete R;
    delete S;
    return true;
}

bool only_one_relation_in_mid_results(struct file_array *file_array, intermidiate_results *intermidiate_results_, std::vector<int> predicate, std::vector<int> relations, int *intermidiate_result_index_a) {
    int64_t file_index_a = relations[predicate[RELATION_A]], file_index_b = relations[predicate[RELATION_B]];
    int64_t predicate_relation_a = predicate[RELATION_A], predicate_relation_b = predicate[RELATION_B];
    int64_t column_a = predicate[COLUMN_A], column_b = predicate[COLUMN_B];
    struct file *file_a = file_array->files[file_index_a], *file_b = file_array->files[file_index_b];
    intermidiate_content *new_intermidiate_result_a = NULL, *new_intermidiate_result_b = NULL;
    relation *R = NULL, *S = new relation();
    results *results_ = NULL;

    // initialize structures for join and do it
    R = create_relation_from_intermidiate_results_for_join(file_a,intermidiate_results_,intermidiate_result_index_a,column_a);
    S->create_relation_from_file(file_b, column_b);
    results_ = sort_join_calculation(R,S,intermidiate_results_,predicate);

    // if there was no results then free structures and return false
    if( results_->total_size == 0 ) {
        delete R;
        delete S;
        delete results_;
        return false;
    }

    intermidiate_content *intermidiate_content_ = intermidiate_results_->results[intermidiate_result_index_a[0]]->content[intermidiate_result_index_a[1]];

    new_intermidiate_result_a = new intermidiate_content(file_index_a,predicate_relation_a,results_->total_size);
    new_intermidiate_result_b = new intermidiate_content(file_index_b,predicate_relation_b,results_->total_size);

    struct bucket *temp_bucket = results_->head;
    for( int i = 0 ; i < results_->number_of_buckets ; i++) {
        for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
            int index_a = temp_bucket->tuples[j].row_key_1;
            int value = intermidiate_content_->row_ids[index_a];
            new_intermidiate_result_a->row_ids.push_back(value);
            new_intermidiate_result_b->row_ids.push_back(temp_bucket->tuples[j].row_key_2);
        }
        temp_bucket = temp_bucket->next_bucket;
    }

    intermidiate_result *intermidiate_result_ = intermidiate_results_->results[intermidiate_result_index_a[0]];
    intermidiate_content *new_intermidiate_result_c = NULL;
    for( int i = 0 ; i < (int)intermidiate_result_->content.size() ; i++ ) {
        intermidiate_content *temp_intermidiate_content_ = intermidiate_result_->content[i];
        if( i == intermidiate_result_index_a[1] )
            continue;
        else {
            struct bucket *temp_bucket = results_->head;
            new_intermidiate_result_c = new intermidiate_content(temp_intermidiate_content_->file_index,temp_intermidiate_content_->predicate_relation,results_->total_size);
            for( int k = 0 ; k < results_->number_of_buckets ; k++) {
                for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
                    int index = temp_bucket->tuples[j].row_key_1;
                    int value = temp_intermidiate_content_->row_ids[index];
                    new_intermidiate_result_c->row_ids.push_back(value);
                }
                temp_bucket = temp_bucket->next_bucket;
            }
            intermidiate_result_->content.erase(intermidiate_result_->content.begin()+i);
            delete temp_intermidiate_content_;
            intermidiate_result_->content.insert(intermidiate_result_->content.begin()+i, new_intermidiate_result_c);
        }
    }

    new_intermidiate_result_c = intermidiate_result_->content[intermidiate_result_index_a[1]];
    intermidiate_result_->content.erase(intermidiate_result_->content.begin()+intermidiate_result_index_a[1]);
    delete new_intermidiate_result_c;
    intermidiate_result_->content.push_back(new_intermidiate_result_a);
    intermidiate_result_->content.push_back(new_intermidiate_result_b);

    intermidiate_result_->inform_intermidiate_result_sort_fields(predicate);

    delete R;
    delete S;
    delete results_;
    return true;
}

bool filter(file_array *file_array, intermidiate_results *intermidiate_results_, std::vector<int> relations, std::vector<int> predicate) {
    intermidiate_result *intermidiate_result_ = NULL;
    intermidiate_content *filter_results = NULL, *intermidiate_content_results = NULL;

    int64_t file_index = relations[predicate[RELATION_A]], predicate_relation = predicate[RELATION_A];
    int64_t column_a = predicate[COLUMN_A], filter_number = predicate[RELATION_B], column_b = predicate[COLUMN_B];
    int64_t operator_ = predicate[OPERATOR];
    int *filter_index = search_intermidiate_results(intermidiate_results_,predicate_relation);

    struct file *file = file_array->files[file_index];

    // if the relation does not exist in intermidiate results
    if( filter_index == NULL ) {
        filter_results = new intermidiate_content(relations[predicate[RELATION_A]],predicate[RELATION_A],file->number_of_rows);
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
        intermidiate_result_ = new intermidiate_result();
        intermidiate_result_->content.push_back(filter_results);
        intermidiate_results_->results.push_back(intermidiate_result_);
    }
    // if the relation exists in intermidiate results
    else {
        intermidiate_content_results = intermidiate_results_->results[filter_index[0]]->content[filter_index[1]];
        filter_results = new intermidiate_content(relations[predicate[RELATION_A]],predicate[RELATION_A],intermidiate_content_results->row_ids.size());
        if( operator_ == EQUAL ) {
            for( uint i = 0 ; i < intermidiate_content_results->row_ids.size() ; i++ )
                if( column_b == -1 && file->array[column_a*file->number_of_rows+intermidiate_content_results->row_ids[i]] == filter_number )
                    filter_results->row_ids.push_back(intermidiate_content_results->row_ids[i]);
                else if( column_b != -1 && file->array[column_a*file->number_of_rows+intermidiate_content_results->row_ids[i]] == file->array[column_b*file->number_of_rows+intermidiate_content_results->row_ids[i]])
                    filter_results->row_ids.push_back(intermidiate_content_results->row_ids[i]);
        }
        else if( operator_ == GREATER ) {
            for( uint i = 0 ; i < intermidiate_content_results->row_ids.size() ; i++ )
                if( column_b == -1 && file->array[column_a*file->number_of_rows+intermidiate_content_results->row_ids[i]] > filter_number )
                    filter_results->row_ids.push_back(intermidiate_content_results->row_ids[i]);
                else if( column_b != -1 && file->array[column_a*file->number_of_rows+intermidiate_content_results->row_ids[i]] > file->array[column_b*file->number_of_rows+intermidiate_content_results->row_ids[i]])
                    filter_results->row_ids.push_back(intermidiate_content_results->row_ids[i]);
        }
        else {	// LESS
            for( uint i = 0 ; i < intermidiate_content_results->row_ids.size() ; i++ )
                if( column_b == -1 && file->array[column_a*file->number_of_rows+intermidiate_content_results->row_ids[i]] < filter_number )
                    filter_results->row_ids.push_back(intermidiate_content_results->row_ids[i]);
                else if( column_b != -1 && file->array[column_a*file->number_of_rows+intermidiate_content_results->row_ids[i]] < file->array[column_b*file->number_of_rows+intermidiate_content_results->row_ids[i]])
                    filter_results->row_ids.push_back(intermidiate_content_results->row_ids[i]);
        }

        intermidiate_results_->results[filter_index[0]]->content.erase(intermidiate_results_->results[filter_index[0]]->content.begin()+filter_index[1]);
        delete intermidiate_content_results;
        intermidiate_results_->results[filter_index[0]]->content.push_back(filter_results);
    }

    if( filter_index != NULL )
        free(filter_index);

    if( filter_results->row_ids.size() == 0 )
        return false;
    else
        return true;
}

void projection_sum_results(file_array *file_array, intermidiate_results *intermidiate_results_, sql_query *sql_query_, int64_t **results, int result_index) {
    for( uint i = 0 ; i < sql_query_->projections.size() ; i++ ) {
        int64_t sum = 0;
        int64_t predicate_relation = sql_query_->projections[i][0];
        int64_t file_index = sql_query_->relations[predicate_relation];
        int64_t column = sql_query_->projections[i][1];
        int *intermidiate_result_index = search_intermidiate_results(intermidiate_results_,predicate_relation);
        if( intermidiate_result_index == NULL ) {
            sql_query_->sql_query_print();
            std::cout << "Relation " << file_index << " does not exists in intermidiate results" << std::endl;
            continue;
        }
        
        struct file *file = file_array->files[file_index];
        intermidiate_content *intermidiate_content_ = intermidiate_results_->results[intermidiate_result_index[0]]->content[intermidiate_result_index[1]];
        for( uint j = 0 ; j < intermidiate_content_->row_ids.size() ; j++ )
            sum = sum + file->array[column*file->number_of_rows+intermidiate_content_->row_ids[j]];
        results[result_index][i] = sum;
        if( intermidiate_result_index != NULL )
            free(intermidiate_result_index);
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
            return;
        }
    //    intermidiate_results_->print_intermidiate_results();
    }

    // execute joins
    for( uint i = 0 ; i < execute_query_arguments->sql_query_->joins.size() ; i++ ) {
        return_value = join(execute_query_arguments->file_array_,intermidiate_results_,execute_query_arguments->sql_query_->relations,execute_query_arguments->sql_query_->joins[i]);
        if( return_value == false ) {
            inform_results_with_null(execute_query_arguments->sql_query_->projections.size(),execute_query_arguments->results, execute_query_arguments->result_index);
            delete intermidiate_results_;
            delete execute_query_arguments->sql_query_;
            return;
        }
    //    intermidiate_results_->print_intermidiate_results();
    }

    projection_sum_results(execute_query_arguments->file_array_, intermidiate_results_, execute_query_arguments->sql_query_, execute_query_arguments->results, execute_query_arguments->result_index);

    delete intermidiate_results_;
    delete execute_query_arguments->sql_query_;
//    free(execute_query_arguments);
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