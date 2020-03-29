#include "../header/results.h"

results::results() {
    this->head = NULL;
    this->tail = NULL;
    this->number_of_buckets = 0;
    this->total_size = 0;
}

results::~results() {
    while(this->head != NULL) {
        this->tail = this->head->next_bucket;
        free_pointer(&this->head->tuples);
        free_pointer(&this->head);
        this->head = this->tail;
    }
}

void results::insert_tuple(uint64_t row_key_1, uint64_t row_key_2) {
    // If the list is empty, insert a new bucket and push the tuple at the first position. Also fix head and tail
    if( this->number_of_buckets == 0 ) {
        // fix list data
        this->head = results_initialize_bucket();
        this->tail = this->head;
        this->number_of_buckets++;
        // fix bucket data
        this->head->tuples[0].row_key_1 = row_key_1;
        this->head->tuples[0].row_key_2 = row_key_2;
        this->head->current_size++;
    }
    // If the bucket is full then insert a new bucket and push the tuple at the first position. Fix only tail
    else if( this->tail->current_size == this->tail->max_size ) {
        // fix list data
        this->tail->next_bucket = results_initialize_bucket();
        this->number_of_buckets++;
        this->tail = this->tail->next_bucket;
        // fix bucket data
        this->tail->tuples[0].row_key_1 = row_key_1;
        this->tail->tuples[0].row_key_2 = row_key_2;
        this->tail->current_size++;
    }
    // If the bucket is not full just insert the tuple at current position
    else {
        // fix only bucket data
        this->tail->tuples[this->tail->current_size].row_key_1 = row_key_1;
        this->tail->tuples[this->tail->current_size].row_key_2 = row_key_2;
        this->tail->current_size++;
    }
    this->total_size++;
}

void results::results_print() {
    struct bucket * temp_bucket = this->head;
    std::cout << "Total size: "<< this->total_size << std::endl;
    for( int i = 0 ; i< this->number_of_buckets ; i++) {
        std::cout << "Bucket[" << i << "]: current_size: " << temp_bucket->current_size << ", max_size: " << temp_bucket->max_size << "\nRowId_1 , RowId_2"<< std::endl;
        for(int j = 0 ; j < temp_bucket->current_size ; j ++ )
            std::cout << temp_bucket->tuples[j].row_key_1 << " , " << temp_bucket->tuples[j].row_key_2 << std::endl;
        temp_bucket = temp_bucket->next_bucket;
    }
}

struct bucket *results::results_initialize_bucket() {
    struct bucket *new_bucket = my_malloc(struct bucket,1);
    error_handler(new_bucket == NULL, "malloc failed");
    new_bucket->tuples = my_malloc(struct row_key_tuple,NUMBER_OF_TUPLES_IN_BUCKET);
    error_handler(new_bucket->tuples == NULL, "malloc failed");

    new_bucket->max_size = NUMBER_OF_TUPLES_IN_BUCKET;
    new_bucket->current_size = 0;
    new_bucket->next_bucket = NULL;

    return new_bucket;
}