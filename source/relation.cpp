#include "../header/relation.h"

relation::relation() {
    this->num_tuples = 0;
    this->tuples = NULL;
}

relation::relation(int number_of_tuples) {
    this->num_tuples = number_of_tuples;
    this->tuples = (struct tuple *)malloc(number_of_tuples*sizeof(struct tuple));
    error_handler(this->tuples == NULL, "malloc failed");
}

relation::~relation() {
    free(this->tuples);
}

void relation::relation_initialize_random(int number_of_tuples) {
    this->num_tuples = number_of_tuples;
    this->tuples = (struct tuple *)malloc(number_of_tuples*sizeof(struct tuple));
    error_handler(this->tuples == NULL, "malloc failed");
    for( uint64_t i = 0 ; i < this->num_tuples ; i++ ) {
        this->tuples[i].row_id = i;
        this->tuples[i].value = rand() % 10 + 1;
    }
}

void relation::relation_initialize_with_dataset(char *filename) {
    FILE *dataset;
    char *line_buffer = NULL, *ptr;
    size_t line_buffer_size=0;
    uint64_t number_of_tuples = 0, i = 0, row_id, value;

    dataset = fopen(filename,"r");
    error_handler(dataset == NULL, "fopen failed");

    // get the number of lines, so we can create the tuples
    while(getline(&line_buffer, &line_buffer_size, dataset)>=0)
        number_of_tuples++;

    this->num_tuples = number_of_tuples;
    this->tuples = (struct tuple *)malloc(number_of_tuples*sizeof(struct tuple));
    error_handler(this->tuples == NULL, "malloc failed");

    // move the file pointer at the begining
    fseek(dataset, 0, SEEK_SET);
    // initialize relation tuples with dataset
    while( getline(&line_buffer, &line_buffer_size, dataset)>=0 ) {
        value = strtoull(line_buffer,&ptr,10);
        row_id = strtoull(ptr+1,&ptr,10);
        this->tuples[i].row_id = row_id;
        this->tuples[i].value = value;
        i++;
    }

    free(line_buffer);
    fclose(dataset);
}

void relation::create_relation_from_file(struct file *file, int column){
    this->num_tuples = file->number_of_rows;
    this->tuples = (struct tuple *)malloc(file->number_of_rows*sizeof(struct tuple));
    error_handler(this->tuples == NULL, "malloc failed");
    for( uint64_t i = 0 ; i < this->num_tuples ; i++ ) {
        this->tuples[i].row_id = i;
        this->tuples[i].value = file->array[column*file->number_of_rows + i];
    }
}

void relation::relation_print() {
    std::cout << "Relation:" << std::endl << "RowId - Value" << std::endl;
    for( uint64_t i = 0 ; i < this->num_tuples ; i++ )
        std::cout << this->tuples[i].row_id << " " << this->tuples[i].value << std::endl;
}

unsigned char relation::binary_mask(uint64_t value, int byte) {
    return (value >> (8*(sizeof(value)-byte))) & 0xff;
}

uint64_t *relation::create_histogram(uint64_t start, uint64_t end, int byte,struct histogram_indexing *histogram_indexes) {
    int hash_index;
    uint64_t *temp= (uint64_t*)calloc(BUCKET_SIZE, sizeof(uint64_t));
    error_handler(temp == NULL, "calloc failed");

    for( uint64_t i = start ; i <end ; i++ ) {
        hash_index = binary_mask(this->tuples[i].value, byte);
        temp[hash_index]++;
        if( temp[hash_index] == 1 ) {
            histogram_indexes->indexes[histogram_indexes->size] = hash_index;
            histogram_indexes->size++;
        }
    }

    return temp;
}

uint64_t *relation::create_prefix_sum(uint64_t *histogram, uint64_t start) {
    int sum = start;
    uint64_t *temp = (uint64_t*)calloc(BUCKET_SIZE, sizeof(uint64_t));
    error_handler(temp == NULL, "calloc failed");

    for( int i = 0 ; i < BUCKET_SIZE ; i++ ) {
        if( histogram[i] != 0 ) {
            temp[i] = sum;
            sum = sum + histogram[i];
        }
    }

    return temp;
}

void relation::swap_tuples(int i, int j) {
    struct tuple temp_tuple;

    temp_tuple.row_id = this->tuples[j].row_id;
    temp_tuple.value = this->tuples[j].value;
    this->tuples[j].row_id = this->tuples[i].row_id;
    this->tuples[j].value = this->tuples[i].value;
    this->tuples[i].row_id = temp_tuple.row_id;
    this->tuples[i].value = temp_tuple.value;
}

int relation::partition(int start, int end) {
    //  pivot                          , index of smaller element
    uint64_t pivot = this->tuples[end].value;
    int64_t i = start-1;
    
    for( int j = start ; j < end ; j++ ) {
        // if current element is smaller than the pivot, increase index of smaller element and swap the tuples 
        if( this->tuples[j].value < pivot ) {
            i++;
            swap_tuples(i,j);
        }
    }
    i++;
    // put pivot at correct position
    if( this->tuples[i].value != this->tuples[end].value )
        swap_tuples(i,end);
    return i;
}

void relation::quick_sort(int start, int end) {
    int partition_index;

    if( start < end ) {
        partition_index = partition(start,end);
        quick_sort(start,partition_index-1);
        quick_sort(partition_index+1,end);
    }
}

void relation::fill_new_relation(relation *R, uint64_t *prefix_sum, uint64_t start, uint64_t end, int byte) {
    int temp_value;
    uint64_t counter[BUCKET_SIZE] = {0};
    for( uint64_t i = start ; i < end ; i++ ) {
        temp_value=binary_mask(this->tuples[i].value,byte);
        R->tuples[((counter[temp_value]++)+prefix_sum[temp_value])] = this->tuples[i];
    }
}

void sort_iterative(void *argument) {
    struct sort_iterative_arguments *sort_iterative_arguments = (struct sort_iterative_arguments *)argument;
    relation *R = sort_iterative_arguments->R;
    relation R_new(R->num_tuples);
    uint64_t start = 0, end = R->num_tuples, *histogram, *prefix_sum;;
    int byte = 1;
    std::queue<struct sort_node> sort_data_list;
    struct sort_node sort_node;
    struct histogram_indexing histogram_indexes;

    // push R
    sort_data_list.push((struct sort_node){.start = start, .end = end, .byte = byte});

    while( sort_data_list.size() > 0 ) {
        // pop a sort data node => {start,end,byte}
        sort_node = sort_data_list.front();
        sort_data_list.pop();

        // fix histogram indexes size to 0
        histogram_indexes.size = 0;

        // fix R_new sorted by current histogram and prefix sum
        if( sort_node.byte%2 == 1 ) {
            // fix histogram for reader R
            histogram = R->create_histogram(sort_node.start,sort_node.end,sort_node.byte,&histogram_indexes);
            // fix prefix sum
            prefix_sum = R->create_prefix_sum(histogram,sort_node.start);
            // R writes to R_new
            R->fill_new_relation(&R_new, prefix_sum, sort_node.start, sort_node.end, sort_node.byte);
        }
        else {
            // fix histogram for reader R_new
            histogram = R_new.create_histogram(sort_node.start,sort_node.end,sort_node.byte,&histogram_indexes);
            // fix prefix sum
            prefix_sum = R->create_prefix_sum(histogram,sort_node.start);
            // R_new writes to R
            R_new.fill_new_relation(R, prefix_sum, sort_node.start, sort_node.end, sort_node.byte);
        }

        for( int i = 0 ; i < histogram_indexes.size ; i++ ) {
            start = prefix_sum[histogram_indexes.indexes[i]];
            end = prefix_sum[histogram_indexes.indexes[i]]+histogram[histogram_indexes.indexes[i]];
            // if the hash duplicates are less or equal to cache size then just do quick sort
            if( histogram[histogram_indexes.indexes[i]] <= DUPLICATES ) {
                // if the byte % 2 == 1 then memcpy the part we just sort from R_new to R
                if( sort_node.byte%2 == 1 ) {
                    R_new.quick_sort(start,end-1);
                    memcpy(R->tuples+start,R_new.tuples+start,(end-start)*sizeof(struct tuple));
                }
                // otherwise just do quick sort on R
                else
                    R->quick_sort(start,end-1);
            }
            // if the hash duplicates are more than cache size then push {start,end,byte+1} to the list
            else {
                // if the next byte is not the 9th, we just push { start , end , byte + 1 } to sort data list
                if( sort_node.byte + 1 != 9 )
                    sort_data_list.push((struct sort_node){.start = start, .end = end, .byte = sort_node.byte+1});
            }
        }
        free(histogram);
        free(prefix_sum);
    }
}

uint64_t relation::get_tuple_value(uint64_t position) {
    return this->tuples[position].value;
}

uint64_t relation::get_tuple_row_id(uint64_t position) {
    return this->tuples[position].row_id;
}

uint64_t relation::get_number_of_tuples() {
    return this->num_tuples;
}

void relation::get_range(uint64_t start, uint64_t *end) {
    // calculate the range
    while( *end != this->num_tuples )
        if( (*end+1 != this->num_tuples)&&(this->tuples[start].value == this->tuples[*end+1].value) )
            (*end)++;
        else
            return;
}

void parallel_join(relation *R, relation *S, results *results) {
    uint64_t index_R_start = 0, index_S_start = 0, index_R_end = 0, index_S_end = 0, i, j;
    while( ( index_R_start != R->num_tuples ) && ( index_S_start != S->num_tuples ) ){
        // calculate ranges
        R->get_range(index_R_start,&index_R_end);
        S->get_range(index_S_start,&index_S_end);

        // if the value is the same then do a double loop and insert all the row id into the list
        if( R->tuples[index_R_start].value == S->tuples[index_S_start].value ) {
            for( i = index_R_start ; i <= index_R_end ; i++ )
                for( j = index_S_start ; j <= index_S_end ; j++ )
                    results->insert_tuple(R->get_tuple_row_id(i), S->get_tuple_row_id(j));
            // also move R pointer
            index_R_start = index_R_end + 1;
            index_R_end = index_R_start;
        }
        // if R's value is less than S's value then move R pointer
        else if( R->tuples[index_R_start].value < S->tuples[index_S_start].value ) {
            index_R_start = index_R_end + 1;
            index_R_end = index_R_start;
        }
        // if S's value is less than R's value then move R pointer
        else {
            index_S_start = index_S_end + 1;
            index_S_end = index_S_start;
        }
    }
} 