#include "../header/relation.h"

relation::relation() {
    this->num_tuples = 0;
    this->tuples = NULL;
    this->join_partition = NULL;
}

relation::relation(int number_of_tuples) {
    this->num_tuples = number_of_tuples;
    this->tuples = my_malloc(struct tuple,number_of_tuples);
    error_handler(this->tuples == NULL, "malloc failed");
    this->join_partition = NULL;
}

relation::~relation() {
    free_pointer(&this->tuples);
    this->free_join_partition();
}

void relation::free_join_partition() {
    if( (join_partition) != NULL ) {
        free_pointer(&join_partition->histogram);
        free_pointer(&join_partition->prefix_sum);
        free_pointer(&join_partition);
    }
}

void relation::relation_initialize_random(int number_of_tuples) {
    this->num_tuples = number_of_tuples;
    this->tuples = my_malloc(struct tuple,number_of_tuples);
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
    this->tuples = my_malloc(struct tuple,number_of_tuples);
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

    free_pointer(&line_buffer);
    fclose(dataset);
}

void relation::create_relation_from_file(struct file *file, int column){
    this->num_tuples = file->number_of_rows;
    this->tuples = my_malloc(struct tuple,file->number_of_rows);
    error_handler(this->tuples == NULL, "malloc failed");
    for( uint64_t i = 0 ; i < this->num_tuples ; i++ ) {
        this->tuples[i].row_id = i;
        this->tuples[i].value = file->array[i][column];
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
    uint64_t *temp= my_calloc(uint64_t,BUCKET_SIZE);
    error_handler(temp == NULL, "calloc failed");

    for( uint64_t i = start ; i <end ; i++ ) {
        hash_index = binary_mask(this->tuples[i].value, byte);
        temp[hash_index]++;
        if( temp[hash_index] == 1 ) {
            histogram_indexes->indexes[histogram_indexes->size] = hash_index;
            histogram_indexes->size++;
        }
    }
    std::sort(histogram_indexes->indexes,histogram_indexes->indexes+histogram_indexes->size);
    return temp;
}

uint64_t *relation::create_prefix_sum(uint64_t *histogram, uint64_t start) {
    int sum = start;
    uint64_t *temp = my_calloc(uint64_t,BUCKET_SIZE);
    error_handler(temp == NULL, "calloc failed");

    for( int i = 0 ; i < BUCKET_SIZE ; i++ ) {
        if( histogram[i] != 0 ) {
            temp[i] = sum;
            sum = sum + histogram[i];
        }
    }

    return temp;
}

int relation::partition(int start, int end) {
    //  pivot                          , index of smaller element
    uint64_t pivot = this->tuples[end].value;
    int64_t i = start-1;
    
    for( int j = start ; j < end ; j++ ) {
        // if current element is smaller than the pivot, increase index of smaller element and swap the tuples 
        if( this->tuples[j].value < pivot ) {
            i++;
            swap(this->tuples[i],this->tuples[j]);
        }
    }
    i++;
    // put pivot at correct position
    if( this->tuples[i].value != this->tuples[end].value )
        swap(this->tuples[i],this->tuples[end]);
    return i;
}

inline void relation::quick_sort(int start, int end) {
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

inline int compare_tuples(const void *tuple_1, const void *tuple_2) {
    return ((struct tuple *)tuple_1)->value - ((struct tuple *)tuple_2)->value;
}

inline void quick_sort_job(void *arguments) {
    struct quick_sort_arguments *quick_sort_arguments = (struct quick_sort_arguments *)arguments;
    quick_sort_arguments->R->quick_sort(quick_sort_arguments->start,quick_sort_arguments->end);
}

uint64_t * sum_histograms(int64_t **thread_histograms, int rows, int columns, struct histogram_indexing *histogram_indexes) {
    uint64_t *temp = my_calloc(uint64_t,BUCKET_SIZE);
    error_handler(temp == NULL, "calloc failed");
    for( int i = 0 ; i < rows ; i++ ) {
        for( int j = 0 ; j < columns ; j++ ) {
            if( thread_histograms[i][j] == 0 )
                continue;
            if( temp[j] == 0 ) {
                histogram_indexes->indexes[histogram_indexes->size] = j;
                histogram_indexes->size++;
            }
            temp[j] = temp[j] + thread_histograms[i][j];
        }
    }
    std::sort(histogram_indexes->indexes,histogram_indexes->indexes+histogram_indexes->size);
    return temp;
}

inline void create_histogram_for_multithread(void *arguments) {
    struct histogram_arguments *histogram_arguments = (struct histogram_arguments *)arguments;
    int hash_index;
    for( uint64_t i = histogram_arguments->start ; i < histogram_arguments->end ; i++ ) {
        hash_index = histogram_arguments->R->binary_mask(histogram_arguments->R->tuples[i].value, histogram_arguments->byte);
        histogram_arguments->histogram[hash_index]++;
    }
}

inline void break_histogram_to_jobs(relation *R, uint64_t start, uint64_t end, int byte,struct histogram_indexing *histogram_indexes, int64_t **thread_histograms, int *job_barrier) {
    extern struct job_scheduler *job_scheduler;
    int step = (end-start)/BREAK_HISTOGRAMS;
    for( int i = 0 ; i < BREAK_HISTOGRAMS ; i++) {
        struct histogram_arguments *histogram_arguments = my_malloc(struct histogram_arguments,1);
        error_handler(histogram_arguments == NULL,"malloc failed");
        histogram_arguments->R = R;
        histogram_arguments->byte = byte;
        histogram_arguments->start = start;
        histogram_arguments->histogram = thread_histograms[i];
        if( i == BREAK_HISTOGRAMS-1 )
            histogram_arguments->end = end;
        else
            histogram_arguments->end = start + step;
        schedule_job_scheduler(job_scheduler,create_histogram_for_multithread,histogram_arguments,job_barrier);
        start = start + step;
    }
}

inline uint64_t *create_histogram_multithread(relation *R, uint64_t start,uint64_t end, int byte, struct histogram_indexing *histogram_indexes) {
    extern struct job_scheduler *job_scheduler;
    int job_barrier = 0;
    int64_t **thread_histograms = allocate_and_initialize_2d_array(BREAK_HISTOGRAMS,BUCKET_SIZE,0);
    break_histogram_to_jobs(R,start,end,byte,histogram_indexes,thread_histograms,&job_barrier);
    dynamic_barrier_job_scheduler(job_scheduler,&job_barrier);
    uint64_t *histogram = sum_histograms(thread_histograms,BREAK_HISTOGRAMS,BUCKET_SIZE,histogram_indexes);
    free_2d_array(&thread_histograms);
    if( histogram_indexes->size == 0 ) {
        free_pointer(&histogram);
        return NULL;
    }
    return histogram;
}

void sort_iterative(void *argument) {
    extern struct job_scheduler *job_scheduler;
    struct sort_iterative_arguments *sort_iterative_arguments = (struct sort_iterative_arguments *)argument;
    relation *R = sort_iterative_arguments->R;
    relation R_new(R->num_tuples);
    uint64_t start = 0, end = R->num_tuples, *histogram, *prefix_sum;;
    int byte = START_BYTE, job_barrier = 0;
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
        if( sort_node.byte%2 == MODULO ) {
            // fix histogram for reader R
            //histogram = R->create_histogram(sort_node.start,sort_node.end,sort_node.byte,&histogram_indexes);
            histogram = create_histogram_multithread(R,sort_node.start,sort_node.end,sort_node.byte,&histogram_indexes);
            // fix prefix sum
            prefix_sum = R->create_prefix_sum(histogram,sort_node.start);
            // hold first histogram and prefix_sum
            if( sort_node.byte == START_BYTE ) {
                struct join_partition *join_partition = my_malloc(struct join_partition,1);
                error_handler(join_partition == NULL,"malloc failed");
                join_partition->histogram = histogram;
                join_partition->histogram_indexes = histogram_indexes;
                join_partition->prefix_sum = prefix_sum;
                R->join_partition = join_partition;
            }
            // R writes to R_new
            R->fill_new_relation(&R_new, prefix_sum, sort_node.start, sort_node.end, sort_node.byte);
        }
        else {
            // fix histogram for reader R_new
            //histogram = R_new.create_histogram(sort_node.start,sort_node.end,sort_node.byte,&histogram_indexes);
            histogram = create_histogram_multithread(&R_new,sort_node.start,sort_node.end,sort_node.byte,&histogram_indexes);
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
                // if the byte % 2 == 1 memcpy R_new to R
                if( sort_node.byte%2 == MODULO )
                    memcpy(R->tuples+start,R_new.tuples+start,(end-start)*sizeof(struct tuple));
                // then do quick sort on R
                //qsort(R->tuples+start,end-start,sizeof(struct tuple),compare_tuples);
                //R->quick_sort(start,end-1);
                struct quick_sort_arguments *quick_sort_arguments = my_malloc(struct quick_sort_arguments,1);
                error_handler(quick_sort_arguments == NULL,"malloc failed");
                *quick_sort_arguments = { .R = R, .start = start, .end = end-1};
                schedule_job_scheduler(job_scheduler,quick_sort_job,quick_sort_arguments,&job_barrier);
            }
            // if the hash duplicates are more than cache size then push {start,end,byte+1} to the list
            else {
                // if the next byte is not the 9th, we just push { start , end , byte + 1 } to sort data list
                if( sort_node.byte + 1 != 9 )
                    sort_data_list.push((struct sort_node){.start = start, .end = end, .byte = sort_node.byte+1});
            }
        }
        // do not free first histogram and prefix sum
        if( sort_node.byte != START_BYTE) {
            free_pointer(&histogram);
            free_pointer(&prefix_sum);
        }
    }
    // wait all sorts to end
    dynamic_barrier_job_scheduler(job_scheduler,&job_barrier);
}

void get_range_multithread(relation *R, int64_t start, int64_t *end, int64_t finish) {
    // calculate the range
    while( *end != finish )
        if( (*end+1 != finish)&&(R->tuples[start].value == R->tuples[*end+1].value) )
            (*end)++;
        else
            return;
}

void parallel_join_multithread(void *argumnets) {
    struct parallel_join_arguments *parallel_join_arguments = (struct parallel_join_arguments *)argumnets;
    int64_t index_R_start = parallel_join_arguments->start_R, index_S_start = parallel_join_arguments->start_S;
    int64_t index_R_end = parallel_join_arguments->start_R, index_S_end = parallel_join_arguments->start_S, i, j;
    int64_t finish_R = parallel_join_arguments->end_R, finish_S = parallel_join_arguments->end_S;
    relation * R = parallel_join_arguments->R;
    relation * S = parallel_join_arguments->S;
    results * results = parallel_join_arguments->list;

    while( ( index_R_start != finish_R ) && ( index_S_start != finish_S ) ){
        // calculate R's range
        get_range_multithread(R,index_R_start,&index_R_end,finish_R);
        // calculate S's range
        get_range_multithread(S,index_S_start,&index_S_end,finish_S);
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
        };
    }
}

void fix_thread_list_results_links(results **thread_list_results, results *list_results, int number_of_rows) {
    for( int i = 0 ; i < number_of_rows ; i ++ ) {
        if( list_results->head == NULL ) {
            list_results->head = thread_list_results[i]->head;
            list_results->tail = thread_list_results[i]->tail;
        }
        else {
            list_results->tail->next_bucket = thread_list_results[i]->head;
            list_results->tail = thread_list_results[i]->tail;
        }
        list_results->total_size = list_results->total_size + thread_list_results[i]->total_size;
        list_results->number_of_buckets = list_results->number_of_buckets + thread_list_results[i]->number_of_buckets;
        thread_list_results[i]->head = NULL;
    }
}

void break_join_to_jobs(relation **R, relation **S, results *list_results, struct join_partition *join_partition_R, struct join_partition *join_partition_S) {
    extern struct job_scheduler *job_scheduler;
    results **thread_list_results = NULL;
    int number_of_rows = 0, index_R = 0, index_S = 0, index_list_results = 0,start_R, end_R, start_S, end_S, join_barrier = 0;

    if( join_partition_R->histogram_indexes.size <= join_partition_S->histogram_indexes.size )
        number_of_rows = join_partition_R->histogram_indexes.size;
    else
        number_of_rows = join_partition_S->histogram_indexes.size;

    thread_list_results = initialize_2d_results(number_of_rows);

    // break join to jobs
    while( ( index_R < join_partition_R->histogram_indexes.size ) && ( index_S < join_partition_S->histogram_indexes.size ) ) {
        if( join_partition_R->histogram_indexes.indexes[index_R] == join_partition_S->histogram_indexes.indexes[index_S] ) {
            start_R = join_partition_R->prefix_sum[join_partition_R->histogram_indexes.indexes[index_R]];
            end_R = join_partition_R->prefix_sum[join_partition_R->histogram_indexes.indexes[index_R]]+join_partition_R->histogram[join_partition_R->histogram_indexes.indexes[index_R]];
            start_S = join_partition_S->prefix_sum[join_partition_S->histogram_indexes.indexes[index_S]];
            end_S = join_partition_S->prefix_sum[join_partition_S->histogram_indexes.indexes[index_S]]+join_partition_S->histogram[join_partition_S->histogram_indexes.indexes[index_S]];

            struct parallel_join_arguments *parallel_join_arguments = my_malloc(struct parallel_join_arguments,1);
            error_handler(parallel_join_arguments == NULL,"malloc failed");
            parallel_join_arguments->R = *R; parallel_join_arguments->S = *S; parallel_join_arguments->list = thread_list_results[index_list_results];
            parallel_join_arguments->start_R = start_R; parallel_join_arguments->end_R = end_R; parallel_join_arguments->start_S = start_S; parallel_join_arguments->end_S = end_S;
            schedule_job_scheduler(job_scheduler,parallel_join_multithread,parallel_join_arguments,&join_barrier);
            
            index_R++;
            index_list_results++;
        }
        else if( join_partition_R->histogram_indexes.indexes[index_R] < join_partition_S->histogram_indexes.indexes[index_S] )
            index_R++;
        else
            index_S++;
    }
    // wait all joins to end
    dynamic_barrier_job_scheduler(job_scheduler,&join_barrier);

    fix_thread_list_results_links(thread_list_results,list_results,number_of_rows);

    free_2d_results(thread_list_results,number_of_rows);
}