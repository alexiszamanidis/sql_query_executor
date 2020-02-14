#include "../header/relation.h"

relation::relation() {
    this->num_tuples = 0;
    this->tuples = NULL;
}

relation::~relation() {
    free(this->tuples);
}

void relation::relation_initialize_random(int number_of_tuples) {
    this->num_tuples = number_of_tuples;
    this->tuples = (struct tuple *)malloc(number_of_tuples*sizeof(struct tuple));
    error_handler(this->tuples == NULL, "malloc failed");
    for( int i = 0 ; i < this->num_tuples ; i++ ) {
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

void relation::relation_print() {
    std::cout << "Relation:" << std::endl << "RowId - Value" << std::endl;
    for( int i = 0 ; i < this->num_tuples ; i++ )
        std::cout << this->tuples[i].row_id << " " << this->tuples[i].value << std::endl;
}