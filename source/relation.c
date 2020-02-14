#include "../header/relation.h"

relation::relation(int number_of_tuples) {
    this->num_tuples = number_of_tuples;
    this->tuples = (struct tuple *)malloc(number_of_tuples*sizeof(struct tuple));
    error_handler(this->tuples == NULL, "malloc failed");
}

relation::~relation() {
    free(this->tuples);
}

void relation::relation_initialize() {
    for( int i = 0 ; i < this->num_tuples ; i++ ) {
        this->tuples[i].row_id = i;
        this->tuples[i].value = rand() % 10 + 1;
    }
}

void relation::relation_print() {
    std::cout << "Relation:" << std::endl << "RowId - Value" << std::endl;
    for( int i = 0 ; i < this->num_tuples ; i++ )
        std::cout << this->tuples[i].row_id << " " << this->tuples[i].value << std::endl;
}