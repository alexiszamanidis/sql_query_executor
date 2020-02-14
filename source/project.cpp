#include "../header/header.h"
#include "../header/relation.h"
#include "../header/results.h"

int main(int argc, char **argv) {
    relation relation;
    results results;

    relation.relation_initialize_with_dataset((char*)"../dataset/tiny/relA");
    relation.relation_print();

//    results.insert_tuple(1,2);
//    results.results_print();

    return SUCCESS;
}