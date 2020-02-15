#include "../header/header.h"
#include "../header/relation.h"
#include "../header/results.h"

int main(int argc, char **argv) {
    relation relation, relation_2;
    results results;

    relation.relation_initialize_with_dataset((char*)"../dataset/small/relA");
    relation.sort_iterative();

    relation_2.relation_initialize_with_dataset((char*)"../dataset/small/relB");
    relation_2.sort_iterative();

    parallel_join(&relation,&relation_2,&results);

    results.results_print();

    return SUCCESS;
}