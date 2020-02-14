#include "../header/header.h"
#include "../header/relation.h"
#include "../header/results.h"

int main(int argc, char **argv) {
    relation relation(3);
    results results;

    relation.relation_initialize();
    relation.relation_print();

    results.insert_tuple(1,2);
    results.results_print();
    results.insert_tuple(1,2);
    results.results_print();
    results.insert_tuple(1,2);
    results.results_print();
    results.insert_tuple(1,2);
    results.results_print();

    return SUCCESS;
}