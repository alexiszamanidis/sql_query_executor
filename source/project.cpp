#include "../header/header.h"
#include "../header/relation.h"

int main(int argc, char **argv) {
    relation relation(3);

    relation.relation_initialize();
    relation.relation_print();

    return SUCCESS;
}