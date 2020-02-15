#include "../header/header.h"
#include "../header/relation.h"
#include "../header/results.h"
#include "../header/sql_query.h"

int main(int argc, char **argv) {
    char query[] = "3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1";
    sql_query sql_query(query);
    
    sql_query.sql_query_print();

    return SUCCESS;
}