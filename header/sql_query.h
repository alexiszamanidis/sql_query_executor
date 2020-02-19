#ifndef sql_query_h_
#define sql_query_h_

#include "./header.h"

enum {
    RELATION_A, COLUMN_A, OPERATOR, RELATION_B, COLUMN_B
};

enum {
    EQUAL, GREATER, LESS
};

class sql_query {
    public:
        std::vector<int> relations;
        std::vector<std::vector<int>> filters;
        std::vector<std::vector<int>> joins;
        std::vector<std::vector<int>> projections;
    public:
        sql_query(char *);
        ~sql_query();
        void parse_relation_query(char *);
        void parse_predicate_query(char *);
        void parse_projection_query(char * );
        void sql_query_print();
};

#endif