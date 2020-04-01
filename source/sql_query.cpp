#include "../header/sql_query.h"

sql_query::sql_query(char *query) {
    char *relations = NULL, *predicates = NULL, *projections = NULL;

    relations = strtok_r(query, "|", &query);
    error_handler(relations == NULL, "strtok_r failed");
    predicates = strtok_r(query, "|", &query);
    error_handler(predicates == NULL, "strtok_r failed");
    projections = strtok_r(query, "|", &query);
    error_handler(projections == NULL, "strtok_r failed");

    this->parse_relation_query(relations);
    this->parse_predicate_query(predicates);
    this->parse_projection_query(projections);
}

sql_query::~sql_query() {}

void sql_query::parse_relation_query(char *relation_string) {
    char *token = NULL;
    while ( (token = strtok_r(relation_string, " ", &relation_string)) != NULL )
        this->relations.push_back(atoi(token));
}

// operators: 1) '=' -> 0, 2) '>' -> 1 , 3) '<' -> 2
// if the predicate is 0.0=1.1, we will have    |0,0,0,0,0| |0,0,0,0,0|     |0,0,0,0,0|
// if next predicate is 0.1>30 we will have                 |0,1,1,30,-1|   |0,1,1,30,-1|
// if next predicate is 1.2<40 we will have                                 |1,2,2,40,-1|
void sql_query::parse_predicate_query(char *predicate_string) {
    char *token = NULL;
    int row_1, column_1, row_2, column_2, i, operator_int = 0;
    char operator_string[]=" ";
    while( (token = strtok_r(predicate_string, "&", &predicate_string)) != NULL ) {
        for( i = 0 ; token[i] != '\0' ; i++ ) {
            if( token[i] == '=' ) {
                operator_string[0] = token[i];
                operator_int = EQUAL;
                break;
            }
            else if( token[i] == '>' ) {
                operator_string[0] = token[i];
                operator_int = GREATER;
                break;
            }
            else if( token[i] == '<' ) {
                operator_string[0] = token[i];
                operator_int = LESS;
                break;
            }
        }
        row_1 = atoi(strtok_r(token, ".", &token));
        column_1 =  atoi(strtok_r(token, operator_string, &token));
        row_2 = atoi(strtok_r(token, ".", &token));
        token = strtok_r(token, "", &token);

        // if token is NULL it means that we have a filter, so the column_2 will be -1
        if( token == NULL )
            column_2 = -1;
        else
            column_2 = atoi(token);

        std::vector<int> predicate;
        predicate.reserve(5);
        predicate.push_back(row_1);
        predicate.push_back(column_1);
        predicate.push_back(operator_int);
        predicate.push_back(row_2);
        predicate.push_back(column_2);
        if( (column_2 == -1) || (row_1 == row_2) )
            this->filters.push_back(predicate);
        else
            this->joins.push_back(predicate);
    }
}

// if the projection is 0.0, we will have   [0,0]   |0 0|
// if next projection is 1.2 we will have           |1 2|
void sql_query::parse_projection_query(char * projection_string) {
    char *token = NULL;
    int row, column;
    while ( (token = strtok_r(projection_string, " ", &projection_string)) != NULL ) {
        row = atoi(strtok_r(token, ".", &token));
        column = atoi(strtok_r(token, ".", &token));
        
        std::vector<int> projection;
        projection.reserve(2);
        projection.push_back(row);
        projection.push_back(column);
        this->projections.push_back(projection);
    }
}

void sql_query::sql_query_print() {
    std::cout << "Relations:" << std::endl;
    for( auto relation : this->relations )
        std::cout << relation << " ";
    std::cout << std::endl << "Filters:" << std::endl;
    for( auto filter : this->filters ) {
        std::cout << filter[0] << " " << filter[1] << " " << filter[2];
        std::cout << " " << filter[3] << " " << filter[4] << std::endl;
    }
    std::cout << "Joins:" << std::endl;
    for( auto join : this->joins ) {
        std::cout << join[0] << " " << join[1] << " " << join[2];
        std::cout << " " << join[3] << " " << join[4] << std::endl;
    }
}

bool compare_number_of_predicates(std::pair<std::string,int> a, std::pair<std::string,int> b) {
    return a.second > b.second;
}

void increase_number_of_predicates(std::map<std::string, int>& map, int relation, int column) {
    std::map<std::string, int>::const_iterator get_predicate;
    std::string buffer = std::to_string(relation) + "." + std::to_string(column);
    get_predicate = map.find(buffer);
    if( get_predicate == map.end() )
        map[buffer] = 1;
    else
        map[buffer] = get_predicate->second + 1;
}

void sql_query::sort_by_frequency() {
    std::map<std::string, int> map;

    // push filters frequency
    for( auto filter : this->filters )
        increase_number_of_predicates(map,filter[0],filter[1]);

    // push joins frequency
    for( auto join : this->joins ) {
        increase_number_of_predicates(map,join[0],join[1]);
        increase_number_of_predicates(map,join[3],join[4]);
    }

    // sort map by frequency
    std::vector<std::pair<std::string, int> > sorted_by_freq(map.begin(), map.end());
    std::sort(sorted_by_freq.begin(), sorted_by_freq.end(), compare_number_of_predicates);

    // remove filters frequency
    for( auto filter : this->filters ) {
        std::string string = std::to_string(filter[0]) + "." + std::to_string(filter[1]);
        auto it = std::find_if( sorted_by_freq.begin(), sorted_by_freq.end(),[&string](const std::pair<std::string, int>& element){ return element.first == string;} );
        it->second = it->second-1;
    }

    uint swap_index = 0, i;
    // optimize query by swapping the predicates
    for( auto it = sorted_by_freq.begin(); it != sorted_by_freq.end(); ++it ) {
        int frequency = 0;
        while( frequency < it->second ) {
            // find where the predicate is, also decrease other predicate frequency
            for( i = swap_index; i < this->joins.size() ; i++ ) {
                if( ((it->first[0]-'0') == this->joins[i][0]) && ((it->first[2]-'0') == this->joins[i][1]) ) {
                    std::string string = std::to_string(this->joins[i][3]) + "." + std::to_string(this->joins[i][4]);
                    auto it = std::find_if( sorted_by_freq.begin(), sorted_by_freq.end(),[&string](const std::pair<std::string, int>& element){ return element.first == string;} );
                    it->second = it->second-1;
                    break;
                }
                else if(  ((it->first[0]-'0') == this->joins[i][3]) && ((it->first[2]-'0') == this->joins[i][4]) ) {
                    std::string string = std::to_string(this->joins[i][0]) + "." + std::to_string(this->joins[i][1]);
                    auto it = std::find_if( sorted_by_freq.begin(), sorted_by_freq.end(),[&string](const std::pair<std::string, int>& element){ return element.first == string;} );
                    it->second = it->second-1;
                    break;
                }
            }
            // swap predicate
            if( swap_index != i )
                swap(this->joins[i],this->joins[swap_index]);
            swap_index++;
            frequency++;
        }
    }
}