#ifndef header_h_
#define header_h_

#include <iostream>
#include <stdint.h>
#include <queue>
#include <cstring>

#define SUCCESS 0
#define FAILURE -1

#define error_handler(expression, message)                                  \
    do {                                                                    \
        if( (expression) == true ) {                                        \
            fprintf(stderr, "%s:%u: %s: '" #expression "' %s.\n",           \
                    __FILE__, __LINE__, __func__,message);                  \
            exit(FAILURE);                                                  \
        }                                                                   \
    } while (0)

#endif