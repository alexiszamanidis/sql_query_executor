#ifndef header_h_
#define header_h_

#include <iostream>
#include <stdint.h>
#include <queue>
#include <cstring>
#include <vector>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <pthread.h>
#include <cstdlib>
#include <stdio.h>
#include <map>
#include <algorithm>

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

#define my_malloc(type, number_of_elements)                                 \
    ( (type*) malloc( (number_of_elements) * sizeof(type)) )

#define free_pointer(pointer)                                               \
    do {                                                                    \
        if( *pointer != NULL ) {                                            \
            free(*pointer);                                                 \
            *pointer = NULL;                                                \
        }                                                                   \
    } while (0)

#define swap(x,y)                                                                   \
    do {                                                                            \
        unsigned char swap_temp[sizeof(x) == sizeof(y) ? (signed)sizeof(x) : -1];   \
        memcpy(swap_temp,&y,sizeof(x));                                             \
        memcpy(&y,&x,       sizeof(x));                                             \
        memcpy(&x,swap_temp,sizeof(x));                                             \
    } while(0)

#endif