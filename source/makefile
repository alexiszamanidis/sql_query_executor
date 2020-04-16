CC = g++
CFLAGS = -g -Wall -O3 -pthread -std=c++11
RMFLAGS = -rf
EXEC = join
OBJS = utilities.o relation.o results.o sql_query.o file_array.o queue.o job_scheduler.o  intermidiate_results.o

all: join usage

join: $(OBJS) join.cpp
	$(CC) $(CFLAGS) $(OBJS) join.cpp -o $(EXEC)
	rm $(RMFLAGS) $(OBJS)

utilities.o: utilities.cpp
	$(CC) $(CFLAGS) -c utilities.cpp

relation.o: relation.cpp
	$(CC) $(CFLAGS) -c relation.cpp

results.o: results.cpp
	$(CC) $(CFLAGS) -c results.cpp

sql_query.o: sql_query.cpp
	$(CC) $(CFLAGS) -c sql_query.cpp

file_array.o: file_array.cpp
	$(CC) $(CFLAGS) -c file_array.cpp

queue.o: ../thread_pool/source/queue.c
	$(CC) $(CFLAGS) -c ../thread_pool/source/queue.c

job_scheduler.o: ../thread_pool/source/job_scheduler.c
	$(CC) $(CFLAGS) -c ../thread_pool/source/job_scheduler.c

intermidiate_results.o: intermidiate_results.cpp
	$(CC) $(CFLAGS) -c intermidiate_results.cpp

usage:
	@echo 'Usage of join: valgrind ./join < ../inputfiles/inputfile_small'

clean:
	rm $(RMFLAGS) $(EXEC) $(OBJS)