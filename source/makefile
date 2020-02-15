CC = g++
CFLAGS = -g -Wall
RMFLAGS = -rf
EXEC = project
OBJS = relation.o results.o sql_query.o file_array.o

all: project usage

project: $(OBJS) project.cpp
	$(CC) $(CFLAGS) $(OBJS) project.cpp -o $(EXEC)
	rm $(RMFLAGS) $(OBJS)

relation.o: relation.cpp
	$(CC) $(CFLAGS) -c relation.cpp

results.o: results.cpp
	$(CC) $(CFLAGS) -c results.cpp

sql_query.o: sql_query.cpp
	$(CC) $(CFLAGS) -c sql_query.cpp

file_array.o: file_array.cpp
	$(CC) $(CFLAGS) -c file_array.cpp

usage:
	@echo 'Usage of project: valgrind ./project'

clean:
	rm $(RMFLAGS) $(EXEC) $(OBJS)