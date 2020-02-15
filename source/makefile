CC = g++
CFLAGS = -g -Wall
RMFLAGS = -rf
EXEC = project
OBJS = relation.o results.o sql_query.o

all: project usage

project: $(OBJS) project.cpp
	$(CC) $(CFLAGS) $(OBJS) project.cpp -o $(EXEC)
	rm $(RMFLAGS) $(OBJS)

relation.o: relation.c
	$(CC) $(CFLAGS) -c relation.c

results.o: results.c
	$(CC) $(CFLAGS) -c results.c

sql_query.o: sql_query.c
	$(CC) $(CFLAGS) -c sql_query.c

usage:
	@echo 'Usage of project: valgrind ./project'

clean:
	rm $(RMFLAGS) $(EXEC) $(OBJS)