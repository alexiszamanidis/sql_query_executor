CC = g++
CFLAGS = -g -Wall
RMFLAGS = -rf
EXEC = project
OBJS = relation.o results.o

all: project usage

project: $(OBJS) project.cpp
	$(CC) $(OBJS) $(CFLAGS) project.cpp -o $(EXEC)
	rm $(RMFLAGS) $(OBJS)

relation.o: relation.c
	$(CC) $(CFLAGS) -c relation.c

results.o: results.c
	$(CC) $(CFLAGS) -c results.c

usage:
	@echo 'Usage of project: valgrind ./project'

clean:
	rm $(RMFLAGS) $(EXEC) $(OBJS)