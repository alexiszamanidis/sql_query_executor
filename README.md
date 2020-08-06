## SQL Query Executor

In this Project we implemented a Parallel SQL Query Executor that parses and executes SQL queries. 
It also rearranges the predicates by frequency to reduce execution time.

### Methology

At the beginning you get a number of file paths, each one from which it has the data of each relation. 
The data of each relation should read and stored in memory. For this reason you will need to keep one array with as many 
cells as the relations that initially come to you. Each cell in this array will hold a metadata such as the row number of this 
array and the number of columns. She will also keep a board that will have pointers in her memory location of each of its columns. 
The first cell of the array should hold the information for the first relation the second for the second etc. When we refer in 
relation 0 we will refer to the relation that is stored in cell 0 etc. The representation of this structure is shown in the figure below:

#### File Array

![file_array](https://user-images.githubusercontent.com/48658768/89515187-75aa5f00-d7df-11ea-966a-e738b5ab4e82.png)

#### Query Parsing
After loading all relations in memory, we parse and execute queries per batches.

**Query input format example**

Relations | Predicates | Projections
```c
0 2 4|0.1=1.2&1.0=2.1&0.1>3000|0.0 1.1
```

Translate to SQL:
```sql
SELECT SUM("0".c0), SUM("1".c1)
FROM r0 "0", r2 "1", r4 "2"
WHERE 0.c1=1.c2 and 1.c0=2.c1 and 0.c1>3000
```

#### Sort Merge Join

Sort Merge Join's idea is to sort the relations as to the key of their coupling and then the coupling to be done with simple 
merge as we will have a passage of sorted tables.

At the Sort stage, the relations will be sorted by the following radix-sort method. The data from the relations will be 
analyzed **in buckets the same size as the L1-cache of the processor (note the maximum size 64 KB)**. The new table will result from 
the following procedure: We commit a new one table the size of the original. In this table we will store the data in series of 
each bucket. To do this we need to know where in the new table start the data of each bucket. For this reason we create a table (histogram) 
256 positions where in each position we hold the number of elements that are in this bucket. Then we will create its cumulative 
histogram which will show the position where each bucket will start. Having this histogram we can write the data in its correct 
place with a passage of the original table new table.

**This process will be repeated** for each bucket retrospectively until the bucket size isis less than desired (64KB). When this happens, 
each bucket can be sortedusing an efficient sort method (quicksort).

**In the end** we will have two fully sorted relations in terms of the key to the link (join key). To get the final results, 
we have to scan them in parallel data of the two relations and extract the final results.

#### Results

**Because we do not know in advance the volume of results**, the results of the link will be written to a new structure which is **in the form of a queue**. 
**Each bucket on the queue will hold a 1MB table and a pointer to the next bucket**. The logic is that we write data to a table. Once the table is full 
then we will introduce a new one bucket in the queue and so on. This structure will have the form shown in the figure below: 

![results](https://user-images.githubusercontent.com/48658768/89527981-ee66e680-d7f2-11ea-9c14-12cfbed4de94.png)

### Optimizations

**Reordeing queries**: We execute the filters first, then execute relations that were used in filters or that appear more often.

**Parallel Query Execution**: As we said before we parse and execute queries per batches, so main thread pushes all batch 
queries to the Queue.

**Parallel Histogram Calculation**: We break the Histogram into two pieces and push them to the Queue. We tried different numbers, 
for example to break the histogram each time with the number of threads in the thread pool, but it was not optimal for our application, 
because calculating the histogram is not a big deal.

**Parallel Sorting between 2 relations that are joined**: The two relations are not interdependent. Thus, we can sort 
each one separately. If any of these were executed in exactly the previous predicate, then we do not push it to the Queue 
because it has already been sorted and saved in intermediate results.

**Parallel Join between elements with the same prefix**: We take the prefixes of each relation and push a Join Job 
if the prefixes match.

### Execution Instructions

Αn example of how you can run the program

```c
make
./join 1 < ../inputfiles/inputfile_small
```

Of course you can change the parameter of the threads as well as the input.