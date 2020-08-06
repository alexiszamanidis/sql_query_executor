### Experiments

#### Computer Specifications

* OS: Linux 5.4
* CPU: i7 6700K 4.00Ghz: 4 cores 8 threads
* RAM: 16 GB RAM

The **input workloads** that were used were from the **SIGMOD 2018 competition**. (We have not pushed the medium workload because it 
was too big.)

| Command | Input   | Threads                                  | Seconds   |
|---------|---------|------------------------------------------|-----------|
|./join   | small   | 1 thread  (before parallelization)       | 0.770227  |
|./join 1 | small   | 2 threads (1 + 1 thread in thread_pool)  | 0.655918  |
|./join 3 | small   | 4 threads (1 + 3 threads in thread_pool) | 0.558085  |
|./join 5 | small   | 6 threads (1 + 5 threads in thread_pool) | 0.559655  |
|./join 7 | small   | 8 threads (1 + 7 threads in thread_pool) | 0.558057  |
|./join   | medium  | 1 thread  (before parallelization)       | 41.330519 |
|./join 1 | medium  | 2 threads (1 + 1 thread in thread_pool)  | 27.662735 |
|./join 3 | medium  | 4 threads (1 + 3 threads in thread_pool) | 20.087889 |
|./join 5 | medium  | 6 threads (1 + 5 threads in thread_pool) | 17.650954 |
|./join 7 | medium  | 8 threads (1 + 7 threads in thread_pool) | 18.127873 |

### Observations

**High Memory Usage**: As we increase the number of queries running in parallel we increase the memory needed. 
If our **Compute Specifications** were worse, the performance would probably slow down our schedule.
