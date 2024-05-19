## Results of JMH measurements for arrows

| System parameter      | Value                              |
|-----------------------|------------------------------------|
| OS                    | linux 6.6.22_1 glibc               |
| Model                 | Realme book RMNBXXXX MP            |
| Chip                  | 11th Gen Intel i5-1135G7)          |
| Total Number of Cores | 8                                  |
| Memory                | 8 GB                               |

Benchmark                                                                 (arraySize)   Mode  Cnt      Score      Error   Units
column4jBench.arrow.AggregatorBenchmark.linearSearch                               16  thrpt   15   8824.499 ±  486.155  ops/ms
column4jBench.arrow.AggregatorBenchmark.linearSearch                              128  thrpt   15   1967.243 ±  214.720  ops/ms
column4jBench.arrow.AggregatorBenchmark.linearSearch                             1024  thrpt   15    291.941 ±    1.874  ops/ms
column4jBench.arrow.AggregatorBenchmark.liniarSearchFieldsInitialization           16  thrpt   15   2170.481 ±    9.658  ops/ms
column4jBench.arrow.AggregatorBenchmark.liniarSearchFieldsInitialization          128  thrpt   15   1239.013 ±   55.008  ops/ms
column4jBench.arrow.AggregatorBenchmark.liniarSearchFieldsInitialization         1024  thrpt   15    276.481 ±    1.096  ops/ms
column4jBench.arrow.ReadWriteBenchmark.allocateArrow                               16  thrpt   15   1990.727 ±   47.739  ops/ms
column4jBench.arrow.ReadWriteBenchmark.allocateArrow                              128  thrpt   15   2090.799 ±   66.814  ops/ms
column4jBench.arrow.ReadWriteBenchmark.allocateArrow                             1024  thrpt   15   1285.582 ±   42.198  ops/ms
column4jBench.arrow.ReadWriteBenchmark.readArrow                                   16  thrpt   15  13812.409 ±  306.549  ops/ms
column4jBench.arrow.ReadWriteBenchmark.readArrow                                  128  thrpt   15   2336.011 ±   17.874  ops/ms
column4jBench.arrow.ReadWriteBenchmark.readArrow                                 1024  thrpt   15    298.074 ±    0.572  ops/ms
column4jBench.arrow.ReadWriteBenchmark.writeArrow                                  16  thrpt   15  10599.857 ±  193.734  ops/ms
column4jBench.arrow.ReadWriteBenchmark.writeArrow                                 128  thrpt   15   1783.475 ±   15.253  ops/ms
column4jBench.arrow.ReadWriteBenchmark.writeArrow                                1024  thrpt   15    203.196 ±    0.548  ops/ms
column4jBench.scalar.AggregatorBenchmark.linearSearch                              16  thrpt   15  55207.929 ±  711.417  ops/ms
column4jBench.scalar.AggregatorBenchmark.linearSearch                             128  thrpt   15  31851.177 ±  613.358  ops/ms
column4jBench.scalar.AggregatorBenchmark.linearSearch                            1024  thrpt   15   9988.297 ±   55.989  ops/ms
column4jBench.scalar.AggregatorBenchmark.min                                       16  thrpt   15  41450.711 ± 3016.984  ops/ms
column4jBench.scalar.AggregatorBenchmark.min                                      128  thrpt   15  11874.126 ±  738.097  ops/ms
column4jBench.scalar.AggregatorBenchmark.min                                     1024  thrpt   15   1736.689 ±   80.898  ops/ms
