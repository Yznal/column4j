## Results of JMH measurements for arrows

| System parameter      | Value                              |
|-----------------------|------------------------------------|
| OS                    | linux 6.6.22_1 glibc               |
| Model                 | Realme book RMNBXXXX MP            |
| Chip                  | 11th Gen Intel i5-1135G7)          |
| Total Number of Cores | 8                                  |
| Memory                | 8 GB                               |


Benchmark                                        (arraySize)   Mode  Cnt         Score        Error  Units
column4jBench.AggregatorBenchmark.linearSearch            16  thrpt    5  14840275.855 ± 180273.934  ops/s
column4jBench.AggregatorBenchmark.linearSearch           128  thrpt    5   2222931.245 ±  59059.621  ops/s
column4jBench.AggregatorBenchmark.linearSearch          1024  thrpt    5    302528.875 ±   2996.990  ops/s
column4jBench.ReadWriteBenchmark.writeArrow               16  thrpt    5   1643268.714 ±  26996.915  ops/s
column4jBench.ReadWriteBenchmark.writeArrow              128  thrpt    5    909624.114 ±  12949.576  ops/s
column4jBench.ReadWriteBenchmark.writeArrow             1024  thrpt    5    189452.257 ±   1508.163  ops/s
column4jBench.ReadWriteBenchmark.writeReadArrow           16  thrpt    5   1581427.469 ±  32457.480  ops/s
column4jBench.ReadWriteBenchmark.writeReadArrow          128  thrpt    5    696393.230 ±  14069.951  ops/s
column4jBench.ReadWriteBenchmark.writeReadArrow         1024  thrpt    5    128387.757 ±   1561.208  ops/s
