## Results of JMH measurements for arrows

| System parameter      | Value                              |
|-----------------------|------------------------------------|
| OS                    | linux 6.6.22_1 glibc               |
| Model                 | Realme book RMNBXXXX MP            |
| Chip                  | 11th Gen Intel i5-1135G7)          |
| Total Number of Cores | 8                                  |
| Memory                | 8 GB                               |

Benchmark                                                                 (arraySize)  (threads)   Mode  Cnt      Score      Error   Units
column4jBench.array.AggregatorBenchmark.linearSearch                               16        N/A  thrpt   15  55041.332 ± 1340.904  ops/ms
column4jBench.array.AggregatorBenchmark.linearSearch                              128        N/A  thrpt   15  32517.131 ±   96.250  ops/ms
column4jBench.array.AggregatorBenchmark.linearSearch                             1024        N/A  thrpt   15  10079.256 ±   36.940  ops/ms
column4jBench.array.AggregatorBenchmark.min                                        16        N/A  thrpt   15  43663.086 ±  737.838  ops/ms
column4jBench.array.AggregatorBenchmark.min                                       128        N/A  thrpt   15  12704.400 ±   25.738  ops/ms
column4jBench.array.AggregatorBenchmark.min                                      1024        N/A  thrpt   15   1920.498 ±    4.011  ops/ms
column4jBench.arrow.AggregatorBenchmark.linearSearch                               16        N/A  thrpt   15   9355.019 ±  282.359  ops/ms
column4jBench.arrow.AggregatorBenchmark.linearSearch                              128        N/A  thrpt   15   2117.588 ±   52.077  ops/ms
column4jBench.arrow.AggregatorBenchmark.linearSearch                             1024        N/A  thrpt   15    296.375 ±    1.501  ops/ms
column4jBench.arrow.AggregatorBenchmark.paralelSearch                              16          1  thrpt   15    159.556 ±    2.598  ops/ms
column4jBench.arrow.AggregatorBenchmark.paralelSearch                              16          2  thrpt   15    131.918 ±    1.848  ops/ms
column4jBench.arrow.AggregatorBenchmark.paralelSearch                              16          4  thrpt   15    141.628 ±   11.268  ops/ms
column4jBench.arrow.AggregatorBenchmark.paralelSearch                             128          1  thrpt   15    146.740 ±    1.827  ops/ms
column4jBench.arrow.AggregatorBenchmark.paralelSearch                             128          2  thrpt   15    125.420 ±    0.826  ops/ms
column4jBench.arrow.AggregatorBenchmark.paralelSearch                             128          4  thrpt   15    122.138 ±    7.707  ops/ms
column4jBench.arrow.AggregatorBenchmark.paralelSearch                            1024          1  thrpt   15     77.110 ±    3.188  ops/ms
column4jBench.arrow.AggregatorBenchmark.paralelSearch                            1024          2  thrpt   15     84.447 ±    1.177  ops/ms
column4jBench.arrow.AggregatorBenchmark.paralelSearch                            1024          4  thrpt   15     78.808 ±    0.445  ops/ms
column4jBench.arrow.ReadWriteBenchmark.allocateArrow                               16        N/A  thrpt   15   2183.879 ±   62.796  ops/ms
column4jBench.arrow.ReadWriteBenchmark.allocateArrow                              128        N/A  thrpt   15   2107.016 ±   79.461  ops/ms
column4jBench.arrow.ReadWriteBenchmark.allocateArrow                             1024        N/A  thrpt   15   1318.336 ±   24.734  ops/ms
column4jBench.arrow.ReadWriteBenchmark.readArrow                                   16        N/A  thrpt   15  14594.657 ±  158.764  ops/ms
column4jBench.arrow.ReadWriteBenchmark.readArrow                                  128        N/A  thrpt   15   2432.770 ±   12.980  ops/ms
column4jBench.arrow.ReadWriteBenchmark.readArrow                                 1024        N/A  thrpt   15    302.592 ±    0.292  ops/ms
column4jBench.arrow.ReadWriteBenchmark.writeArrow                                  16        N/A  thrpt   15  11282.501 ±  160.944  ops/ms
column4jBench.arrow.ReadWriteBenchmark.writeArrow                                 128        N/A  thrpt   15   1851.215 ±   27.037  ops/ms
column4jBench.arrow.ReadWriteBenchmark.writeArrow                                1024        N/A  thrpt   15    205.358 ±    0.926  ops/ms
column4jBench.array.ReadWriteBenchmark.allocateArray                               16        N/A  thrpt    5  51292.039 ± 10529.463  ops/ms
column4jBench.array.ReadWriteBenchmark.allocateArray                              128        N/A  thrpt    5  34934.571 ±  9937.084  ops/ms
column4jBench.array.ReadWriteBenchmark.allocateArray                             1024        N/A  thrpt    5   5177.278 ±  1320.133  ops/ms
column4jBench.array.ReadWriteBenchmark.readArrow                                   16        N/A  thrpt    5  57706.722 ±   797.484  ops/ms
column4jBench.array.ReadWriteBenchmark.readArrow                                  128        N/A  thrpt    5  31827.553 ±   623.165  ops/ms
column4jBench.array.ReadWriteBenchmark.readArrow                                 1024        N/A  thrpt    5   6478.512 ±   392.659  ops/ms
column4jBench.array.ReadWriteBenchmark.writeArrow                                  16        N/A  thrpt    5  52252.605 ±  2796.664  ops/ms
column4jBench.array.ReadWriteBenchmark.writeArrow                                 128        N/A  thrpt    5  31784.833 ±  1016.647  ops/ms
column4jBench.array.ReadWriteBenchmark.writeArrow                                1024        N/A  thrpt    5  14306.745 ±  1036.481  ops/ms
