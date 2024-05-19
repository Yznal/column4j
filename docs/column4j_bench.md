## Results of JMH measurements for column4j

| System parameter      | Value                              |
|-----------------------|------------------------------------|
| OS                    | linux 6.6.22_1 glibc               |
| Model                 | Realme book RMNBXXXX MP            |
| Chip                  | 11th Gen Intel i5-1135G7)          |
| Total Number of Cores | 8                                  |
| Memory                | 8 GB                               |


Benchmark                                                 (arraySize)  (columnChunkSize)   Mode  Cnt      Score      Error   Units
o.c.aggregate.Int32AggregatorBenchmark.indexOf                     16                 16  thrpt   15  34619.570 ± 1105.530  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.indexOf                     16                128  thrpt   15  33724.078 ±   81.936  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.indexOf                     16               1024  thrpt   15  32454.625 ±  699.615  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.indexOf                    128                 16  thrpt   15  32545.319 ±  113.433  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.indexOf                    128                128  thrpt   15  35146.898 ±  436.149  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.indexOf                    128               1024  thrpt   15  35641.109 ±  534.283  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.indexOf                   1024                 16  thrpt   15  30400.181 ±  133.397  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.indexOf                   1024                128  thrpt   15  35189.474 ±  153.277  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.indexOf                   1024               1024  thrpt   15  35621.106 ± 1024.574  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.min                         16                 16  thrpt   15  29321.082 ± 2205.025  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.min                         16                128  thrpt   15  26838.485 ± 1594.420  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.min                         16               1024  thrpt   15  27644.935 ± 1335.366  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.min                        128                 16  thrpt   15  15745.037 ± 2314.624  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.min                        128                128  thrpt   15  18607.765 ± 1439.324  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.min                        128               1024  thrpt   15  19725.107 ± 2593.842  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.min                       1024                 16  thrpt   15   5164.097 ±  583.861  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.min                       1024                128  thrpt   15  12517.556 ± 2509.705  ops/ms
o.c.aggregate.Int32AggregatorBenchmark.min                       1024               1024  thrpt   15  12282.126 ± 2863.315  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.indexOf                     16                 16  thrpt   15  31370.115 ± 2643.783  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.indexOf                     16                128  thrpt   15  31527.571 ± 2665.555  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.indexOf                     16               1024  thrpt   15  32208.975 ± 1369.806  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.indexOf                    128                 16  thrpt   15  31704.804 ± 2176.034  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.indexOf                    128                128  thrpt   15  33833.938 ± 1255.741  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.indexOf                    128               1024  thrpt   15  31730.031 ± 3261.854  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.indexOf                   1024                 16  thrpt   15  28889.897 ±  876.629  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.indexOf                   1024                128  thrpt   15  32359.112 ±  746.861  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.indexOf                   1024               1024  thrpt   15  32555.304 ±  551.317  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.min                         16                 16  thrpt   15  25445.647 ±  371.203  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.min                         16                128  thrpt   15  24731.990 ±  389.632  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.min                         16               1024  thrpt   15  24303.947 ±  678.290  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.min                        128                 16  thrpt   15  15347.625 ±  750.921  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.min                        128                128  thrpt   15  20097.104 ±  137.737  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.min                        128               1024  thrpt   15  19313.698 ±  688.657  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.min                       1024                 16  thrpt   15   3967.517 ±  182.514  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.min                       1024                128  thrpt   15  12419.367 ±  223.831  ops/ms
o.c.aggregate.Int64AggregatorBenchmark.min                       1024               1024  thrpt   15   7158.623 ±  132.499  ops/ms
o.c.utils.Int32VectorUtilsBenchmark.indexOfByVectorUtils           16                     thrpt   15  50070.070 ±  317.873  ops/ms
o.c.utils.Int32VectorUtilsBenchmark.indexOfByVectorUtils          128                     thrpt   15  45463.155 ± 1709.579  ops/ms
o.c.utils.Int32VectorUtilsBenchmark.indexOfByVectorUtils         1024                     thrpt   15  30158.942 ± 2907.329  ops/ms
o.c.utils.Int32VectorUtilsBenchmark.minByVectorUtils               16                     thrpt   15  44837.934 ±  500.942  ops/ms
o.c.utils.Int32VectorUtilsBenchmark.minByVectorUtils              128                     thrpt   15  37094.824 ±  212.234  ops/ms
o.c.utils.Int32VectorUtilsBenchmark.minByVectorUtils             1024                     thrpt   15  17732.230 ±   69.407  ops/ms
o.c.utils.Int64VectorUtilsBenchmark.indexOfByVectorUtils           16                     thrpt   15  48913.291 ±  271.524  ops/ms
o.c.utils.Int64VectorUtilsBenchmark.indexOfByVectorUtils          128                     thrpt   15  42770.241 ±  182.279  ops/ms
o.c.utils.Int64VectorUtilsBenchmark.indexOfByVectorUtils         1024                     thrpt   15  23052.546 ±  657.778  ops/ms
o.c.utils.Int64VectorUtilsBenchmark.minByVectorUtils               16                     thrpt   15  40834.562 ±  237.025  ops/ms
o.c.utils.Int64VectorUtilsBenchmark.minByVectorUtils              128                     thrpt   15  27203.252 ±  334.461  ops/ms
o.c.utils.Int64VectorUtilsBenchmark.minByVectorUtils             1024                     thrpt   15   7937.767 ±   45.246  ops/ms
