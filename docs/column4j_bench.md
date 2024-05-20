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
Int32MutableColumnBenchmark.allocColumn4j                          16                 16  thrpt    5  25856.215 ± 17217.732  ops/ms
Int32MutableColumnBenchmark.allocColumn4j                          16                128  thrpt    5  26568.112 ±  4154.936  ops/ms
Int32MutableColumnBenchmark.allocColumn4j                          16               1024  thrpt    5  26656.780 ±  9346.628  ops/ms
Int32MutableColumnBenchmark.allocColumn4j                         128                 16  thrpt    5  19976.640 ±  2598.818  ops/ms
Int32MutableColumnBenchmark.allocColumn4j                         128                128  thrpt    5  19476.608 ±  2446.693  ops/ms
Int32MutableColumnBenchmark.allocColumn4j                         128               1024  thrpt    5  20083.971 ± 14496.598  ops/ms
Int32MutableColumnBenchmark.allocColumn4j                        1024                 16  thrpt    5   2313.887 ±  1213.339  ops/ms
Int32MutableColumnBenchmark.allocColumn4j                        1024                128  thrpt    5   2197.149 ±  1668.026  ops/ms
Int32MutableColumnBenchmark.allocColumn4j                        1024               1024  thrpt    5   2153.097 ±  1814.479  ops/ms
Int32MutableColumnBenchmark.readColumn4j                           16                 16  thrpt    5  19214.971 ±  1278.726  ops/ms
Int32MutableColumnBenchmark.readColumn4j                           16                128  thrpt    5  19393.957 ±  4649.699  ops/ms
Int32MutableColumnBenchmark.readColumn4j                           16               1024  thrpt    5  16640.011 ± 15253.332  ops/ms
Int32MutableColumnBenchmark.readColumn4j                          128                 16  thrpt    5   4142.994 ±   522.401  ops/ms
Int32MutableColumnBenchmark.readColumn4j                          128                128  thrpt    5   4136.070 ±   154.538  ops/ms
Int32MutableColumnBenchmark.readColumn4j                          128               1024  thrpt    5   4192.788 ±   336.437  ops/ms
Int32MutableColumnBenchmark.readColumn4j                         1024                 16  thrpt    5    592.989 ±    27.772  ops/ms
Int32MutableColumnBenchmark.readColumn4j                         1024                128  thrpt    5    578.398 ±    78.074  ops/ms
Int32MutableColumnBenchmark.readColumn4j                         1024               1024  thrpt    5    594.162 ±    19.510  ops/ms
Int32MutableColumnBenchmark.writeColumn4j                          16                 16  thrpt    5   5970.837 ±  1520.531  ops/ms
Int32MutableColumnBenchmark.writeColumn4j                          16                128  thrpt    5   5521.468 ±   452.279  ops/ms
Int32MutableColumnBenchmark.writeColumn4j                          16               1024  thrpt    5   5263.120 ±  1345.087  ops/ms
Int32MutableColumnBenchmark.writeColumn4j                         128                 16  thrpt    5    674.879 ±   386.280  ops/ms
Int32MutableColumnBenchmark.writeColumn4j                         128                128  thrpt    5    692.716 ±   107.897  ops/ms
Int32MutableColumnBenchmark.writeColumn4j                         128               1024  thrpt    5    626.065 ±   152.787  ops/ms
Int32MutableColumnBenchmark.writeColumn4j                        1024                 16  thrpt    5     86.174 ±    29.878  ops/ms
Int32MutableColumnBenchmark.writeColumn4j                        1024                128  thrpt    5     74.413 ±    37.770  ops/ms
Int32MutableColumnBenchmark.writeColumn4j                        1024               1024  thrpt    5     76.233 ±    24.252  ops/ms
Int64MutableColumnBenchmark.allocColumn4j                          16                 16  thrpt    5  20446.017 ±  5343.415  ops/ms
Int64MutableColumnBenchmark.allocColumn4j                          16                128  thrpt    5  24223.416 ±  2039.617  ops/ms
Int64MutableColumnBenchmark.allocColumn4j                          16               1024  thrpt    5  24439.085 ±  2165.927  ops/ms
Int64MutableColumnBenchmark.allocColumn4j                         128                 16  thrpt    5  10169.040 ±  1223.993  ops/ms
Int64MutableColumnBenchmark.allocColumn4j                         128                128  thrpt    5  10654.015 ±   505.705  ops/ms
Int64MutableColumnBenchmark.allocColumn4j                         128               1024  thrpt    5   9864.922 ±  1007.799  ops/ms
Int64MutableColumnBenchmark.allocColumn4j                        1024                 16  thrpt    5   1248.804 ±   697.014  ops/ms
Int64MutableColumnBenchmark.allocColumn4j                        1024                128  thrpt    5   1059.913 ±   666.662  ops/ms
Int64MutableColumnBenchmark.allocColumn4j                        1024               1024  thrpt    5    891.001 ±   351.610  ops/ms
Int64MutableColumnBenchmark.readColumn4j                           16                 16  thrpt    5  18713.434 ±  5678.290  ops/ms
Int64MutableColumnBenchmark.readColumn4j                           16                128  thrpt    5  21427.118 ±   578.098  ops/ms
Int64MutableColumnBenchmark.readColumn4j                           16               1024  thrpt    5  21683.047 ±   567.505  ops/ms
Int64MutableColumnBenchmark.readColumn4j                          128                 16  thrpt    5   4320.048 ±   111.803  ops/ms
Int64MutableColumnBenchmark.readColumn4j                          128                128  thrpt    5   4323.985 ±   162.325  ops/ms
Int64MutableColumnBenchmark.readColumn4j                          128               1024  thrpt    5   4328.814 ±   102.354  ops/ms
Int64MutableColumnBenchmark.readColumn4j                         1024                 16  thrpt    5    569.685 ±   224.512  ops/ms
Int64MutableColumnBenchmark.readColumn4j                         1024                128  thrpt    5    569.376 ±   228.858  ops/ms
Int64MutableColumnBenchmark.readColumn4j                         1024               1024  thrpt    5    566.211 ±   218.908  ops/ms
Int64MutableColumnBenchmark.writeColumn4j                          16                 16  thrpt    5   5733.058 ±   163.709  ops/ms
Int64MutableColumnBenchmark.writeColumn4j                          16                128  thrpt    5   5711.847 ±   110.329  ops/ms
Int64MutableColumnBenchmark.writeColumn4j                          16               1024  thrpt    5   5747.695 ±   114.945  ops/ms
Int64MutableColumnBenchmark.writeColumn4j                         128                 16  thrpt    5    763.087 ±     3.162  ops/ms
Int64MutableColumnBenchmark.writeColumn4j                         128                128  thrpt    5    752.908 ±    21.331  ops/ms
Int64MutableColumnBenchmark.writeColumn4j                         128               1024  thrpt    5    758.945 ±    16.190  ops/ms
Int64MutableColumnBenchmark.writeColumn4j                        1024                 16  thrpt    5     92.005 ±     4.544  ops/ms
Int64MutableColumnBenchmark.writeColumn4j                        1024                128  thrpt    5     92.870 ±     3.078  ops/ms
Int64MutableColumnBenchmark.writeColumn4j                        1024               1024  thrpt    5     92.952 ±     2.295  ops/ms
