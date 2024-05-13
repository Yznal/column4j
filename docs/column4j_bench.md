## Results of JMH measurements for column4j

| System parameter      | Value                              |
|-----------------------|------------------------------------|
| OS                    | linux 6.6.22_1 glibc               |
| Model                 | Realme book RMNBXXXX MP            |
| Chip                  | 11th Gen Intel i5-1135G7)          |
| Total Number of Cores | 8                                  |
| Memory                | 8 GB                               |


Benchmark                                                                   (arraySize)  (columnChunkSize)   Mode  Cnt          Score          Error  Units
o.c.aggregate.Int32AggregatorBenchmark.indexOfAnother                                16                 16  thrpt    5  154391205.740 ±  2210945.796  ops/s
o.c.aggregate.Int32AggregatorBenchmark.indexOfAnother                                16                128  thrpt    5  155196050.139 ±  1042149.878  ops/s
o.c.aggregate.Int32AggregatorBenchmark.indexOfAnother                                16               1024  thrpt    5  155276925.188 ±  2018289.068  ops/s
o.c.aggregate.Int32AggregatorBenchmark.indexOfAnother                               128                 16  thrpt    5  203564031.807 ±  8360906.051  ops/s
o.c.aggregate.Int32AggregatorBenchmark.indexOfAnother                               128                128  thrpt    5  154834726.937 ±  1468669.951  ops/s
o.c.aggregate.Int32AggregatorBenchmark.indexOfAnother                               128               1024  thrpt    5  154860862.976 ±  1924434.995  ops/s
o.c.aggregate.Int32AggregatorBenchmark.indexOfAnother                              1024                 16  thrpt    5  207837633.184 ± 10374354.555  ops/s
o.c.aggregate.Int32AggregatorBenchmark.indexOfAnother                              1024                128  thrpt    5  204296434.013 ±  9643255.605  ops/s
o.c.aggregate.Int32AggregatorBenchmark.indexOfAnother                              1024               1024  thrpt    5  155594799.000 ±  1203125.102  ops/s
o.c.aggregate.Int32AggregatorBenchmark.min                                           16                 16  thrpt    5   63572117.059 ±  1236248.009  ops/s
o.c.aggregate.Int32AggregatorBenchmark.min                                           16                128  thrpt    5   63553320.450 ±   441152.936  ops/s
o.c.aggregate.Int32AggregatorBenchmark.min                                           16               1024  thrpt    5   62748408.769 ±  2387306.218  ops/s
o.c.aggregate.Int32AggregatorBenchmark.min                                          128                 16  thrpt    5   26903074.260 ±   369351.226  ops/s
o.c.aggregate.Int32AggregatorBenchmark.min                                          128                128  thrpt    5   40376738.868 ±  2141981.512  ops/s
o.c.aggregate.Int32AggregatorBenchmark.min                                          128               1024  thrpt    5   40138063.376 ±   358444.633  ops/s
o.c.aggregate.Int32AggregatorBenchmark.min                                         1024                 16  thrpt    5    6384108.145 ±  1289876.061  ops/s
o.c.aggregate.Int32AggregatorBenchmark.min                                         1024                128  thrpt    5   17014345.236 ±  3111963.844  ops/s
o.c.aggregate.Int32AggregatorBenchmark.min                                         1024               1024  thrpt    5   15800151.402 ±  8674193.696  ops/s
o.c.aggregate.Int64AggregatorBenchmark.indexOfAnother                                16                 16  thrpt    5  115066148.581 ± 61084435.483  ops/s
o.c.aggregate.Int64AggregatorBenchmark.indexOfAnother                                16                128  thrpt    5  122520724.603 ± 66292237.396  ops/s
o.c.aggregate.Int64AggregatorBenchmark.indexOfAnother                                16               1024  thrpt    5  147839851.406 ±  6216969.564  ops/s
o.c.aggregate.Int64AggregatorBenchmark.indexOfAnother                               128                 16  thrpt    5  191253141.942 ± 11735823.342  ops/s
o.c.aggregate.Int64AggregatorBenchmark.indexOfAnother                               128                128  thrpt    5  138686219.050 ± 69690698.766  ops/s
o.c.aggregate.Int64AggregatorBenchmark.indexOfAnother                               128               1024  thrpt    5  150849351.221 ±  8255876.747  ops/s
o.c.aggregate.Int64AggregatorBenchmark.indexOfAnother                              1024                 16  thrpt    5  199955630.754 ±  4149075.737  ops/s
o.c.aggregate.Int64AggregatorBenchmark.indexOfAnother                              1024                128  thrpt    5  200665236.184 ±  1724532.201  ops/s
o.c.aggregate.Int64AggregatorBenchmark.indexOfAnother                              1024               1024  thrpt    5  154262671.699 ±   529509.580  ops/s
o.c.aggregate.Int64AggregatorBenchmark.min                                           16                 16  thrpt    5   59982802.658 ±   286486.337  ops/s
o.c.aggregate.Int64AggregatorBenchmark.min                                           16                128  thrpt    5   58875691.288 ±   317170.459  ops/s
o.c.aggregate.Int64AggregatorBenchmark.min                                           16               1024  thrpt    5   55729984.933 ±   522709.017  ops/s
o.c.aggregate.Int64AggregatorBenchmark.min                                          128                 16  thrpt    5   27825866.631 ±   112958.196  ops/s
o.c.aggregate.Int64AggregatorBenchmark.min                                          128                128  thrpt    5   41983818.854 ±  5005408.514  ops/s
o.c.aggregate.Int64AggregatorBenchmark.min                                          128               1024  thrpt    5   41400263.347 ±   534779.624  ops/s
o.c.aggregate.Int64AggregatorBenchmark.min                                         1024                 16  thrpt    5    6522022.392 ±  1261187.529  ops/s
o.c.aggregate.Int64AggregatorBenchmark.min                                         1024                128  thrpt    5   18705558.199 ±   514729.014  ops/s
o.c.aggregate.Int64AggregatorBenchmark.min                                         1024               1024  thrpt    5    9210518.134 ±   131088.967  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeColumn4j               16                 16  thrpt    5    9941270.040 ±   125433.927  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeColumn4j               16                128  thrpt    5    9075779.700 ±   106507.438  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeColumn4j               16               1024  thrpt    5    2477907.888 ±    23048.769  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeColumn4j              128                 16  thrpt    5     915624.427 ±    18992.007  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeColumn4j              128                128  thrpt    5    2045685.607 ±    50052.532  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeColumn4j              128               1024  thrpt    5    1123810.978 ±     6767.512  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeColumn4j             1024                 16  thrpt    5     117887.824 ±     2604.548  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeColumn4j             1024                128  thrpt    5     149236.432 ±     3708.276  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeColumn4j             1024               1024  thrpt    5     216669.812 ±     5990.826  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeReadColumn4j           16                 16  thrpt    5    8192015.043 ±   107008.941  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeReadColumn4j           16                128  thrpt    5    7177941.716 ±    71150.697  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeReadColumn4j           16               1024  thrpt    5    2356532.105 ±    11988.923  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeReadColumn4j          128                 16  thrpt    5     809678.457 ±    11437.477  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeReadColumn4j          128                128  thrpt    5    1354003.112 ±    23983.202  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeReadColumn4j          128               1024  thrpt    5     917268.155 ±     9224.196  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeReadColumn4j         1024                 16  thrpt    5      96019.672 ±      966.654  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeReadColumn4j         1024                128  thrpt    5     115595.740 ±     2015.164  ops/s
o.c.column.mutable.primitive.Int32MutableColumnBenchmark.writeReadColumn4j         1024               1024  thrpt    5     154238.578 ±     3186.169  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeColumn4j               16                 16  thrpt    5    9381293.995 ±    80125.975  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeColumn4j               16                128  thrpt    5    7121111.689 ±    78510.597  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeColumn4j               16               1024  thrpt    5    1341531.773 ±     5988.532  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeColumn4j              128                 16  thrpt    5     954717.784 ±     4908.140  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeColumn4j              128                128  thrpt    5    1660056.376 ±   556525.233  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeColumn4j              128               1024  thrpt    5     818383.861 ±    25630.740  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeColumn4j             1024                 16  thrpt    5     118314.733 ±     1790.634  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeColumn4j             1024                128  thrpt    5     160144.759 ±    11525.056  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeColumn4j             1024               1024  thrpt    5     188637.643 ±     2171.880  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeReadColumn4j           16                 16  thrpt    5    7504872.278 ±    69491.558  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeReadColumn4j           16                128  thrpt    5    5866649.340 ±    55988.810  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeReadColumn4j           16               1024  thrpt    5    1292139.503 ±     3778.856  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeReadColumn4j          128                 16  thrpt    5     769585.178 ±    11463.501  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeReadColumn4j          128                128  thrpt    5    1143158.707 ±    90400.801  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeReadColumn4j          128               1024  thrpt    5     692847.396 ±    22815.705  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeReadColumn4j         1024                 16  thrpt    5      99349.295 ±     1231.307  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeReadColumn4j         1024                128  thrpt    5     129158.311 ±     1320.822  ops/s
o.c.column.mutable.primitive.Int64MutableColumnBenchmark.writeReadColumn4j         1024               1024  thrpt    5     150784.488 ±     1075.017  ops/s
