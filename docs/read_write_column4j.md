## Results of JMH measurements

### Test: writeReadColumn4j

| System parameter      | Value                              |
|-----------------------|------------------------------------|
| OS                    | linux 6.6.22_1 glibc               |
| Model                 | Realme book RMNBXXXX MP            |
| Chip                  | 11th Gen Intel i5-1135G7)          |
| Total Number of Cores | 8                                  |
| Memory                | 8 GB                               |

Benchmark                                                      (arraySize)  (columnChunkSize)   Mode  Cnt        Score         Error  Units
Int64MutableColumnBenchmark.writeReadColumn4j           16                 16  thrpt    5  7662627.811 ±  520716.574  ops/s
Int64MutableColumnBenchmark.writeReadColumn4j           16                128  thrpt    5  5910995.631 ±   24376.917  ops/s
Int64MutableColumnBenchmark.writeReadColumn4j           16               1024  thrpt    5  1291707.753 ±    2936.836  ops/s
Int64MutableColumnBenchmark.writeReadColumn4j          128                 16  thrpt    5   763446.295 ±   19768.576  ops/s
Int64MutableColumnBenchmark.writeReadColumn4j          128                128  thrpt    5  1132437.540 ±   70799.010  ops/s
Int64MutableColumnBenchmark.writeReadColumn4j          128               1024  thrpt    5   666819.391 ±   16484.081  ops/s
Int64MutableColumnBenchmark.writeReadColumn4j         1024                 16  thrpt    5    99047.863 ±    1533.931  ops/s
Int64MutableColumnBenchmark.writeReadColumn4j         1024                128  thrpt    5   126778.528 ±   12899.714  ops/s
Int64MutableColumnBenchmark.writeReadColumn4j         1024               1024  thrpt    5   142988.785 ±    1477.508  ops/s
