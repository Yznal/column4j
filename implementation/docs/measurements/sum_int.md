## Results of JMH measurements

### Test: SumIntArrays

Test compare sum int arrays.

| System parameter      | Value                              |
|-----------------------|------------------------------------|
| Model Name            | MacBook Air                        |
| Model Identifier      | MacBookAir10,1                     |
| Chip                  | Apple M1                           |
| Total Number of Cores | 8 (4 performance and 4 efficiency) |
| Memory                | 16 GB                              |

```text
Benchmark                           (arraySize)   Mode  Cnt          Score          Error  Units
SumIntArrays.benchmarkSumViaFor              16  thrpt    5  223344546.275 ± 22883096.333  ops/s
SumIntArrays.benchmarkSumViaFor             128  thrpt    5   36911428.216 ±  2802690.411  ops/s
SumIntArrays.benchmarkSumViaFor            1024  thrpt    5    3105441.574 ±   371548.072  ops/s
SumIntArrays.benchmarkSumViaVector           16  thrpt    5  420479305.230 ± 37920919.377  ops/s
SumIntArrays.benchmarkSumViaVector          128  thrpt    5   86758045.965 ±  3241335.484  ops/s
SumIntArrays.benchmarkSumViaVector         1024  thrpt    5   10389387.228 ±  1000003.007  ops/s
```

| System parameter      | Value                               |
|-----------------------|-------------------------------------|
| Model Name            | MacBook Pro                         |
| Model Identifier      | Mac14,9                             |
| Chip                  | Apple M2 Pro                        |
| Total Number of Cores | 10 (6 performance and 4 efficiency) |
| Memory                | 16 GB                               |

```text
Benchmark                           (arraySize)   Mode  Cnt          Score          Error  Units
SumIntArrays.benchmarkSumViaFor              16  thrpt    5  262854804.677 ±  2403127.290  ops/s
SumIntArrays.benchmarkSumViaFor             128  thrpt    5   41675156.693 ±   613593.360  ops/s
SumIntArrays.benchmarkSumViaFor            1024  thrpt    5    3510297.980 ±    72568.823  ops/s
SumIntArrays.benchmarkSumViaVector           16  thrpt    5  469081653.488 ± 13140325.206  ops/s
SumIntArrays.benchmarkSumViaVector          128  thrpt    5   93508279.582 ±   400491.486  ops/s
SumIntArrays.benchmarkSumViaVector         1024  thrpt    5   11057285.197 ±   204369.082  ops/s
```

| System parameter      | Value                                           |
|-----------------------|-------------------------------------------------|
| OS                    | Windows 11                                      |
| Chip                  | 13th Gen Intel(R) Core(TM) i7-13700K   3.40 GHz |
| Total Number of Cores | 16 (8 performance and 8 efficiency)             |
| Memory                | 32 GB                                           |

```text
Benchmark                           (arraySize)   Mode  Cnt          Score          Error  Units
SumIntArrays.benchmarkSumViaFor              16  thrpt    5  426407705,732   12232483,273  ops/s
SumIntArrays.benchmarkSumViaFor             128  thrpt    5   62082480,400    1078539,908  ops/s
SumIntArrays.benchmarkSumViaFor            1024  thrpt    5    5306158,066      99565,206  ops/s
SumIntArrays.benchmarkSumViaVector           16  thrpt    5  726277909,096   17602825,141  ops/s
SumIntArrays.benchmarkSumViaVector          128  thrpt    5  240821202,966    2174203,280  ops/s
SumIntArrays.benchmarkSumViaVector         1024  thrpt    5   25886282,962     119370,944  ops/s
```


| System parameter      | Value                                           |
|-----------------------|-------------------------------------------------|
| OS                    | Linux 5.15.0-86-generic x86_64 x86_64 GNU/Linux |
| Chip                  | Intel Xeon Processor (Icelake)                  |
| Memory                | 4 GB                                            |

```text
Benchmark                           (arraySize)   Mode  Cnt          Score          Error  Units
SumIntArrays.benchmarkSumViaFor              16  thrpt    5  152158725.823 ±  1263903.576  ops/s
SumIntArrays.benchmarkSumViaFor             128  thrpt    5   27129017.126 ±    41558.695  ops/s
SumIntArrays.benchmarkSumViaFor            1024  thrpt    5    2587013.049 ±     1892.255  ops/s
SumIntArrays.benchmarkSumViaVector           16  thrpt    5  304075034.342 ± 29815142.929  ops/s
SumIntArrays.benchmarkSumViaVector          128  thrpt    5  135596378.139 ±   934321.705  ops/s
SumIntArrays.benchmarkSumViaVector         1024  thrpt    5   19681629.643 ±    71251.811  ops/s
```
