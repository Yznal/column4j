**Column4j**

In memory column data store for Java.

Ссылки:
* [Базовые концепции мониторинга](https://docs.victoriametrics.com/keyConcepts.html)
* На какие реализации можно ссылаться посмотреть:
    * https://arrow.apache.org/
    * https://ignite.apache.org/
    * https://hazelcast.com/
    * https://clickhouse.com/docs/en/optimize/skipping-indexes
    * https://clickhouse.com/docs/en/development/architecture

Используем:
* [OpenJDK 21](https://docs.aws.amazon.com/corretto/latest/corretto-21-ug/downloads-list.html) и [Early Access Build с Panama](https://jdk.java.net/jextract/).
* [Vector API](https://openjdk.org/jeps/417).
* [JMH](https://github.com/openjdk/jmh) для сравнения имплементаций.


Два сценария использования:
1. Храним колонки со значениями таймсерий и реализуем различные операции над векторами таймсерий.
2. Храним строки для выполнения OLAP-запросов и поиска значений в таймсерии.
