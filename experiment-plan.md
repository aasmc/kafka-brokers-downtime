# План эксперимента: отключение трёх из шести брокеров Kafka

## 1. Цель и границы

Проверить поведение кластера Apache Kafka 3.9.0 с ZooKeeper, продюсера и консюмера при внезапном отказе всех брокеров, которым назначены реплики единственной партиции топика.

Проверяются два независимых прогона:

- `unclean.leader.election.enable=false`;
- `unclean.leader.election.enable=true`.

Неизменные параметры топика `downtime-experiment`:

- одна партиция;
- replication factor = 3;
- `min.insync.replicas=2`;
- replicas = `[1,2,3]`, исходный leader = `1`;
- брокеры `4,5,6` остаются работать, но реплики этой партиции на них не назначены.

Отдельно проверяется не только смена лидера, но и само предположение о «переносе партиции»: автоматическая репликация на свободные брокеры и ручное переназначение реплик — разные операции.

## 2. Основание для проверяемой гипотезы

В Kafka 3.9.0 алгоритм выбора лидера ищет сначала живую ISR-реплику в `assignment`. При разрешённом unclean election он затем ищет любую живую реплику **в том же `assignment`**. Если ни одной назначенной живой реплики нет, кандидат отсутствует независимо от значения `unclean.leader.election.enable`. Алгоритм не добавляет брокеры 4–6 в список реплик.

Ожидаемая гипотеза до прогона (не фактический результат): партиция станет offline в обоих вариантах; автоматического переноса на брокеры 4–6 не будет. Значение `unclean.leader.election.enable` в этом сценарии не должно изменить результат, потому что живой out-of-sync реплики среди назначенных тоже не остаётся.

Официальные источники:

- [Kafka 3.9: replication и leader election](https://kafka.apache.org/39/documentation.html#design_replication);
- [Kafka 3.9: `min.insync.replicas`](https://kafka.apache.org/39/documentation.html#topicconfigs_min.insync.replicas);
- [Kafka 3.9: `unclean.leader.election.enable`](https://kafka.apache.org/39/documentation.html#topicconfigs_unclean.leader.election.enable);
- [исходный код Kafka 3.9.0: `PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection`](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/controller/PartitionStateMachine.scala);
- [Kafka 3.9.0: `ProducerConfig` (`acks`, retries и `delivery.timeout.ms`)](https://github.com/apache/kafka/blob/3.9.0/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java);
- [официальный Docker-образ Apache Kafka](https://hub.docker.com/r/apache/kafka);
- [Spring Boot 4.1.2: `spring-boot-starter-webmvc` и `spring-boot-starter-kafka`](https://docs.spring.io/spring-boot/4.1.2/reference/using/build-systems.html).

## 3. Стенд

`docker-compose.yml` поднимает в общей bridge-сети `kafka-net`:

- `zookeeper:3.8.4`;
- шесть контейнеров `apache/kafka:3.9.0` с broker.id от 1 до 6;
- приложение Spring Boot 4.1.2 с Spring MVC и Spring Kafka.

Для межброкерского взаимодействия и клиентов внутри Docker используются адреса `kafka-N:9092`. Автоматическое создание топиков отключено. Настройка `unclean.leader.election.enable` задаётся отдельно для топика в каждом прогоне, а не меняется глобально на работающем кластере.

В образе `apache/kafka:3.9.0` стандартный `/etc/kafka/docker/launch` вызывает `KafkaDockerWrapper setup` и форматирование хранилища KRaft. Поэтому для ZooKeeper Compose формирует `server.properties` и запускает штатный `/opt/kafka/bin/kafka-server-start.sh` напрямую, без KRaft-only wrapper. Используемый брокер и CLI остаются версией 3.9.0.

Приложение содержит один `KafkaTemplate<String,String>`-продюсер и один `@KafkaListener`-консюмер группы `downtime-experiment`:

- продюсер отправляет одно сообщение в секунду, ключ и значение содержат монотонный sequence;
- `acks=all`, idempotence включена;
- `delivery.timeout.ms=10000`, `request.timeout.ms=3000`, `max.block.ms=5000`, чтобы отказ был виден в логах за разумное время;
- каждый ACK и каждая ошибка отправки логируются;
- консюмер логирует ключ, значение, partition и offset каждого сообщения.

Таймауты выбраны для наблюдаемости эксперимента и не являются рекомендацией для production.

## 4. Порядок проведения

### Автоматический прогон

```bash
./scripts/run-experiment.sh
```

Скрипт последовательно выполняет оба варианта с чистыми Docker volumes. Длительности можно изменить:

```bash
BASELINE_SECONDS=15 OUTAGE_SECONDS=45 RECOVERY_SECONDS=20 ./scripts/run-experiment.sh
```

### Контролируемые шаги для каждого варианта

1. Пересоздать стенд и дождаться регистрации всех шести broker.id в ZooKeeper.
2. Создать топик с явным `--replica-assignment 1:2:3`, одной партицией, RF=3, `min.insync.replicas=2` и проверяемым значением `unclean.leader.election.enable`.
3. Сохранить `kafka-topics.sh --describe` и `kafka-configs.sh --describe --all`; убедиться, что leader = 1, replicas = `[1,2,3]`, ISR содержит все три реплики.
4. Запустить приложение и дождаться как минимум одного `PRODUCER_ACK`; собрать 15 секунд нормальной работы продюсера и консюмера.
5. Зафиксировать UTC-время и выполнить `docker kill kafka-1 kafka-2 kafka-3`. Используется именно SIGKILL, а не graceful stop.
6. Через 5 секунд и ещё через 45 секунд outage сохранить описание топика через выжившие брокеры 4–6, состояние контейнеров и логи.
7. Проверить leader, replicas и ISR: отдельно зафиксировать отсутствие лидера и неизменность назначения реплик.
8. По логам приложения определить:
   - прекратились ли новые `PRODUCER_ACK` после завершения уже находившихся в работе запросов;
   - какие ошибки получает продюсер и когда срабатывает delivery timeout;
   - продолжает ли работать процесс консюмера, появляются ли новые `CONSUMER_RECORD` во время outage;
   - какие ошибки metadata/fetch видны в клиентских логах.
9. Запустить брокеры 1–3 заново, подождать 20 секунд и повторить describe. Зафиксировать восстановление лидера/ISR, ACK продюсера и чтение консюмером.
10. Сохранить полные логи до завершения соответствующего прогона. Не сравнивать варианты только по числу ошибок: моменты сетевых таймаутов и количество сообщений могут отличаться.

## 5. Критерии выводов

- **Перенос партиции:** подтверждается только изменением назначения replicas. `Leader: none` при неизменных `Replicas: 1,2,3` означает отсутствие и выбора лидера, и автоматического переназначения.
- **Доступность записи:** оценивается по `PRODUCER_ACK`/`PRODUCER_ERROR`, а не только по тому, что метод `send()` был вызван.
- **Доступность чтения:** оценивается по новым `CONSUMER_RECORD` и клиентским ошибкам. Отсутствие новых записей отдельно сверяется с отсутствием успешной записи, чтобы не принять остановку продюсера за неисправность консюмера.
- **Влияние unclean election:** сравниваются leader/replicas/ISR в двух прогонах. Этот сценарий не изолирует разницу между clean и unclean election, поскольку живых назначенных реплик не остаётся вовсе.
- **Потери и дубли:** sequence анализируется только после сопоставления ACK и offsets. Сообщения с исчерпанным delivery timeout приложение повторно не отправляет; их пропуск после восстановления не следует называть потерей подтверждённой Kafka записи.

## 6. Артефакты

Для каждого значения настройки создаётся каталог `results/false/` или `results/true/`:

- `topic-create.txt`, `topic-config.txt`;
- `topic-before.txt`, `topic-during-5s.txt`, `topic-during-final.txt`, `topic-after.txt`;
- `registered-brokers.txt`, `containers-during.txt`, `containers-final.txt`;
- `timestamps.txt`, `killed-containers.txt`, `restarted-containers.txt`;
- `broker-1.log` … `broker-6.log`, `zookeeper.log`;
- `application.log`, `producer-events.log`, `consumer-events.log`.

Фактические выводы должны ссылаться на эти файлы. Заранее предсказанные ошибки клиентов не подставляются в отчёт как реальные.

## 7. Фактические результаты

Оба варианта выполнены на стенде из этого каталога. До отказа ZooKeeper показывал все broker.id `[1, 2, 3, 4, 5, 6]`, а топик имел `Leader: 1`, `Replicas: 1,2,3`, `Isr: 1,2,3` (`registered-brokers.txt`, `topic-before.txt`).

### Состояние партиции

| Наблюдение | `unclean...=false` | `unclean...=true` |
|---|---|---|
| Через 5 секунд после SIGKILL | `Leader: 1`, `Isr: 1,2,3` — отказ ещё не обнаружен контроллером | То же |
| Через 52 секунды после SIGKILL | `Leader: none`, `Replicas: 1,2,3`, `Isr: 3` | `Leader: none`, `Replicas: 1,2,3`, `Isr: 3` |
| После возврата брокеров 1–3 | `Leader: 3`, `Replicas: 1,2,3`, `Isr: 3,2,1` | `Leader: 1`, `Replicas: 1,2,3`, `Isr: 1,2,3` |

Ключевые строки из `topic-during-final.txt` обоих прогонов:

```text
unclean.leader.election.enable=false:
Partition: 0  Leader: none  Replicas: 1,2,3  Isr: 3

unclean.leader.election.enable=true:
Partition: 0  Leader: none  Replicas: 1,2,3  Isr: 3
```

Ни в одном прогоне брокеры 4–6 не появились в списке replicas. Следовательно, автоматического переноса или переназначения партиции не произошло. Разницы в доступности между `true` и `false` не наблюдалось: unclean election не может выбрать лидера, когда ни одной живой назначенной реплики нет.

### Поведение продюсера и консюмера

| Метрика за весь прогон | `unclean...=false` | `unclean...=true` |
|---|---:|---:|
| `PRODUCER_ACK` | 47 | 36 |
| `PRODUCER_ERROR` | 44 | 54 |
| `CONSUMER_RECORD` | 47 | 36 |
| Sequence с ошибкой отправки | 18–61 | 18–71 |
| Последний ACK перед outage | 17, offset 16 | 17, offset 16 |
| Первый ACK после восстановления | 62, offset 17 | 72, offset 17 |

До отказа продюсер и консюмер работали непрерывно. После SIGKILL оба клиента остались запущены, логировали разрывы соединений и ошибки discovery/metadata. Новых `CONSUMER_RECORD` в интервале outage не было.

Пример реальной ошибки продюсера из `results/false/application.log:328` и `results/false/application.log:344`:

```text
PRODUCER_ERROR sequence=18 error=org.springframework.kafka.core.KafkaProducerException: Failed to send
Caused by: org.apache.kafka.common.errors.TimeoutException:
Expiring 11 record(s) for downtime-experiment-0:10005 ms has passed since batch creation
```

Аналогичный `TimeoutException` получен и в прогоне `true` (`results/true/application.log:332`, `results/true/application.log:348`). Ошибки начали приходить примерно через настроенные 10 секунд `delivery.timeout.ms`; приложение продолжало периодические вызовы отправки, а Kafka Producer повторял доставку в пределах этого таймаута.

Граница чтения в прогоне `false`: последнее сообщение перед outage — key 17 в `07:29:16`, следующее — key 62 в `07:30:11` (`results/false/application.log:310`, `results/false/application.log:2673`). В прогоне `true`: key 17 в `07:30:58`, затем key 72 в `07:31:58` (`results/true/application.log:310`, `results/true/application.log:2684`). После возврата брокеров консюмер продолжил чтение со следующего offset 17 без ручного вмешательства. Отправки, которые ещё не исчерпали delivery timeout к моменту восстановления, были подтверждены; сообщения с завершившейся ошибкой приложение повторно не отправляло.

В обоих прогонах множества sequence с `PRODUCER_ACK` и `CONSUMER_RECORD` совпали, дублей sequence не обнаружено. Это наблюдение относится только к выполненным прогонам с сохранившимися Docker volumes и не отменяет документированный риск потери данных при unclean election или уничтожении всех реплик.

### Итоговые выводы

1. Кластер и ZooKeeper остались доступны для административных запросов через брокеры 4–6, но партиция `downtime-experiment-0` стала недоступной в обоих вариантах.
2. `min.insync.replicas=2` не переносит реплики и не обеспечивает запись при отсутствии лидера.
3. `unclean.leader.election.enable=true` не повысило доступность: живых кандидатов в назначении `[1,2,3]` не осталось.
4. Продюсер не получал ACK во время outage, затем завершал отправки ошибкой по delivery timeout; после восстановления запись продолжилась автоматически.
5. Консюмер не завершал работу, но не мог получать новые сообщения из offline-партиции; после восстановления чтение продолжилось автоматически.
6. Для переноса партиции на брокеры 4–6 потребовалось бы отдельное переназначение реплик с копированием данных; в эксперименте оно не выполнялось.

## 8. Завершение

После сохранения артефактов удалить контейнеры и volumes эксперимента:

```bash
docker compose down -v
```
