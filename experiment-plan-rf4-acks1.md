# Эксперимент: отключение трёх из шести брокеров Kafka при RF=4 и `acks=1`

## 1. Цель и отличия от предыдущих прогонов

Проверить поведение Apache Kafka 3.9.0 с ZooKeeper, продюсера и консюмера при внезапном отключении брокеров `kafka-1`, `kafka-2` и `kafka-3`.

Выполняются два независимых прогона:

- `unclean.leader.election.enable=false`;
- `unclean.leader.election.enable=true`.

Неизменные параметры:

- шесть брокеров `apache/kafka:3.9.0` в одном кластере ZooKeeper;
- топик `downtime-experiment`, одна партиция;
- replication factor = 4, replicas = `[1,2,3,4]`, исходный leader = 1;
- `min.insync.replicas=2`;
- одно сообщение в секунду;
- `delivery.timeout.ms=10000`, `request.timeout.ms=3000`, `max.block.ms=5000`;
- `retries=2147483647`.

Отличие от эксперимента RF=4 с `acks=all`: продюсер использует `acks=1`. Поскольку Kafka Client не разрешает сочетание `enable.idempotence=true` и `acks=1`, идемпотентность в этих двух прогонах отключена. Это обязательное сопутствующее изменение, которое учитывается при сравнении результатов.

## 2. Проверяемая гипотеза

До прогона ожидается следующее (это гипотеза, а не результат):

1. После отказа брокеров 1–3 контроллер выберет новым лидером живую ISR-реплику 4. Значение `unclean.leader.election.enable` не должно изменить исход clean election.
2. Назначение останется `[1,2,3,4]`: автоматического переноса на брокеры 5–6 не произойдёт.
3. В отличие от `acks=all`, подтверждение лидера при `acks=1` не требует двух ISR-реплик. Ограничение `min.insync.replicas=2` при `acks=1` не должно блокировать запись на лидера 4.
4. При продолжении записи консюмер должен получать новые сообщения во время outage.
5. После возврата брокеров 1–3 реплики должны догнать лидера и ISR должен восстановиться.

Официальные источники:

- [Kafka 3.9: `acks`](https://kafka.apache.org/39/documentation.html#producerconfigs_acks);
- [Kafka 3.9: `enable.idempotence`](https://kafka.apache.org/39/documentation.html#producerconfigs_enable.idempotence);
- [Kafka 3.9: `min.insync.replicas`](https://kafka.apache.org/39/documentation.html#topicconfigs_min.insync.replicas);
- [Kafka 3.9: replication и leader election](https://kafka.apache.org/39/documentation.html#design_replication);
- [Kafka 3.9: `unclean.leader.election.enable`](https://kafka.apache.org/39/documentation.html#topicconfigs_unclean.leader.election.enable);
- [исходный код Kafka 3.9.0: `ProducerConfig`](https://github.com/apache/kafka/blob/3.9.0/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java);
- [исходный код Kafka 3.9.0: `PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection`](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/controller/PartitionStateMachine.scala).

## 3. Порядок проведения

Автоматический запуск обоих вариантов:

```bash
RESULTS_ROOT=results/rf4-acks1 \
REPLICA_ASSIGNMENT=1:2:3:4 \
KAFKA_PRODUCER_ACKS=1 \
KAFKA_PRODUCER_IDEMPOTENCE=false \
./scripts/run-experiment.sh
```

Для каждого варианта сценарий:

1. Пересоздаёт стенд с чистыми Docker volumes и проверяет регистрацию шести брокеров в ZooKeeper.
2. Сохраняет назначение реплик, `acks` и состояние идемпотентности в `experiment-config.txt`.
3. Создаёт топик с явным `--replica-assignment 1:2:3:4`, `min.insync.replicas=2` и проверяемым значением `unclean.leader.election.enable`.
4. Сохраняет исходные leader/replicas/ISR и конфигурацию топика.
5. Запускает приложение, ждёт первый `PRODUCER_ACK` и собирает 15 секунд нормальной работы.
6. Фиксирует UTC-время и выполняет `docker kill kafka-1 kafka-2 kafka-3` (SIGKILL).
7. Через 5 секунд и ещё через 45 секунд сохраняет описание топика через выжившие брокеры 4–6.
8. Возвращает брокеры 1–3, ждёт 20 секунд и снова сохраняет описание топика.
9. Сохраняет полные логи всех брокеров, ZooKeeper и приложения, а также отдельные события продюсера и консюмера.

## 4. Критерии анализа

- **Выбор лидера:** сравнить `Leader` и `Isr` до отказа, во время него и после восстановления.
- **Отсутствие переназначения:** `Replicas` должны оставаться `1,2,3,4`; брокеры 5–6 не должны автоматически добавляться.
- **Эффективная конфигурация клиента:** проверить `experiment-config.txt` и строки `ProducerConfig` в `application.log`, включая `acks = 1` и `enable.idempotence = false`.
- **Запись:** сопоставить `PRODUCER_ACK` и `PRODUCER_ERROR` с временем отказа. Отдельно проверить наличие или отсутствие `NOT_ENOUGH_REPLICAS`.
- **Чтение:** сопоставить `CONSUMER_RECORD`, sequence и offsets с ACK продюсера, включая записи во время outage.
- **Влияние unclean election:** сравнить два прогона; при доступной ISR-реплике флаг не должен быть причиной различий.
- **Целостность наблюдаемых данных:** сравнить множества sequence с ACK и реально прочитанные sequence, выявить дубли, записи без ACK и чтение сообщений с завершившейся ошибкой отправки. Сам по себе `acks=1` не гарантирует сохранность при последующем unclean election, но данный сценарий с живой ISR-репликой не проверяет такую потерю.

## 5. Артефакты

Новые результаты сохраняются отдельно:

- `results/rf4-acks1/false/`;
- `results/rf4-acks1/true/`.

Предыдущие результаты RF=3 и RF=4 с `acks=all` не перезаписываются.

## 6. Фактические результаты

Оба варианта выполнены на стенде из этого каталога. Фактическая конфигурация клиента подтверждена строками `acks = 1` и `enable.idempotence = false` в `results/rf4-acks1/false/application.log:143,156` и `results/rf4-acks1/true/application.log:143,156`. Брокеры использовали Kafka 3.9.0; приложение — Kafka Client 4.2.2 из зависимостей Spring Boot 4.1.2.

### Состояние партиции

| Наблюдение | `unclean...=false` | `unclean...=true` |
|---|---|---|
| До отказа | `Leader: 1`, `Replicas: 1,2,3,4`, `Isr: 1,2,3,4` | То же |
| Через 5 секунд после SIGKILL | `Leader: 1`, `Isr: 1,2,3,4` — metadata ещё не обновлены | То же |
| Конец outage | `Leader: 4`, `Replicas: 1,2,3,4`, `Isr: 4` | То же |
| После возврата брокеров 1–3 | `Leader: 4`, `Isr: 4,2,1,3` | `Leader: 4`, `Isr: 4,3,1,2` |

Источники: `topic-before.txt`, `topic-during-5s.txt`, `topic-during-final.txt`, `topic-after.txt` в соответствующих каталогах результатов.

В обоих прогонах контроллер выбрал живую ISR-реплику 4 — это clean leader election. Разрешение unclean election не изменило исход. Назначение осталось `[1,2,3,4]`: автоматического переноса партиции или добавления реплик на брокеры 5–6 не произошло.

### Поведение продюсера

| Метрика за весь прогон | `unclean...=false` | `unclean...=true` |
|---|---:|---:|
| `PRODUCER_ACK` | 79 | 58 |
| `PRODUCER_ERROR` | 22 | 33 |
| Sequence с `TimeoutException` | 18–39 | 18–50 |
| Последний ACK перед outage | 17, offset 16 | 17, offset 16 |
| Первый ACK после выбора нового лидера | 40, offset 17 | 51, offset 17 |
| Последний ACK в сохранённых логах | 101, offset 78 | 91, offset 57 |

До обнаружения нового лидера продюсер получал `TimeoutException` по истечении `delivery.timeout.ms=10000`: первая ошибка находится в `results/rf4-acks1/false/application.log:323`, её причина — в строке 339; для второго прогона — `results/rf4-acks1/true/application.log:320,336`.

После выбора лидера 4 запись возобновилась ещё до возврата брокеров 1–3: sequence 40 в `results/rf4-acks1/false/application.log:814` и sequence 51 в `results/rf4-acks1/true/application.log:1093`. Далее ACK поступали каждую секунду при `Isr: 4`.

В отличие от прогона с `acks=all`, ошибок `NOT_ENOUGH_REPLICAS` / `NotEnoughReplicasException` нет ни в клиентских логах, ни в логах брокеров. Это соответствует исходному коду Kafka 3.9.0: проверка недостаточного ISR перед append в `Partition.appendRecordsToLeader` выполняется только при `requiredAcks == -1`. Поэтому при `acks=1` лидер подтверждает запись, несмотря на `min.insync.replicas=2` и ISR из одной реплики.

Различие общего числа ACK и ошибок между вариантами объясняется временем обнаружения отказа и выбора лидера, а не значением unclean-флага.

### Поведение консюмера и high watermark

| Метрика за весь прогон | `unclean...=false` | `unclean...=true` |
|---|---:|---:|
| `CONSUMER_RECORD` | 79 | 58 |
| Дубли по sequence | 0 | 0 |
| Прочитанные sequence без ACK | 0 | 0 |
| ACK, не прочитанные к концу прогона | 0 | 0 |
| Sequence с `PRODUCER_ERROR`, которые затем прочитаны | 0 | 0 |

Гипотеза о немедленном чтении новых записей во время outage **не подтвердилась**. Продюсер получал ACK, но консюмер не выводил новые `CONSUMER_RECORD` до возврата брокеров 1–3. После восстановления ISR консюмер прочитал накопленные записи с offset 17: sequence 40–81 в `results/rf4-acks1/false/application.log:856–897` и sequence 51–70 в `results/rf4-acks1/true/application.log:1113–1132`. Затем чтение новых сообщений продолжилось обычным образом.

Причина подтверждается исходным кодом Kafka 3.9.0:

- `Partition.maybeIncrementLeaderHW` не увеличивает high watermark, пока `isUnderMinIsr` истинно, то есть ISR меньше `min.insync.replicas`;
- для обычного консюмера с `isolation.level=read_uncommitted` верхней границей чтения является high watermark, а не log end offset;
- следовательно, `acks=1` позволяет append и ACK на лидере, но не делает эти записи немедленно доступными консюмеру, пока `Isr: 4` и `min.insync.replicas=2`.

Источник: [Kafka 3.9.0, `Partition.scala`](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/cluster/Partition.scala), методы `maybeIncrementLeaderHW`, `appendRecordsToLeader` и `fetchOffsetForTimestamp`.

После возврата хотя бы одной реплики в ISR high watermark смог продвинуться, и консюмер дочитал все подтверждённые записи. Логи брокера 4 показывают восстановление ISR: `results/rf4-acks1/false/kafka-4.log:879,894,897` и `results/rf4-acks1/true/kafka-4.log:898,907,914`.

### Итоговые выводы

1. При RF=4 отказ брокеров 1–3 приводит к clean election лидера 4 в обоих вариантах; `unclean.leader.election.enable` не влияет на этот исход.
2. Автоматического переназначения реплик на брокеры 5–6 нет.
3. В отличие от `acks=all`, запись с `acks=1` после выбора лидера 4 возобновляется при `Isr: 4`. Проверка `min.insync.replicas=2` не блокирует produce с `acks=1`.
4. До выбора нового лидера продюсер всё равно получает ошибки по `delivery.timeout.ms`; `acks=1` не устраняет интервал недоступности.
5. ACK при `acks=1` не означает немедленную видимость сообщения консюмеру в этом сценарии. Поскольку ISR меньше `min.insync.replicas`, high watermark не растёт, и новые записи читаются только после восстановления ISR.
6. В обоих фактических прогонах все ACK-сообщения в конечном итоге прочитаны ровно один раз; сообщения с `TimeoutException` не прочитаны. Это наблюдение не является общей гарантией сохранности `acks=1`: при другом отказе и unclean election подтверждённая только лидером запись может быть потеряна.
