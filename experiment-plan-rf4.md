# Эксперимент: отключение трёх из шести брокеров Kafka при RF=4

## 1. Цель и отличие от эксперимента RF=3

Проверить поведение Apache Kafka 3.9.0 с ZooKeeper, продюсера и консюмера при внезапном отключении брокеров `kafka-1`, `kafka-2` и `kafka-3`.

От исходного эксперимента отличается только фактор репликации: теперь replication factor = 4, replicas = `[1,2,3,4]`. Поэтому после отказа брокеров 1–3 остаётся живая назначенная реплика на брокере 4. В эксперименте RF=3 живых назначенных реплик не оставалось.

Проверяются два независимых прогона:

- `unclean.leader.election.enable=false`;
- `unclean.leader.election.enable=true`.

Неизменные параметры:

- шесть брокеров `apache/kafka:3.9.0` в одном кластере ZooKeeper;
- топик `downtime-experiment`, одна партиция;
- replication factor = 4;
- `min.insync.replicas=2`;
- исходный leader = 1, replicas = `[1,2,3,4]`;
- одно сообщение в секунду, `acks=all`, включённая идемпотентность продюсера;
- `delivery.timeout.ms=10000`, `request.timeout.ms=3000`, `max.block.ms=5000`.

## 2. Проверяемая гипотеза

До прогона ожидается следующее (это гипотеза, а не результат):

1. Если реплика 4 находится в ISR на момент отказа, контроллер сможет выбрать её новым лидером с помощью clean leader election. Значение `unclean.leader.election.enable` в этом случае не должно менять исход выбора лидера.
2. Назначение реплик должно остаться `[1,2,3,4]`: выбор лидера не является автоматическим переносом партиции на брокеры 5–6.
3. Пока ISR состоит только из реплики 4, `min.insync.replicas=2` не выполняется. Запись с `acks=all` должна быть недоступна, несмотря на наличие лидера.
4. Консюмер должен сохранить возможность читать уже записанные данные с лидера 4. Отсутствие новых записей нужно отдельно отличать от недоступности чтения.
5. После возврата брокеров 1–3 и восстановления ISR запись должна возобновиться.

Официальные источники:

- [Kafka 3.9: replication и leader election](https://kafka.apache.org/39/documentation.html#design_replication);
- [Kafka 3.9: `min.insync.replicas`](https://kafka.apache.org/39/documentation.html#topicconfigs_min.insync.replicas);
- [Kafka 3.9: `unclean.leader.election.enable`](https://kafka.apache.org/39/documentation.html#topicconfigs_unclean.leader.election.enable);
- [исходный код Kafka 3.9.0: `PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection`](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/controller/PartitionStateMachine.scala);
- [Kafka 3.9.0: `ProducerConfig`](https://github.com/apache/kafka/blob/3.9.0/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java).

## 3. Порядок проведения

Автоматический запуск обоих вариантов:

```bash
RESULTS_ROOT=results/rf4 REPLICA_ASSIGNMENT=1:2:3:4 ./scripts/run-experiment.sh
```

Для каждого варианта сценарий:

1. Пересоздаёт стенд с чистыми Docker volumes и проверяет регистрацию шести брокеров в ZooKeeper.
2. Создаёт топик с явным `--replica-assignment 1:2:3:4`, `min.insync.replicas=2` и проверяемым значением `unclean.leader.election.enable`.
3. Сохраняет исходные leader/replicas/ISR и конфигурацию топика.
4. Запускает приложение, ждёт первый `PRODUCER_ACK` и собирает 15 секунд нормальной работы.
5. Фиксирует UTC-время и выполняет `docker kill kafka-1 kafka-2 kafka-3` (SIGKILL).
6. Через 5 секунд и ещё через 45 секунд сохраняет описание топика через выжившие брокеры 4–6.
7. Возвращает брокеры 1–3, ждёт 20 секунд и снова сохраняет описание топика.
8. Сохраняет полные логи всех брокеров, ZooKeeper и приложения, а также отдельные события продюсера и консюмера.

## 4. Критерии анализа

- **Выбор лидера:** сравнить `Leader` и `Isr` до отказа, во время него и после восстановления. Проверить, что лидером стала назначенная реплика 4.
- **Отсутствие переназначения:** `Replicas` должны оставаться `1,2,3,4`; брокеры 5–6 не должны автоматически добавляться.
- **Запись:** сопоставить `PRODUCER_ACK` и `PRODUCER_ERROR` с временем отказа. Установить фактические причины ошибок по `Caused by`, включая возможные `NotEnoughReplicasException`, `NotEnoughReplicasAfterAppendException` и `TimeoutException`.
- **Чтение:** сопоставить `CONSUMER_RECORD`, offsets и client-side fetch/metadata ошибки. Проверить доступность старых данных, а не делать вывод только по отсутствию новых сообщений.
- **Влияние unclean election:** сравнить два прогона. Наличие живой ISR-реплики позволяет ожидать clean election в обоих вариантах; флаг не следует считать причиной различий без подтверждения логами.
- **ACK и фактическая запись:** истечение `delivery.timeout.ms` само по себе не доказывает, что запись не попала в лог. Сравнить множества sequence с ACK и sequence, реально прочитанные консюмером; отдельно выявить дубли и записи без ACK.

## 5. Артефакты

Результаты сохраняются отдельно от RF=3:

- `results/rf4/false/`;
- `results/rf4/true/`.

В каждом каталоге должны быть `topic-before.txt`, `topic-during-5s.txt`, `topic-during-final.txt`, `topic-after.txt`, `topic-config.txt`, `timestamps.txt`, `containers-during.txt`, полные логи брокеров и ZooKeeper, `application.log`, `producer-events.log` и `consumer-events.log`.

Исходные результаты RF=3 в `results/false/` и `results/true/` не перезаписываются.

## 6. Фактические результаты

Оба варианта выполнены на стенде из этого каталога. До отказа ZooKeeper показывал все broker.id `[1, 2, 3, 4, 5, 6]`, а топик имел `Leader: 1`, `Replicas: 1,2,3,4`, `Isr: 1,2,3,4` (`registered-brokers.txt`, `topic-before.txt`).

### Состояние партиции

| Наблюдение | `unclean...=false` | `unclean...=true` |
|---|---|---|
| До отказа | `Leader: 1`, `Isr: 1,2,3,4` | `Leader: 1`, `Isr: 1,2,3,4` |
| Через 5 секунд после SIGKILL | `Leader: 1`, `Isr: 1,2,3,4` — отказ ещё не отражён в metadata | То же |
| Через 52 секунды после SIGKILL | `Leader: 4`, `Replicas: 1,2,3,4`, `Isr: 4` | `Leader: 4`, `Replicas: 1,2,3,4`, `Isr: 4` |
| После возврата брокеров 1–3 | `Leader: 4`, `Isr: 4,3,1,2` | `Leader: 4`, `Isr: 4,1,2,3` |

В обоих прогонах контроллер выбрал лидером живую ISR-реплику 4. Это clean leader election, поэтому разрешение unclean election не изменило результат. Назначение осталось `Replicas: 1,2,3,4`: автоматического переноса или добавления реплик на брокеры 5–6 не произошло. Выбор нового лидера среди уже назначенных реплик не является переназначением партиции.

В отличие от RF=3, партиция не стала offline: в RF=3 не оставалось живых назначенных реплик и наблюдался `Leader: none`, а здесь осталась назначенная ISR-реплика 4.

### Поведение продюсера

| Метрика за весь прогон | `unclean...=false` | `unclean...=true` |
|---|---:|---:|
| `PRODUCER_ACK` | 41 | 47 |
| `PRODUCER_ERROR` | 60 | 53 |
| Sequence с ошибкой отправки | 18–77 | 18–70 |
| Последний ACK перед outage | 17, offset 16 | 17, offset 16 |
| Первый ACK после восстановления | 78, offset 17 | 71, offset 17 |

Наличие лидера 4 не обеспечило запись. В ISR осталась одна реплика, что меньше `min.insync.replicas=2`. Брокер 4 отклонял produce-запросы, например (`results/rf4/false/kafka-4.log:846`, `results/rf4/true/kafka-4.log:848`):

```text
org.apache.kafka.common.errors.NotEnoughReplicasException:
The size of the current ISR Set(4) is insufficient to satisfy
the min.isr requirement of 2 for partition downtime-experiment-0
```

В клиентских логах видны повторные produce-запросы с `Error: NOT_ENOUGH_REPLICAS` (`results/rf4/false/application.log:1794`). Настроенный `delivery.timeout.ms=10000` ограничивал время этих повторов. Колбеки приложения получали `TimeoutException` примерно через 10 секунд, например (`results/rf4/false/application.log:326`, `results/rf4/false/application.log:342`):

```text
PRODUCER_ERROR sequence=18 error=org.springframework.kafka.core.KafkaProducerException: Failed to send
Caused by: org.apache.kafka.common.errors.TimeoutException:
Expiring 11 record(s) for downtime-experiment-0:10002 ms has passed since batch creation
```

В прогоне `true` первая ошибка sequence 18 зафиксирована в `results/rf4/true/application.log:335`, её `TimeoutException` — в `results/rf4/true/application.log:351`. Приложение продолжало периодические отправки; сообщения с уже завершившейся ошибкой само не повторяло.

После возврата брокеров ISR восстановился, ещё не истёкшие отправки получили ACK и новые сообщения снова стали подтверждаться. Первый ACK после outage: sequence 78, offset 17 в `results/rf4/false/application.log:1802`; sequence 71, offset 17 в `results/rf4/true/application.log:1605`.

### Поведение консюмера

| Метрика за весь прогон | `unclean...=false` | `unclean...=true` |
|---|---:|---:|
| `CONSUMER_RECORD` | 41 | 47 |
| Дубли по sequence | 0 | 0 |
| Прочитанные sequence без ACK | 0 | 0 |
| Sequence с `PRODUCER_ERROR`, которые затем прочитаны | 0 | 0 |

После SIGKILL процесс консюмера остался запущен, логировал разрывы соединений, отмену fetch к прежнему лидеру и повторный поиск group coordinator. Новых `CONSUMER_RECORD` во время outage не было: успешных новых записей тоже не было. Последнее сообщение перед outage в обоих прогонах — key 17, offset 16.

После восстановления консюмер продолжил со следующего offset 17 без ручного вмешательства: key 78 в `results/rf4/false/application.log:1801`, key 71 в `results/rf4/true/application.log:1606`. Множества sequence с ACK и прочитанных sequence совпали в каждом прогоне; дублей не обнаружено. Сообщения, завершившиеся `PRODUCER_ERROR`, в лог топика по результатам чтения не попали.

Этот сценарий проверяет поведение работающего консюмера на текущей позиции. Отдельного rewind и повторного чтения старых записей во время outage не выполнялось, поэтому по этим логам нельзя самостоятельно утверждать, что проверена доступность всего исторического диапазона для нового консюмера.

### Итоговые выводы

1. При RF=4 и replicas `[1,2,3,4]` отказ брокеров 1–3 не делает партицию offline: лидером становится брокер 4 в обоих вариантах.
2. `unclean.leader.election.enable=true` не даёт преимущества в этом сценарии, поскольку доступна живая ISR-реплика и используется clean election.
3. Автоматического переноса реплик на брокеры 5–6 нет; изменяется только лидер среди существующего назначения.
4. Несмотря на доступный лидер и работающий кластер, запись с `acks=all` останавливается при `Isr: 4`, потому что одна ISR-реплика не удовлетворяет `min.insync.replicas=2`.
5. Консюмер остаётся работоспособным как процесс и сохраняет позицию, но новых сообщений во время outage не получает. После восстановления ISR продюсер и консюмер продолжают работу с offset 17.
6. Число ACK и ошибок отличается между прогонами из-за таймингов обнаружения отказа, восстановления и истечения delivery timeout; само по себе это не эффект unclean-флага.
