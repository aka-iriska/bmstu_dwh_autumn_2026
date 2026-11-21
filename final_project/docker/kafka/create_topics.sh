#!/bin/sh
# Создаёт два топика проекта.
# 3 партиции - чтобы архиватор и ClickHouse честно работали с несколькими партициями;
# retention.ms=-1 - хранить всё: данные маленькие, а перечитать поток бывает нужно.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh

for topic in cinema.events cinema.profile_changes; do
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$topic" --partitions 3 --replication-factor 1 \
        --config retention.ms=-1
done

"$KT" --bootstrap-server "$BOOTSTRAP" --describe --topic 'cinema\..*'
