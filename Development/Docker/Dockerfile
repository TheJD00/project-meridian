FROM spark:3.5.9-java17 AS compiler
USER root
WORKDIR /build
COPY ["Data Ingestion/Streaming/MeridianStreaming.scala", "./MeridianStreaming.scala"]
COPY ["Data Ingestion/Batch/src/main/scala/meridian/BatchJob.scala", "./BatchJob.scala"]
RUN mkdir classes && \
    java -cp '/opt/spark/jars/*' scala.tools.nsc.Main -usejavacp -d classes MeridianStreaming.scala BatchJob.scala && \
    jar cf meridian.jar -C classes .

FROM spark:3.5.9-java17 AS spark-jobs
USER root
WORKDIR /opt/meridian
COPY --from=compiler /build/meridian.jar ./meridian.jar
COPY Datasets/batch/raw ./batch-input
COPY Datasets/streaming/micro_batches ./stream-input
COPY Development/Config/meridian-spark-defaults.conf ./
USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit"]

FROM apache/kafka:4.1.2 AS kafka-runtime
WORKDIR /opt/meridian

FROM kafka-runtime AS replay-files
COPY Datasets/streaming/tracking_events_kafka_keyed.txt ./tracking-events.txt
COPY Development/Scripts/ReplayMeridianKafka.sh ./replay.sh

FROM kafka-runtime AS kafka-producer
COPY --from=replay-files /opt/meridian/ /opt/meridian/
USER 1000
ENTRYPOINT ["/bin/bash", "/opt/meridian/replay.sh"]
