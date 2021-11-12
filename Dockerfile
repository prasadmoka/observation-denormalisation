FROM flink:1.14.0-java11
COPY ${JAR_FILE} /opt/flink/lib
USER flink