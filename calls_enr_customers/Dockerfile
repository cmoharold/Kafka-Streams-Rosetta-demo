FROM maven:3-openjdk-8-slim AS BUILD
COPY pom.xml /tmp/
COPY src /tmp/src/
COPY configuration /tmp/configuration/
WORKDIR /tmp/
RUN mvn package

FROM openjdk:8-alpine AS RUNTIME
RUN apk update && apk add --no-cache libc6-compat
RUN ln -s /lib64/ld-linux-x86-64.so.2 /lib/ld-linux-x86-64.so.2
ENV BOOTSTRAP_SERVERS_CONFIG=host.docker.internal:9092
ENV SCHEMA_REGISTRY_URL_CONFIG="http://host.docker.internal:8081"
WORKDIR /app/
COPY --from=BUILD /tmp/target/calls_enr_customers-*-jar-with-dependencies.jar .
COPY configuration configuration
ENTRYPOINT ["java", "-cp", "*", "com.harold.kafka.streams.calls.orange.CallsEnrichedApp"]
CMD ["configuration/dev.properties"]

