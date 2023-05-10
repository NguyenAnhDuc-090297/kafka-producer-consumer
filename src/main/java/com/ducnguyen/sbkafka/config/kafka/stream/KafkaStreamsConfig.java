package com.ducnguyen.sbkafka.config.kafka.stream;

import com.ducnguyen.sbkafka.constant.ApplicationConstant;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;

@Component
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

//    @Bean
//    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
//        KStream<String, String> inputTopicStream = streamsBuilder.stream(ApplicationConstant.TOPIC_STREAM_INPUT);
//
//        inputTopicStream
//                .groupBy((key, value) -> key)
//                .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
//                .reduce((oldValue, newValue) -> oldValue)
//                .toStream();
//        inputTopicStream.to(ApplicationConstant.TOPIC_STREAM_OUTPUT);
//
//        return inputTopicStream;
//    }

//    @Bean
//    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
//        KStream<String, String> inputTopicStream = streamsBuilder.stream(ApplicationConstant.TOPIC_STREAM_INPUT);
//
//        inputTopicStream
//                .groupBy((key, value) -> key)
//                .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
//                .reduce((oldValue, newValue) -> oldValue)
//                .toStream();
//
//        inputTopicStream
//                .groupByKey()
//                .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
//                .aggregate(
//                        () -> "", // initial value
//                        (key, value, aggregate) -> value, // update function
//                        Materialized.with(Serdes.String(), Serdes.String())
//                )
//                .toStream()
//                .foreach((windowedKey, value) -> {
//                    String outputKey = windowedKey.key(); // Extracting the key from the windowed key
//                    System.out.println("outputKey: " + outputKey);
//                    // Sending the output to the output topic with the extracted key
//                    streamsBuilder.stream(outputKey).to(ApplicationConstant.TOPIC_STREAM_OUTPUT);
//                });
//
//        return inputTopicStream;
//    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> inputTopicStream = streamsBuilder.stream(ApplicationConstant.TOPIC_STREAM_INPUT);

        inputTopicStream
                .filter((key, value) -> key != null) // Filter out records with null keys
                .groupBy((key, value) -> key)
                .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
                .reduce((oldValue, newValue) -> oldValue)
                .toStream();

        inputTopicStream
                .filter((key, value) -> key != null) // Filter out records with null keys
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
                .aggregate(
                        () -> "", // initial value
                        (key, value, aggregate) -> value, // update function
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .foreach((windowedKey, value) -> {

                    String outputKey = windowedKey.key(); // Extracting the key from the windowed key
                    // Sending the output to the output topic with the extracted key
                    streamsBuilder.stream(outputKey).to(ApplicationConstant.TOPIC_STREAM_OUTPUT);
                });

        return inputTopicStream;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "streams-app");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return new KafkaStreamsConfiguration(props);
    }
}
