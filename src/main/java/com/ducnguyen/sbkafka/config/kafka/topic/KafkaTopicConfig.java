package com.ducnguyen.sbkafka.config.kafka.topic;

import com.ducnguyen.sbkafka.constant.ApplicationConstant;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic topic1() {
        return TopicBuilder.name(ApplicationConstant.TOPIC_NAME_2).build();
    }

    @Bean
    public NewTopic topic2() {
        return TopicBuilder.name(ApplicationConstant.TOPIC_NAME_2).build();
    }
}
