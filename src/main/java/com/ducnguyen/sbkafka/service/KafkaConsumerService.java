package com.ducnguyen.sbkafka.service;

import com.ducnguyen.sbkafka.constant.ApplicationConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @KafkaListener(topics = ApplicationConstant.TOPIC_NAME_2, groupId = ApplicationConstant.GROUP_ID)
    public void consume(String message) {
        System.out.println("ok");
        logger.info(String.format("Message received -> %s", message));
    }
}
