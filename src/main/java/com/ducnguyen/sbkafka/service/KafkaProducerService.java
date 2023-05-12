package com.ducnguyen.sbkafka.service;

import com.ducnguyen.sbkafka.constant.ApplicationConstant;
import com.ducnguyen.sbkafka.dto.WatchLogDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.openjdk.jol.info.ClassLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        logger.info(String.format("Message sent -> %s", message));
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(ApplicationConstant.TOPIC_NAME_2, message);
        System.out.println(send);
    }

    public void streamMessage(WatchLogDto watchLog) throws JsonProcessingException {
        logger.info("size of watchLog object is: ");
        logger.info(ClassLayout.parseInstance(watchLog).toPrintable());
        ObjectMapper mapper = new ObjectMapper();
        String message = mapper.writeValueAsString(watchLog);
        logger.info(String.format("Message sent -> %s", message));
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(ApplicationConstant.TOPIC_STREAM_INPUT, message);
        System.out.println(send);
    }
}
