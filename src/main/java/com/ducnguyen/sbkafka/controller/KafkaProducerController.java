package com.ducnguyen.sbkafka.controller;

import com.ducnguyen.sbkafka.dto.WatchLogDto;
import com.ducnguyen.sbkafka.service.KafkaProducerService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/kafka")
public class KafkaProducerController {

    private final KafkaProducerService kafkaProducerService;


    public KafkaProducerController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @GetMapping("/publish")
    public ResponseEntity<String> sendMessage(@RequestParam String message) {
        kafkaProducerService.sendMessage(message);
        return ResponseEntity.ok("Message sent to kafka topic");
    }

    @PostMapping("/stream")
    public ResponseEntity<String> streamMessage(@RequestBody WatchLogDto watchLogDto) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String message = mapper.writeValueAsString(watchLogDto);
        kafkaProducerService.streamMessage(watchLogDto.getVideoId().toString(), message);
        return ResponseEntity.ok("Message sent to kafka topic");
    }
}
