package com.ducnguyen.sbkafka.service;

import com.ducnguyen.sbkafka.constant.ApplicationConstant;
import com.ducnguyen.sbkafka.entities.WatchLog;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    private static final int BATCH_SIZE = 10;

    private List<WatchLog> watchLogBatch = new ArrayList<>();

    private final JdbcTemplate jdbcTemplate;

    public KafkaConsumerService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @KafkaListener(topics = ApplicationConstant.TOPIC_NAME_2, groupId = ApplicationConstant.GROUP_ID)
    public void consume(String message) {
        System.out.println("ok");
        logger.info(String.format("Message received -> %s", message));
    }

    @KafkaListener(topics = ApplicationConstant.TOPIC_STREAM_INPUT, groupId = ApplicationConstant.GROUP_ID)
    public void consumeBatch(ConsumerRecord<String, String> record) {
        try {
            String message = record.value();
            ObjectMapper mapper = new ObjectMapper();
            WatchLog wLog = mapper.readValue(message, WatchLog.class);
            watchLogBatch.add(wLog);

            if (watchLogBatch.size() >= BATCH_SIZE) {
                insertBatch();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void insertBatch() {
        // batch inserts to MariaDB
        String sql = "INSERT INTO test.tv360_watch_log (item_id, item_type, number_views, watch_duration) " +
                "VALUES (?, ?, ?, ?)";

        jdbcTemplate.batchUpdate(sql, watchLogBatch, BATCH_SIZE,
                (PreparedStatement ps, WatchLog watchLog) -> {
                    ps.setLong(1, watchLog.getItemId());
                    ps.setInt(2, watchLog.getItemType());
                    ps.setDouble(3, watchLog.getNumberView());
                    ps.setDouble(4, watchLog.getDuration());
                });

        // clear the batch collection
        watchLogBatch.clear();
    }
}
