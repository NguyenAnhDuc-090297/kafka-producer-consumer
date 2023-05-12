package com.ducnguyen.sbkafka.repository;

import com.ducnguyen.sbkafka.entities.WatchLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface WatchLogRepository extends JpaRepository<WatchLog, Long> {
}
