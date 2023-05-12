package com.ducnguyen.sbkafka.entities;

import lombok.*;

import javax.persistence.*;

@Getter
@Setter
@RequiredArgsConstructor
@Entity
@Table(name = "tv360_watch_log", schema = "test")
public class WatchLog {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Long id;

    @Column(name = "item_id")
    private Long itemId;

    @Column(name = "item_type")
    private Integer itemType;

    @Column(name = "number_views")
    private Double numberView;

    @Column(name = "watch_duration")
    private Double duration;
}
