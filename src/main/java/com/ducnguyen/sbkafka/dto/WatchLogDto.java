package com.ducnguyen.sbkafka.dto;

import lombok.Data;

@Data
public class WatchLogDto {
    private Long itemId;
    private Integer itemType;
    private Integer numberView;
    private Integer duration;
}
