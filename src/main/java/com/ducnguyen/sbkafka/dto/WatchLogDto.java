package com.ducnguyen.sbkafka.dto;

import lombok.Data;

@Data
public class WatchLogDto {
    private Long videoId;
    private Integer numberView;
    private Integer duration;
}
