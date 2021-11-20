package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SilverBookingsModel implements Serializable {

    private Integer distributionChannelId;
    private Integer customerGroupId;
    private String origin;
    private String destination;
    private Integer flightNumber;
    private String flightDate;
    private Long bookingId;
    private Long segmentId;
    private Double revenue;
    private Date bookingDate;
}
