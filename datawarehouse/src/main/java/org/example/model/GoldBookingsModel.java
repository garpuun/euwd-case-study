package org.example.model;



import java.io.Serializable;
import java.sql.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GoldBookingsModel implements Serializable {

    private Integer distributionChannelId;
    private Integer customerGroupId;
    private String route;
    private Date flightDate;
    private Double revenue;
    private Date bookingDate;

}
