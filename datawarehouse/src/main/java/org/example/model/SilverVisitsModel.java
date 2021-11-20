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
public class SilverVisitsModel implements Serializable {

    private static final long serialVersionUID = -1629587015591870068L;

    private Long visitId;
    private Long visitorId;
    private Boolean loggedIn;
    private String country;
    private Date visitDate;
    private Date visitMonth;

}
