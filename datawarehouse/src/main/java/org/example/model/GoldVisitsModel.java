package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GoldVisitsModel implements Serializable {

    private static final long serialVersionUID = -1629587015591870068L;

    private Long visits;
    private String country;
    private Date visitDate;

}
