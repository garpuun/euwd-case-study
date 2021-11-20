package org.example.properties;

import lombok.Data;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
public class PathProperties {

//    it would be better to use tools for external configuration like Consul or Vault
    private String rowBookingsPath = "src/main/resources/bookings";
    private String bronzeLayerBookingsPath = "src/test/java/resources/datalake/bronze/bookings";
    private String silverLayerBookingsPath = "src/test/java/resources/datalake/silver/bookings";
    private String goldLayerBookingsPath = "src/test/java/resources/datalake/gold/bookings";
    private String rowVisitsPath = "src/main/resources/visits";
    private String bronzeLayerVisitsPath = "src/test/java/resources/datalake/bronze/visits";
    private String silverLayerVisitsPath = "src/test/java/resources/datalake/silver/visits";
    private String goldLayerVisitsPath = "src/test/java/resources/datalake/gold/visits";
}
