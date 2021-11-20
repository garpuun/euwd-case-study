package org.example.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.model.GoldBookingsModel;
import org.example.model.SilverBookingsModel;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class BookingsMapper implements Serializable {

    public Dataset<SilverBookingsModel> mapToSilver(Dataset<Row> source) {
        return source.map(
                (MapFunction<Row, SilverBookingsModel>) this::mapToSilverDataset, Encoders.bean(SilverBookingsModel.class)
        );
    }

    public Dataset<GoldBookingsModel> mapToGold(Dataset<Row> source) {
        return source.map(
                (MapFunction<Row, GoldBookingsModel>) this::mapToGoldDataset, Encoders.bean(GoldBookingsModel.class)
        );
    }

    private SilverBookingsModel mapToSilverDataset(Row row) {
        SilverBookingsModel model = new SilverBookingsModel();
        model.setDistributionChannelId(Integer.valueOf(row.getAs("DistributionChannelId")));
        model.setCustomerGroupId(Integer.valueOf(row.getAs("CustomerGroupId")));
        model.setOrigin(row.getAs("Origin"));
        model.setDestination(row.getAs("Destination"));
        model.setFlightNumber(Integer.valueOf(row.getAs("FlightNumber")));
        model.setFlightDate(row.getAs("FlightDate"));
//        model.setFlightDate(LocalDateTime.parse(row.getAs("FlightDate").toString(),  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        model.setBookingId(Long.valueOf(row.getAs("BookingId")));
        model.setSegmentId(Long.valueOf(row.getAs("SegmentId")));
        model.setRevenue(Double.parseDouble(row.getAs("Revenue").toString().replace(",", ".")));
        model.setBookingDate(row.getAs("BookingDate"));
        return model;
    }

    private GoldBookingsModel mapToGoldDataset(Row row) {

        GoldBookingsModel model = new GoldBookingsModel();
        model.setDistributionChannelId(row.getAs("distributionChannelId"));
        model.setCustomerGroupId(row.getAs("customerGroupId"));
        model.setRoute(row.getAs("route"));
        model.setFlightDate(row.getAs("flightDate"));
        model.setRevenue(row.getAs("revenue"));
        model.setBookingDate(row.getAs("bookingDate"));
        return model;
    }

    private Date getMonth(Date date) {
        LocalDate localDate = date.toLocalDate();
        return Date.valueOf(LocalDate.of(localDate.getYear(), localDate.getMonth(), 1));
    }
}
