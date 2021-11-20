package org.example.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.model.GoldVisitsModel;
import org.example.model.SilverVisitsModel;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;

@Component
public class VisitsMapper implements Serializable {

    public Dataset<SilverVisitsModel> mapToSilver(Dataset<Row> source) {
        return source.map(
                (MapFunction<Row, SilverVisitsModel>) this::mapToSilverDataset, Encoders.bean(SilverVisitsModel.class)
        );
    }

    public Dataset<GoldVisitsModel> mapToGold(Dataset<Row> source) {
        return source.map(
                (MapFunction<Row, GoldVisitsModel>) this::mapToGoldDataset, Encoders.bean(GoldVisitsModel.class)
        );
    }

    private SilverVisitsModel mapToSilverDataset(Row row) {

        SilverVisitsModel model = new SilverVisitsModel();
        model.setVisitId(Long.valueOf(row.getAs("visit_id")));
        model.setVisitorId(Long.valueOf(row.getAs("visitor_id")));
        model.setLoggedIn(Boolean.valueOf(row.getAs("logged_in")));
        model.setCountry(row.getAs("country"));
        model.setVisitDate(row.getAs("visit_date"));
        model.setVisitMonth(getMonth(row.getAs("visit_date")));
        return model;
    }

    private GoldVisitsModel mapToGoldDataset(Row row) {

        GoldVisitsModel model = new GoldVisitsModel();
        model.setVisits(row.getAs("visits"));
        model.setCountry(row.getAs("country"));
        model.setVisitDate(row.getAs("visitDate"));
        return model;
    }

    private Date getMonth(Date date) {
        LocalDate localDate = date.toLocalDate();
        return Date.valueOf(LocalDate.of(localDate.getYear(), localDate.getMonth(), 1));
    }
}
