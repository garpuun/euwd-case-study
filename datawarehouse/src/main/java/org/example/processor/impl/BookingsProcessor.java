package org.example.processor.impl;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.example.mapper.BookingsMapper;
import org.example.model.GoldBookingsModel;
import org.example.model.SilverBookingsModel;
import org.example.processor.Processor;
import org.example.properties.PathProperties;
import org.springframework.stereotype.Component;

import java.util.Locale;

import static org.apache.spark.sql.functions.*;

// Processor implements data processing or entity Bookings to Bronze, Silver and Gold layers

@Component
@AllArgsConstructor
public class BookingsProcessor implements Processor<SilverBookingsModel, GoldBookingsModel> {

    private final SparkSession spark;
    private final PathProperties pathProperties;
    private final BookingsMapper mapper;

    @Override
    public Dataset<Row> processToBronzeLayer() {
        Dataset<Row> dataset = spark
                .read()
                .option("header", true)
                .option("sep", ",")
                .csv(pathProperties.getRowBookingsPath());

        Dataset<Row> bookings = dataset
                .withColumn("date", to_date(col("BookingDate"), "dd/MM/yyyy"))
                .drop(col("BookingDate"))
                .withColumnRenamed("date", "BookingDate");

        bookings.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("BookingDate")
                .parquet(pathProperties.getBronzeLayerBookingsPath());
        bookings.show();
        return bookings;

    }

    @Override
    public Dataset<SilverBookingsModel> processToSilverLayer() {
        Dataset<Row> dataset = spark.read().parquet(pathProperties.getBronzeLayerBookingsPath());
        Dataset<SilverBookingsModel> silverBookingsModelDataset = mapper.mapToSilver(dataset).distinct();
        silverBookingsModelDataset
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("bookingDate")
                .parquet(pathProperties.getSilverLayerBookingsPath());
        return silverBookingsModelDataset;
    }

    @Override
    public Dataset<GoldBookingsModel> processToGoldLayer() {
        Dataset<Row> dataset = spark.read().parquet(pathProperties.getSilverLayerBookingsPath());
        Dataset<Row> aggDataset = dataset
                .withColumn(
                        "route",
                        concat(col("origin"),lit(" - "), col("destination"))
                )
                .groupBy(
                        col("distributionChannelId"),
                        col("customerGroupId"),
                        col("route"),
                        to_date(col("flightDate")).alias("flightDate"),
                        col("bookingDate")
                ).agg(
                        round(sum(col("revenue")), 2).alias("revenue")
                );
        Dataset<GoldBookingsModel> goldBookingsModelDataset = mapper.mapToGold(aggDataset);
        goldBookingsModelDataset
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("bookingDate")
                .parquet(pathProperties.getGoldLayerBookingsPath());

        return null;
    }
}
