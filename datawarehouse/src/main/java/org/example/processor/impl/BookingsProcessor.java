package org.example.processor.impl;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.example.mapper.BookingsMapper;
import org.example.model.GoldBookingsModel;
import org.example.model.SilverBookingsModel;
import org.example.processor.Processor;
import org.example.properties.PathProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

// Processor implements data processing for entity Bookings to Bronze, Silver and Gold layers

@AllArgsConstructor
@Component("bookings-processor")
@ConditionalOnProperty(prefix = "runner", value = "service-name", havingValue = "bookings-processor")
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
        Dataset<Row> validDataset = validate(dataset);
        Dataset<SilverBookingsModel> silverBookingsModelDataset = mapper.mapToSilver(validDataset).distinct();
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
                        concat(col("origin"), lit(" - "), col("destination"))
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

        return goldBookingsModelDataset;
    }

    private Dataset<Row> validate(Dataset<Row> rowDataset) {
//        check that flightDate is equal or greater than bookingDate
//        and revenue has positive value
        Dataset<Row> dataset = rowDataset
                .withColumn("hasDateError",
                        when(to_date(col("flightDate")).$less(to_date(col("bookingDate"))), true).otherwise(false))
                .withColumn("hasRevenueError",
                        when(col("revenue").cast("double").$less(lit(0.0)), true).otherwise(false));
        return dataset
                .filter(col("hasDateError").equalTo(false)
                        .and(col("hasRevenueError").equalTo(false)));
    }
}
