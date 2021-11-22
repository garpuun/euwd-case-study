package org.example.processor.impl;

import org.apache.spark.sql.SparkSession;
import org.example.properties.PathProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest( properties = {
        "runner.service-name = bookings-processor"
})
class BookingsProcessorTest {

    @Autowired
    BookingsProcessor processor;

    @Autowired
    SparkSession spark;

    @Autowired
    PathProperties pathProperties;

    @Test
    void process() {
        processor.processToBronzeLayer();
        processor.processToSilverLayer();
        processor.processToGoldLayer();
    }

//    @Test
//    void toCsv() {
//        Dataset<Row> parquet = spark.read().parquet(pathProperties.getGoldLayerBookingsPath());
//        parquet.coalesce(1).write().mode(SaveMode.Overwrite).option("header", "true").option("delimiter", ",").csv("src/test/java/resources/csv/bookings/bookings.csv");
//    }

}
