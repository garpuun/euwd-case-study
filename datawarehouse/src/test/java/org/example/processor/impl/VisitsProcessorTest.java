package org.example.processor.impl;

import org.apache.spark.sql.SparkSession;
import org.example.properties.PathProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "runner.service-name = visits-processor"
})
class VisitsProcessorTest {

    @Autowired
    VisitsProcessor processor;

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
//        Dataset<Row> parquet = spark.read().parquet(pathProperties.getGoldLayerVisitsPath());
//        parquet.coalesce(1).write().mode(SaveMode.Overwrite).option("header", "true").option("delimiter", ",").csv("src/test/java/resources/csv/visits/1");
//    }
}