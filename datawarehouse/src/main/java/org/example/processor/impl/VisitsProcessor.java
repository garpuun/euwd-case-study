package org.example.processor.impl;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.example.mapper.VisitsMapper;
import org.example.model.GoldVisitsModel;
import org.example.model.SilverVisitsModel;
import org.example.processor.Processor;
import org.example.properties.PathProperties;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

// Processor implements data processing or entity Visits to Bronze, Silver and Gold layers


@Component
@AllArgsConstructor
public class VisitsProcessor implements Processor<SilverVisitsModel, GoldVisitsModel> {

    private final SparkSession sparkSession;
    private final PathProperties pathProperties;
    private final VisitsMapper mapper;

    @Override
    public Dataset<Row> processToBronzeLayer() {

        Dataset<Row> dataset = sparkSession
                .read()
                .option("header", true)
                .option("sep", ",")
                .csv(pathProperties.getRowVisitsPath());

        Dataset<Row> withColumn = dataset.withColumn("visit_date", to_date(col("visit_start_time"), "dd/MM/yyyy"));

        withColumn
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("visit_date")
                .parquet(pathProperties.getBronzeLayerVisitsPath());

        return withColumn;

    }

    @Override
    public Dataset<SilverVisitsModel> processToSilverLayer() {
        Dataset<Row> rowDataset = sparkSession.read().parquet(pathProperties.getBronzeLayerVisitsPath());
        Dataset<SilverVisitsModel> mappedDataset = mapper.mapToSilver(rowDataset).distinct();
        mappedDataset
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("visitDate")
                .parquet(pathProperties.getSilverLayerVisitsPath());
        return mappedDataset;
    }

    @Override
    public Dataset<GoldVisitsModel> processToGoldLayer() {
        Dataset<Row> dataset = sparkSession.read().parquet(pathProperties.getSilverLayerVisitsPath());

        Dataset<Row> aggDataset = dataset.groupBy(
                col("country"),
                col("visitDate"),
                col("visitMonth")
        ).agg(
                count("visitId").alias("visits")
        );
        Dataset<GoldVisitsModel> goldVisitsModelDataset = mapper.mapToGold(aggDataset);
        goldVisitsModelDataset
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("visitDate")
                .parquet(pathProperties.getGoldLayerVisitsPath());

        return goldVisitsModelDataset;
    }
}
