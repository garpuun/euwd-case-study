package org.example.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Processor<T1, T2> {

    Dataset<Row> processToBronzeLayer();
    Dataset<T1> processToSilverLayer();
    Dataset<T2> processToGoldLayer();

}
