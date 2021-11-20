package org.example.configuration;

import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfiguration {

    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession(JavaSparkContext jsc) {
        return new SparkSession.Builder()
                .appName("${app.name:viewability}")
                .master("${master.uri:local[*]}")
                .sparkContext(jsc.sc())
                .config("spark.sql.streaming.schemaInference", "true")
                .getOrCreate();
    }

    @Bean
    public org.apache.hadoop.conf.Configuration conf() {
        return new org.apache.hadoop.conf.Configuration();
    }

    @Bean
    @SneakyThrows
    public FileSystem fs(org.apache.hadoop.conf.Configuration conf) {
        return FileSystem.get(conf);
    }

    @Bean(destroyMethod = "stop")
    public JavaSparkContext sparkContext(SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
                .setMaster("local[*]")
                .set("spark.executor.memory", "1G")
                .set("spark.driver.memory", "1G")
                .set("spark.sql.shuffle.partitions", "20")
                .set("spark.local.dir", "/tmp/spark-temp")
                .setAppName("Test");
    }
}
