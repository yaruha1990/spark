package org.example.movie;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * Example of dataFrame creation from file
 */
public class MovieRenderer {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().appName("Test").config("key", "value").master("local")
                .getOrCreate();

        Dataset<Row> dataframe = session.read().csv("C:\\Users\\yaruh\\IdeaProjects\\spark\\src\\main\\resources\\movie_data.csv");

        dataframe = dataframe.withColumnRenamed("_c0", "movie_name");

        dataframe = dataframe.withColumnRenamed("_c1", "rating");

        dataframe = dataframe.withColumnRenamed("_c2", "timestamp");

        dataframe.show();

        dataframe = dataframe.withColumn("rating", col("rating").cast("double"));

        dataframe.select(col("movie_name"), col("rating"), col("rating").plus(1)).show();

        dataframe.groupBy("movie_name").avg("rating").show();

        dataframe.createOrReplaceTempView("movie_table");

        session.sql("select * from movie_table").show();
    }
}
