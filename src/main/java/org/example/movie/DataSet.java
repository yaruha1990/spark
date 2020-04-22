package org.example.movie;

import org.apache.spark.sql.*;

import java.util.Arrays;

/**
 * Example of dataFrame creation with using Encoder
 */
public class DataSet {

    public static void main(String[] args) {

        Movie movie1 = new Movie("Autograph", 4.5, 23232);
        Movie movie2 = new Movie("Mugulu Nage", 4.8, 43232);

        Encoder<Movie> movie_encoder = Encoders.bean(Movie.class);

        SparkSession session = SparkSession.builder().appName("Test").config("key", "value").master("local")
                .getOrCreate();

        Dataset<Movie> movieDataSet = session.createDataset(Arrays.asList(movie1, movie2), movie_encoder);

        movieDataSet.show();

        Dataset<Row> dataframe = session.read().csv("C:\\Users\\yaruh\\IdeaProjects\\spark\\src\\main\\resources\\movie_data.csv");

        dataframe = dataframe.withColumnRenamed("_c0", "author");

        dataframe = dataframe.withColumnRenamed("_c1", "rating");

        dataframe = dataframe.withColumnRenamed("_c2", "timestamp");

        Dataset<Movie> movie_set = dataframe.as(movie_encoder);

        movie_set.show();

    }

}