package com.dannik.spark_nasa;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import scala.Tuple2;

import static org.apache.spark.api.java.JavaSparkContext.toSparkContext;

public class Starter {


    public static void main(String... args){
        if (args.length < 2) {
            throw new IllegalArgumentException("Invalid arguments");
        }
        new Starter().start(args[0], "hdfs://" + args[0] + ":9000" + args[1]);

    }

    public void start(String hdfsHost, String inputFilePath){
        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> inputRdd = context.textFile(inputFilePath);

        inputRdd
                .mapToPair(word -> new Tuple2<>(new NasaRowFirst(word), 1))
                .reduceByKey((a, b) -> a + b)
                .filter((tuple) -> (tuple._1.getCode() >= 500) && (tuple._1.getCode() < 600))
                .sortByKey()
                .coalesce(1, true)
                .reduceByKey((a, b) -> a + b)
                .saveAsTextFile("hdfs://" + hdfsHost + ":9000/task1");

        inputRdd
                .mapToPair(word -> new Tuple2<>(new NasaRowSecond(word), 1))
                .reduceByKey((a, b) -> a + b)
                .filter((tuple) -> tuple._2 >= 10)
                .coalesce(1, false)
                .reduceByKey((a, b) -> a + b)
                .sortByKey()
                .saveAsTextFile("hdfs://" + hdfsHost + ":9000/task2");


        SparkSession spark = SparkSession
                .builder()
                .sparkContext(toSparkContext(context))
                .getOrCreate();

        JavaRDD<NasaRow> nasarowRDD = inputRdd
                .mapToPair(word -> new Tuple2<>(new NasaRowThird(word), 1))
                .filter((tuple) -> (tuple._1.getCode() >= 400) && (tuple._1.getCode() < 600))
                .reduceByKey((a, b) -> a + b)
                .sortByKey()
                .map(tuple -> {tuple._1.setCount(tuple._2); return tuple._1;});

        MapFunction<Row, String> resultMapper = (row)->
                String.format("[%s]\tcount:\t%d\tavg:\t%d",
                        row.<String>getAs("date"),
                        row.<Integer>getAs("count"),
                        row.<Double>getAs("avg").intValue()
                ).concat(System.lineSeparator());


        Dataset<Row> nasaRowDf = spark.createDataFrame(nasarowRDD, NasaRowThird.class);

        WindowSpec ws = Window
                .orderBy(nasaRowDf.col("date").cast("timestamp").cast("long"))
                .rangeBetween(-3 * 86400, 3 * 86400);

        nasaRowDf
                .withColumn( "avg", functions.avg("count").over(ws))
                .map(resultMapper, Encoders.STRING())
                .write()
                .text("hdfs://" + hdfsHost + ":9000/task3");


    }

}
