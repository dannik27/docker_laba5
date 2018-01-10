package com.dannik.spark_nasa;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import static org.apache.spark.api.java.JavaSparkContext.toSparkContext;

import java.io.File;
import java.util.Arrays;


public class Starter {


    private static final Logger LOGGER = LoggerFactory.getLogger(Starter.class);



    public static void main(String... args){

        deleteDirectory(new File("result"));
        //new Starter().task1("access_log_Aug95");
        //new Starter().task3old("part-00000");


        //new Starter().task3("access_log_Aug95");

        new Starter().start("sd", "access_log_Aug95");
    }

    public void start(String hdfsHost, String inputFilePath){
        SparkConf conf = new SparkConf()
                .setAppName("nasa")
                .setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> inputRdd = context.textFile(inputFilePath);

        inputRdd
                .mapToPair(word -> new Tuple2<>(new NasaRowFirst(word), 1))
                .reduceByKey((a, b) -> a + b)
                .filter((tuple) -> (tuple._1.getCode() >= 500) && (tuple._1.getCode() < 600))
                .sortByKey()
                .coalesce(1, true)
                .reduceByKey((a, b) -> a + b)
                .saveAsTextFile("result/task1");

        inputRdd
                .mapToPair(word -> new Tuple2<>(new NasaRowSecond(word), 1))
                .reduceByKey((a, b) -> a + b)
                .filter((tuple) -> tuple._2 >= 10)
                .coalesce(1, false)
                .reduceByKey((a, b) -> a + b)
                .sortByKey()
                .saveAsTextFile("result/task2");


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
                .text("result/task3");


    }


    public void task1(String inputFilePath) {

        SparkConf conf = new SparkConf()
                .setAppName("nasa")
                .setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);


        context.textFile(inputFilePath)
                .mapToPair(word -> new Tuple2<>(new NasaRowFirst(word), 1))
                .reduceByKey((a, b) -> a + b)
                .filter((tuple) -> (tuple._1.getCode() >= 500) && (tuple._1.getCode() < 600))
                .sortByKey()
                .coalesce(1, true)
                .reduceByKey((a, b) -> a + b)
                .saveAsTextFile("result/task1");

    }

    public void task2(String inputFilePath) {

        SparkConf conf = new SparkConf()
                .setAppName("nasa")
                .setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        context.textFile(inputFilePath)
                .mapToPair(word -> new Tuple2<>(new NasaRowSecond(word), 1))
                .reduceByKey((a, b) -> a + b)
                .filter((tuple) -> tuple._2 >= 10)
                .coalesce(1, false)
                .reduceByKey((a, b) -> a + b)
                .sortByKey()
                .saveAsTextFile("result/task2");

    }



    public void task3(String inputFilePath) {


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();

        JavaRDD<NasaRowThird> nasarowRDD = spark.read()
                .textFile(inputFilePath)
                .javaRDD()
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
                .text("result/task3");
    }



    private static void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                File f = new File(dir, children[i]);
                deleteDirectory(f);
            }
            dir.delete();
        } else dir.delete();
    }

}
