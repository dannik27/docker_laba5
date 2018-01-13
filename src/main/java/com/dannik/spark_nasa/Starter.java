package com.dannik.spark_nasa;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.apache.spark.api.java.JavaSparkContext.toSparkContext;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.window;


public class Starter {

    static DateTimeFormatter oldFormat = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.UK);
    static DateTimeFormatter newFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");



    public static void main(String... args){
        if (args.length < 2) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        new Starter().start("hdfs://" + args[0] + ":9000" + args[1], "hdfs://" + args[0] + ":9000" );
        //new Starter().start("access_log_Aug95", "result");
    }




    public void start(String inputFile, String outputFolder){

        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext(conf);
        SparkSession spark = SparkSession
                .builder()
                .sparkContext(toSparkContext(context))
                .getOrCreate();


        spark
                .read().textFile(inputFile)
                .map(pojoMapper, Encoders.bean(LogRow.class))
                .filter("code >= 500").filter("code < 600")
                .groupBy("method", "url").agg(count("method").as("count"))
                .write().json(outputFolder + "/task1");


        spark
                .read().textFile(inputFile)
                .map(pojoMapper, Encoders.bean(LogRow.class))
                .groupBy("date", "method", "code").agg(count("method").as("count"))
                .filter("count >= 10")
                .orderBy("date")
                .write().json(outputFolder + "/task2");


        Dataset<LogRow> ds = spark
                .read().textFile(inputFile)
                .map(pojoMapper, Encoders.bean(LogRow.class));

        ds
                .filter("code >= 400").filter("code < 600")
                .groupBy(window(ds.col("date").as("timestamp"), "1 week", "1 day" ))
                .agg(count("url").as("weekly_avg"))
                .select("window.start", "window.end", "weekly_avg")
                .sort("start")
                .map(resultMapper, Encoders.STRING())
                .write().text(outputFolder + "/task3");

    }

    private static MapFunction<String, LogRow> pojoMapper = row -> {


        LogRow pojo = new LogRow();

        int dateStart = row.indexOf('[') + 1;
        int dateEnd = row.indexOf(']');
        int bracketsStart = row.indexOf('"', dateEnd) + 1;
        int bracketsEnd = row.indexOf('"', bracketsStart);
        int codeStart = row.lastIndexOf('"') + 2;
        int codeEnd = row.indexOf(' ', codeStart);

        String[] brackets = row.substring(bracketsStart, bracketsEnd).split(" ");
        if(brackets.length >= 3){
            pojo.setMethod(brackets[0]);
            pojo.setUrl(brackets[1]);
        }else if(brackets.length == 2){
            if(brackets[0].equals(brackets[0].toUpperCase())){
                pojo.setMethod(brackets[0]);
                pojo.setUrl(brackets[1]);
            }else{
                pojo.setMethod("NONE");
                pojo.setUrl(brackets[0]);
            }

        }else if(brackets.length == 1){
            pojo.setMethod("NONE");
            pojo.setUrl(brackets[0]);
        }

        String date = row.substring(dateStart, dateEnd);
        date = newFormat.format(ZonedDateTime.parse(date, oldFormat));

        pojo.setDate(date);
        pojo.setCode(row.substring(codeStart, codeEnd));

        return pojo;
    };

    private static MapFunction<Row, String> resultMapper = (row)->
            String.format("from:\t%s\tto:\t%s\tavg:\t%d",
                    row.<Timestamp>getAs("start").toString().split(" ")[0],
                    row.<Timestamp>getAs("end").toString().split(" ")[0],
                    row.<Long>getAs("weekly_avg").intValue()
            ).concat(System.lineSeparator());

}
