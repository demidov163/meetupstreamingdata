package com.streamingdata.spark.starters;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class StarterAnalysis {

    public static void main(String[] args) throws Exception {
        SparkConf conf = prepareSparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
    }

    /*
    https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
     */
    private static SparkConf prepareSparkConf() {
        String appName = "group-analysis";
        String master = "local[*]";
        return new SparkConf().setAppName(appName).setMaster(master);
    }

}
