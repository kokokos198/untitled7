package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Primer.Primer1;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                //   .config("spark.sql.catalog.defaultDatabase", "GVS")
                .master("local[*]")
                .getOrCreate();
        Primer1 primer1 = new Primer1();
        primer1.runner(spark);

    }
}