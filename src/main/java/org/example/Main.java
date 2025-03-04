package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Primer.Primer1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws IOException {

        String fil_unload1 = "C:/Users/koshkinas/Desktop/spark/untitled6/src/main/resources/df_itog1.csv";
        String fil_unload2 = "C:/Users/koshkinas/Desktop/spark/untitled6/src/main/resources/df_itog2.csv";

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                //   .config("spark.sql.catalog.defaultDatabase", "GVS")
                .master("local[*]")
                .getOrCreate();
        Primer1 primer1 = new Primer1();
        primer1.runner(spark, fil_unload1, fil_unload2);


//        if(Files.exists(Path.of(fil_unload1)))
//        {
//            primer1.select(spark, fil_unload1);
//        }
//
//        if(Files.exists(Path.of(fil_unload2)))
//        {
//            primer1.select(spark, fil_unload2);
//        }

    }
}