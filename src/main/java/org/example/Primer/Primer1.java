package org.example.Primer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;
import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;

public class Primer1 {
    public void runner(SparkSession spark)
    {
        Dataset<Row> df_Categories = load(spark, "Categories");
        Dataset<Row> df_Products = load(spark, "Products");
        Dataset<Row> df_OrderDetails = load(spark, "OrderDetails");
        Dataset<Row> df_Orders = load(spark, "Orders");
        Dataset<Row> df_Customers = load(spark, "Customers");

        Dataset<Row> df_itog1 = df_Customers.as("cust")
                .join(df_Orders.as("ord"), col("cust.customer_id").equalTo(col("ord.customer_id")), "left")
                .join(df_OrderDetails.as("det"), col("det.order_id").equalTo(col("ord.order_id")), "left")
                .join(df_Products.as("p"), col("p.product_id").equalTo(col("det.product_id")), "left");

        df_itog1.show();

        Dataset<Row> df_itog2 = df_Products.as("p")
                .join(df_Categories.as("cat"), col("p.category_id").equalTo(col("cat.category_id")));

        df_itog2.show();
    }

    public Dataset<Row> load(SparkSession spark, String str)
    {
        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("inferSchema", "true")
                .csv("C:/Users/koshkinas/Desktop/spark/untitled6/src/main/resources/"+str+".csv");
        return df;
    }
}
