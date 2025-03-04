package org.example.Primer;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.apache.spark.sql.functions.*;

public class Primer1 {
    public void runner(SparkSession spark, String fil_unload1, String fil_unload2) throws IOException {
        Dataset<Row> df_Categories = load(spark, "Categories");
        Dataset<Row> df_Products = load(spark, "Products");
        Dataset<Row> df_OrderDetails = load(spark, "OrderDetails");
        Dataset<Row> df_Orders = load(spark, "Orders");
        Dataset<Row> df_Customers = load(spark, "Customers");

        Dataset<Row> df_itog1 = df_Customers.as("cust")
                .join(df_Orders.as("ord"), col("cust.customer_id").equalTo(col("ord.customer_id")), "left")
                .join(df_OrderDetails.as("det"), col("det.order_id").equalTo(col("ord.order_id")), "left")
                .join(df_Products.as("p"), col("p.product_id").equalTo(col("det.product_id")), "left")
                .select("cust.*",
                        "ord.order_id",
                        "ord.order_date",
                        "ord.status",
                        "det.product_id",
                        "det.quantity",
                        "p.product_name",
                        "p.price",
                        "p.category_id");

        Dataset<Row> df_itog2 = df_Products.as("p")
                .join(df_Categories.as("cat"), col("p.category_id").equalTo(col("cat.category_id")))
                .select("p.*","cat.category_name");
       // df_itog2.show();

        df_sel(spark, fil_unload1, df_itog1, "df_itog1");
    //    df_sel(spark, fil_unload2, df_itog2, "df_itog2");
    }

    public void df_sel(SparkSession spark, String fil_unload, Dataset<Row> dataset, String string1) throws IOException {
        if(!Files.exists(Paths.get(fil_unload)))
        {
            Dataset<Row>  df_itog = dataset.withColumn("update_status", lit(0));
            List<String> list = df_itog.collectAsList().stream()
                    .map(s->s.toString())
                    .map(str->str.substring(1, str.length() - 1))
                    .collect(Collectors.toList());
            if (string1.equals("df_itog2")) {
                upload_file(list, fil_unload, 1, 2);
            } else {
                upload_file(list, fil_unload, 1, 1);
            }

        }
        else {
            System.out.println(string1);
            Dataset<Row> df_ex3 = null;
            if(string1.equals("df_itog2")) {
                Dataset<Row> df_up2 = load(spark, string1);

                Dataset<Row> df_ex1 = dataset.except(df_up2.select("product_id", "product_name", "price", "category_id", "category_name"));

                Dataset<Row> df_up3 = df_up2.groupBy("product_id").agg(max("update_status").as("update_status")).select("product_id", "update_status");

                Dataset<Row> df_ex2 = df_ex1.as("ex1").join(df_up3.as("up3"), col("up3.product_id").equalTo(col("ex1.product_id")), "inner")
                        .select(col("ex1.product_id")
                                , col("ex1.product_name")
                                , col("ex1.price")
                                , col("ex1.category_id")
                                , col("ex1.category_name")
                                , col("up3.update_status"));


                df_ex3 = df_ex2.select(
                        col("product_id"), col("product_name"), col("price"), col("category_id"), col("category_name"),
                        when(col("update_status").isNotNull(), col("update_status").plus(1)).otherwise(lit(0)).as("update_status"));

                df_ex3.show();
                System.out.println("df_ex3");


            }else {
                Dataset<Row> df_up2 = load(spark, string1);
              //  Dataset<Row> df_up  = dataset.withColumn("price", when(col("price").gt(100), lit(8)).otherwise(col("price")));

                Dataset<Row> df_ex1 = dataset.except(df_up2.select(
                        col("df_up2.customer_id"),
                        col("df_up2.name"),
                        col("df_up2.email"),
                        col("df_up2.phone"),
                        col("df_up2.delivery_address"),
                        col("df_up2.order_id"),
                        col("df_up2.order_date"),
                        col("df_up2.status"),
                        col("df_up2.product_id"),
                        col("df_up2.quantity"),
                        col("df_up2.product_name"),
                        col("df_up2.price"),
                        col("df_up2.category_id")));

                Dataset<Row> df_up3 = df_up2.groupBy("customer_id", "product_id", "order_id").agg(max("update_status").as("update_status")).select("customer_id", "product_id", "order_id", "update_status");

                Dataset<Row> df_ex2 = df_ex1.as("ex1")
                        .join(df_up3.as("up3"), col("up3.customer_id").equalTo(col("ex1.customer_id"))
                                        .and(col("up3.product_id").equalTo(col("ex1.product_id")))
                                        .and(col("up3.order_id").equalTo(col("ex1.order_id")))
                                        .and(col("up3.update_status").equalTo(col("ex1.update_status"))), "inner")
                        .select(col("ex1.customer_id")
                                , col("ex1.name")
                                , col("ex1.email")
                                , col("ex1.phone")
                                , col("ex1.delivery_address")
                                , col("ex1.order_id")
                                , col("ex1.order_date")
                                , col("ex1.status")
                                , col("ex1.product_id")
                                , col("ex1.quantity")
                                , col("ex1.product_name")
                                , col("ex1.price")
                                , col("ex1.category_id")
                                , col("up3.update_status"));

                df_ex3 = df_ex2.select(
                        col("product_id"), col("product_name"), col("price"), col("category_id"), col("category_name"),
                        when(col("update_status").isNotNull(), col("update_status").plus(1)).otherwise(lit(0)).as("update_status"));
            }

            List<String> list = df_ex3.collectAsList().stream().
                    map(s -> s.toString())
                    .map(str -> str.substring(1, str.length() - 1))
                    .collect(Collectors.toList());
            upload_file(list, fil_unload, 0, 2);

        }

    }

    public void select(SparkSession spark, String fil_unload)
    {
        Dataset<Row> df_itog2 = load(spark, fil_unload);
         Set<String> list= df_itog2.select(col("update_status")).distinct().collectAsList().stream().map(s->s.toString()).sorted().collect(Collectors.toSet());

        Optional<Set<String>> opt = Optional.ofNullable(list);
        if(opt.isPresent())
        {
            System.out.println("введите номер загрузки из предоставленных который хотите отобразить");
            list.forEach(l->System.out.println(l));
            Scanner scanner = new Scanner(System.in);
            int sc = scanner.nextInt();
            Dataset<Row> df_select =  df_itog2.where(col("update_status").equalTo(sc));
            df_select.show();
        }
       else {
           System.out.println("Загрузок в файле " + df_itog2 + " нету");
        }
    }

    public Dataset<Row> load(SparkSession spark, String str)
    {
        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("inferSchema", "true")
                .csv("C:/Users/koshkinas/Desktop/spark/untitled6/src/main/resources/"+str+".csv");
        return df;
    }

    public void upload_file(List<String> list, String path, int parametr1, int parametr2) throws IOException
    {
        String string1 = parametr2 == 2 ? "product_id,product_name,price,category_id,category_name,update_status" : "customer_id, name, email, phone, delivery_address, order_id, order_date, status, product_id, quantity, product_name, price, category_id, update_status";

        FileWriter fileWriter = new FileWriter(path, true);

if(parametr1 == 1)
{
    fileWriter.write(string1);
    fileWriter.write("\n");
}

        list.forEach(x -> {
            try {
                fileWriter.write(x);
                fileWriter.write("\n");
                //    fileWriter.close();
            } catch (IOException e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }
        });
        fileWriter.close();
    }
}
