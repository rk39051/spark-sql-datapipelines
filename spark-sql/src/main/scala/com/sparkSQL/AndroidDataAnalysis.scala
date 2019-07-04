package com.sparkSQL

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType

object AndroidDataAnalysis {
  def main(args:Array[String]){
    System.setProperty("haddop.tmp.dir","D:\\winutils");
    Logger.getLogger("org").setLevel(Level.ERROR);
   
    val spark=new SparkSession.Builder().  //creating spark session
             appName("SparkSQL").
             master("local[*]").
            getOrCreate();
    
    val appsDF=spark.read.format("csv").       //loading apps data
                option("header", "true").
                option("inferSchema", "true").
                option("mode", "DROPMALFORMED").
                load("data/googleplaystore.csv");
    appsDF.printSchema();//printing appsDF DataFrame schema
    
     val reviewsDF=spark.read.format("csv").       //loading reviews data
                option("header", "true").
                option("inferSchema", "true").
                option("mode", "DROPMALFORMED").
                load("data/googleplaystore_user_reviews.csv");
    reviewsDF.printSchema();//printing reviewsDF DataFrame schema
    
    appsDF.show(4);// showing top 4 rows
    reviewsDF.show(4);
    /* 
     * for analysis purpose we have to clean the data and rename its column data types,
     * for example,
     * the $ symbol from price and convert it into DoubleType,
     * the , and + symbol from installs and convert it into DoubleType,
     * the symbol M from Size and convert it into DoubleType,
     * convert reviews into IntegerType etc.
     * */
     
    
    import spark.implicits._
     val appsDF1=appsDF.withColumn("Price", functions.regexp_replace($"Price", "$", "")).
            withColumn("Size", functions.regexp_replace($"Size","M", "")).
            withColumn("Installs", functions.regexp_replace($"Installs", ",|\\+", "")).
            withColumn("Reviews", functions.regexp_replace($"Reviews", "NaN|,", "0"));
    
    val appsDF2=appsDF1.withColumnRenamed("Content Rating", "content_rating").
                        withColumnRenamed("Last Updated", "last_updated").
                        withColumnRenamed("Current Ver", "current_ver").
                        withColumnRenamed("Android Ver", "android_ver");
     
     val modifiedAppsDF=appsDF2.withColumn("Price", $"Price".cast(DoubleType)).
            withColumn("Size", $"Size".cast(DoubleType)).
            withColumn("Installs", $"Installs".cast(DoubleType)).
            withColumn("Reviews", $"Reviews".cast(IntegerType));
            
    modifiedAppsDF.printSchema();  
    modifiedAppsDF.show(4);
    
    modifiedAppsDF.groupBy("Category").count().show(4);// category wise count
    modifiedAppsDF.filter("Size > 10").show(4);//filtering based on size of apps
    modifiedAppsDF.select(functions.avg("Size"),functions.max("Size"),functions.min("Size")).show();//finding min,max,avg size of apps
    
    
    val op1=modifiedAppsDF.select("App","Category","Rating","Size","Price","current_ver","android_ver");
    //saving output as parquet file
    op1.write.mode("overwrite").parquet("./output/op1");
    
    //load data from parquet file and registering as table
    spark.read.parquet("./output/op1/part*").registerTempTable("final_table");
    spark.sql("select * from final_table").show(10);
    
    //performing join
    val joinDF=op1.join(reviewsDF, "App");
    //selecting particular value from joinDF and registering as join_table
    joinDF.select("App","Rating","Size","Sentiment","Sentiment_Polarity").filter("Sentiment == 'Positive'").registerTempTable("join_table");
    
    val op2=spark.sql("Select App,avg(Rating) as avg_rating from join_table group by App order by avg_rating desc  ");
    op2.show(10);
    op2.write.mode("overwrite").parquet("./output/op2");
    


  }
}