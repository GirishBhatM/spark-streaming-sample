package com.streaming.sample;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;

public class Application {

	public static void main(String[] args) throws StreamingQueryException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("XML_PARSER")
				.getOrCreate();
		
		spark.sqlContext().udf().register("amt_Extractor", new AmountExtractor(),DataTypes.DoubleType);
		
		spark.sqlContext().udf().register("id_Extractor", new IdExtractor(),DataTypes.StringType);
		
		
		Dataset<Row> lines = spark
				  .readStream()
				  .format("socket")
				  .option("host", "localhost")
				  .option("port", 9999)
				  .load();
		
		Dataset<Row> amtDs=lines
				.withColumn("amount",functions.callUDF("amt_Extractor",functions.col("value")))
				.withColumn("id",functions.callUDF("id_Extractor",functions.col("value")))
				.drop("value");
		
		Dataset<Row> amtByIdDs=amtDs.groupBy("id").sum("amount").alias("amount");
		
		StreamingQuery query=amtByIdDs.writeStream()
		.outputMode(OutputMode.Update())
		.format("console")
		.start();
		
		query.awaitTermination();
	}
}
