package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataFrameBasics.{carsDFSchema, spark}

//Caused by: java.io.FileNotFoundException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset

object DataSources extends App {
  val spark = SparkSession
    .builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  //schema
  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // failfast will throw exception incase faulty other modes include: dropMalformed(ignore faulty rows), permissive(default)
    .option("path","src/main/resources/data/cars.json")
    .load()

  //alternative reading
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode"->"failFast",
      "path"->"src/main/resources/data/cars.json",
      "inferSchema"->"true"
    ))
    .load()

  /*
  Writing DFs:
  -format
  -save mode = overwrite, append, ignore, errorIfExists
  -path
  -zero or more options
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

}
