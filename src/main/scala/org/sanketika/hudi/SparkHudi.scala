package org.sanketika.hudi

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME

object SparkHudi extends App {

  val sparkConf = new SparkConf()
    .setAppName("spark-hudi")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .set("spark.sql.warehouse.dir", "/Users/anand/Documents/Sanketika/obsrv_product/hudi/spark_warehouse")
    .set("hive.metastore.uris", "thrift://localhost:9083")

  val spark = SparkSession
    .builder
    .appName("spark-hudi")
    .master("local[*]")
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:4566")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "test")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "testSecret")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")


  val tableName = "hudi_ondc_mor"

  println(spark.sharedState.externalCatalog.unwrapped)

  writeHudiData()
  readHudiData()

  def writeHudiData(): Unit = {
    val hudiWriteDF = spark.read.json(this.getClass.getClassLoader.getResource("magicpin-replaced.json").getPath)
    hudiWriteDF.write.format("hudi")
    .option(RECORDKEY_FIELD.key(), "context.message_id")
    .option(PRECOMBINE_FIELD.key(), "context.timestamp")
    .option(PARTITIONPATH_FIELD.key(), "context.bap_id")
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
    .option("hoodie.datasource.hive_sync.enable", "true")
    .option("hoodie.datasource.hive_sync.database", "hudi_poc")
    .option("hoodie.datasource.hive_sync.table", "hudi_ondc_mor")
    .option("hoodie.datasource.hive_sync.username", "postgres")
    .option("hoodie.datasource.hive_sync.password", "postgres")
    .option("hoodie.datasource.hive_sync.mode", "hms")
    .option("hoodie.datasource.hive_sync.use_jdbc", "false")
    .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://localhost:9083")
    .option(TBL_NAME.key(), tableName)
    .mode(Overwrite)
    // .save(s"/opt/data/hudi/hudi_ondc_mor")
    .save("s3a://hudi/data/hudi_ondc_mor")
  }

  def readHudiData(): Unit = {
     val hudiReadDF = spark.read.format("hudi").load("s3a://hudi/data/hudi_ondc_mor")
     hudiReadDF.createOrReplaceTempView("ondc_data")
     spark.sql("select count(*) from ondc_data").show()
     spark.sql("select context.message_id from ondc_data").show()
     // spark.sql("select context.bap_id, context.bpp_id, count(*) from ondc_data group by 1 order by 1").show()
  }
}
