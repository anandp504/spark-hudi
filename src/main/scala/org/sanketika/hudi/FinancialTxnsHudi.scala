package org.sanketika.hudi

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.hudi.config.HoodieWriteConfig.{KEYGENERATOR_CLASS_NAME, KEYGENERATOR_TYPE, TBL_NAME}

object FinancialTxnsHudi extends App {

  val sparkConf = new SparkConf()
    .setAppName("spark-hudi")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .set("hive.metastore.uris", "thrift://localhost:9083")

  val spark = SparkSession
    .builder
    .appName("financian-txns-hudi")
    .master("local[*]")
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  println(spark.sharedState.externalCatalog.unwrapped)

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:4566")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "test")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "testSecret")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")


  val tableName = "financial_txns"
  writeHudiData()
  readHudiData()

  def writeHudiData(): Unit = {
    val hudiWriteDF = spark.read.json("/Users/anand/Documents/Sanketika/obsrv_product/Finacle/demo/data/transaction_data.json")
    hudiWriteDF.write.format("hudi")
      .option(RECORDKEY_FIELD.key(), "txn_id")
      .option(PRECOMBINE_FIELD.key(), "txn_date")
      .option(PARTITIONPATH_FIELD.key(), "txn_date")
      .option(KEYGENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.TimestampBasedKeyGenerator")
      .option("hoodie.deltastreamer.keygen.timebased.timestamp.type", "DATE_STRING")
      .option("hoodie.keygen.timebased.input.dateformat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
      // .option("hoodie.deltastreamer.keygen.timebased.timezone", "GMT+5:30")
      .option("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyy-MM-dd")
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.hive_sync.enable", "true")
      .option("hoodie.datasource.hive_sync.database", "obsrv")
      .option("hoodie.datasource.hive_sync.table", "financial_txns")
      .option("hoodie.datasource.hive_sync.username", "postgres")
      .option("hoodie.datasource.hive_sync.password", "postgres")
      .option("hoodie.datasource.hive_sync.mode", "hms")
      .option("hoodie.datasource.hive_sync.use_jdbc", "false")
      .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://localhost:9083")
      .option(TBL_NAME.key(), tableName)
      .mode(Overwrite)
      // .save(s"/opt/data/hudi/hudi_ondc_mor")
      .save(s"s3a://obsrv/data/$tableName")
  }

  def readHudiData(): Unit = {
    val hudiReadDF = spark.read.format("hudi").load(s"s3a://obsrv/data/$tableName")
    hudiReadDF.createOrReplaceTempView("financial_txns")
    spark.sql("select count(*) from financial_txns").show()
    // spark.sql("select context.message_id from financial_txns").show()
    // spark.sql("select context.bap_id, context.bpp_id, count(*) from ondc_data group by 1 order by 1").show()
  }
}

