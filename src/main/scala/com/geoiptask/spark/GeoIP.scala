package com.geoiptask.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GeoIP {

  def readFile(blocks: RDD[String], csv: Boolean = true): (RDD[Array[String]], Array[String]) = {
    val header = if (csv) blocks.first() else ""
    val blocksWithoutHeader = blocks.filter(x => x != header)
    val splittedHeader = header.split(",")
    val mappedBlocks = blocksWithoutHeader.map(x => {
      val delQuotes = x.replaceAll("\"", "")
      delQuotes.split(",")
    })
    (mappedBlocks, splittedHeader)
  }


  def udfDataframe(ip: Long,  ipLoc: Array[Array[String]]): Array[String] = {

    val ipRangeStart = ipLoc.map(x=>x(0).toLong)
    val bs = binarySearch(ip,ipRangeStart)
    val location = ipLoc.filter(_(0).toLong == bs)(0)
    location

  }

  def binarySearch(ip: Long, startIpNums: Array[Long]): Long = {
    var left = 0
    var right = startIpNums.length - 1
    var pickedIp = Long.MinValue
    var diff = Long.MaxValue
    var id = 0
    while(left <= right) {
      val mid = (left + right) / 2
      id=mid
      val current = startIpNums(mid)
      if(current == ip) {
        pickedIp = ip
        left = right + 1
      }
      else {
        if(current < ip) left = mid + 1  else right = mid - 1
        val iterDiff = (current - ip).abs
        if(iterDiff < diff) {
          diff = iterDiff
          pickedIp = current
        }
      }
    }
    if(ip<pickedIp)
      pickedIp = startIpNums(id-1)
    pickedIp
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local[*]").appName("geoIP").getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", spark.sparkContext.getConf.get("spark.aws.credentials"))

    val blocks = spark.sparkContext.textFile(spark.sparkContext.getConf.get("spark.filepath.blocks"))
    import spark.implicits._
    val blocksCsv = readFile(blocks)
    val mappedBlocks = blocksCsv._1
    val header = blocksCsv._2
    val blocksDf = mappedBlocks.map(x => {
      (x(0).toLong, x(1).toLong, x(2).toInt)
    }).toDF(header: _*)

    val minmax = blocksDf.agg(min("startIpNum"),max("endIpNum"))
    val minIp = minmax.select("min(startIpNum)").first().getLong(0)
    val maxIp = minmax.select("max(endIpNum)").first().getLong(0)


    val location = spark.sparkContext.textFile(spark.sparkContext.getConf.get("spark.filepath.location"))
    val locationCsv = readFile(location)
    val mappedLocation = locationCsv._1
    val headerLoc = locationCsv._2
    val locationDf = mappedLocation.map(x => {
      (x(0).toInt, x(1), x(3))
    }).toDF(headerLoc(0), headerLoc(1), headerLoc(3))


    val joined = blocksDf.join(locationDf, Seq("locId"), "left").orderBy("startIpNum").collect().map(x=>
      Array(x.getLong(1).toString,
      x.getString(3),
      x.getString(4))
    )

    val broadcastedJoin =  spark.sparkContext.broadcast(joined)

    val visits = spark.sparkContext.textFile(spark.sparkContext.getConf.get("spark.filepath.visits"))
    val splittedVisits = readFile(visits, false)._1
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val visitsDf = splittedVisits.map(x => {
      (x(0).replaceAll("\\.", "").toLong, format.parse(x(2)).getYear+1900, x(3).toFloat)
    }).toDF("ip", "year", "adRevenue")

    val filtredVisits = visitsDf.filter(
      visitsDf("ip")>=minIp && visitsDf("ip")<=maxIp
    )

    val findStartIpNumUdf = udf((ip: Long) => udfDataframe(ip, broadcastedJoin.value))
    val visitsWithStartIpNum = filtredVisits.withColumn("startIpNum", findStartIpNumUdf(filtredVisits("ip")))
      .select($"year", $"startIpNum"(1).as("country"), $"startIpNum"(2).as("city"), $"adRevenue")

    val filteredCities = visitsWithStartIpNum.filter(visitsWithStartIpNum("city") =!= "" )


    val adRevenueSum = filteredCities.groupBy("year","country","city")
      .agg(sum("adRevenue"))

    val ranking = Window.partitionBy("year").orderBy($"sum(adRevenue)".desc)

    val rankedCities = adRevenueSum.withColumn("rank", rank().over(ranking))

    val topCities = rankedCities.filter(rankedCities("rank")<=3).orderBy("year","rank")

    topCities.show(10)

    topCities.
      coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .save(spark.sparkContext.getConf.get("spark.filepath.output"))

  }

}
