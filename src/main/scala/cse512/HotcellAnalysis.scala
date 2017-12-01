package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=> HotcellUtils.CalculateCoordinate(pickupPoint, 0))
  spark.udf.register("CalculateY",(pickupPoint: String)=> HotcellUtils.CalculateCoordinate(pickupPoint, 1) )
  spark.udf.register("CalculateZ",(pickupTime: String)=> HotcellUtils.CalculateCoordinate(pickupTime, 2) )
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  val cellsFull = pickupInfo.collect()
  var features = scala.collection.mutable.Map[(String, String, String), Double]()

  for (cell <- cellsFull) {
    if(features.contains(cell.get(0).toString, cell.get(1).toString, cell.get(2).toString)) {
      features((cell.get(0).toString, cell.get(1).toString, cell.get(2).toString)) += 1
    }
    else {
      features += ((cell.get(0).toString, cell.get(1).toString, cell.get(2).toString) -> 1)
    }
  }

  // Calculate average X
  val averageX = features.values.sum / numCells

  // Calculate sumX
  val sumX = features.values.sum

  // Calculate sumXSquare
  var x_square : Double = 0.0
  features.values.foreach{(value) => x_square += math.pow(value, 2)}

  /*
    listData is for construct the final DF
    parameters: x, y, t, z_value
  */
  var listData  : List[(Int, Int, Int, Double)] = List()
  for ( x <- -7450 to -7370; y <- 4050 to 4090; t <- 1 to 31) {
    val sumXW = HotcellUtils.calculatesumWX(features, x, y, t)
    val z_value = HotcellUtils.calculateZvalue(sumXW, x_square,sumX, numCells)
    listData = listData:+(x,y,t,z_value)
  }

  // Convert listData to DF

  // Set up DF schema
  val someSchema = List(
    StructField("X", IntegerType, true),
    StructField("Y", IntegerType, true),
    StructField("T", IntegerType, true),
    StructField("ZVALUE", DoubleType, true)
  )

  import spark.implicits._
  var finalDF = listData.toDF("X", "Y", "T", "ZVALUE")

  finalDF.createOrReplaceTempView("records")

  finalDF = spark.sql("SELECT X,Y,T FROM records order by ZVALUE DESC")

  return finalDF



//  val finalDF = spark.createDataFrame(
//    spark.sparkContext.parallelize(someData),
//    StructType(someSchema)
//  )

}
}
