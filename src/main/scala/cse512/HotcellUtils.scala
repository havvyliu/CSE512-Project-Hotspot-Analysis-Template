package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }


  // Calculate out sumWX
  def calculatesumWX(features: scala.collection.mutable.Map[(String, String, String), Double], x: Int, y:Int, t:Int): Double = {
    var xx: Integer = 0
    var yy: Integer= 0
    var tt: Integer = 0
    var result: Double = 0
    for (xx <- x-1 to x+1; yy <- y-1 to y+1; tt <- t-1 to t+1) {
      // Return the count value or 0 which means that no count or cell doesnt exist
      val Xj:Double = features.getOrElse((xx.toString, yy.toString, tt.toString), 0.0)
      result += Xj
    }
    return result
  }

  // To calculate out the z-value
  def calculateZvalue (sumWX: Double, sumXSquare: Double, sumX: Double, n: Double) : Double =
  {
    // TODO change averageX to a constant? To avoid calculate it every time
    val averageX = sumX/n
    val S = math.sqrt(sumXSquare/n - averageX * averageX)
    val lower = S*math.sqrt((n*27 -27*27)/(n-1))
    val upper = sumWX - averageX*27
    return upper/lower
  }

}
