package dataquality.file.producer

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.util.concurrent.TimeUnit
import java.util.{Date, GregorianCalendar}

import scala.concurrent.duration.Duration
import scala.util.Random

sealed trait Anomaly

case object TemperatureA extends Anomaly

case object HumidityA extends Anomaly

case object VisibilityA extends Anomaly

case object WindA extends Anomaly

object TransactionFileProducer {


  val startDateMillis = new GregorianCalendar(2020, 0, 1).getTime.getTime
  val tempBase = 32
  val humidityBase: Int = 100
  val visibilityBase = 10
  val windSpeedBase = 0

  val abnormalTemp = List(90, 100, 110)
  val abnormalHumidity = List(0, 5, 10)
  val abnormalVisibility = List(0, 1)
  val abnormalWind = List(40, 50, 60)

  val random = new Random()

  def main(args: Array[String]) {
    val station1 = "55511100222"
    val station2 = "33399977444"
    val stations = List(station1, station2)
    val anomalies = List(TemperatureA, HumidityA, VisibilityA, WindA)

    val linesCount = args.headOption.map(_.toInt).getOrElse(1000) / 2
    val file = args.lift(1).getOrElse("weather_data")

    var writer: BufferedWriter = null
    try {
      writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
      println(s"Begin generating $linesCount records for 2 weather stations into a file $file")
      // Anomaly will appear randomly within 24h
      var anomalyHour = random.nextInt(24)
      for (i <- 0 until linesCount) {
        def generateDataAndOutput(stat: String): Unit = {
          val stationData = produceWeatherData(stat, i, None)
          println(stationData)
          writer.write(stationData + "\n")
        }

        if (i == anomalyHour) {
          val stationIndex = random.nextInt(stations.size - 1)
          val stationWithAnomaly = stations(stationIndex)
          val anomaly = anomalies(random.nextInt(anomalies.size - 1))

          stations.foreach { s =>
            if (s == stationWithAnomaly) {
              val stationData = produceWeatherData(s, i, Some(anomaly))
              println(stationData)
              writer.write(stationData + "\n")
            } else {
              generateDataAndOutput(s)
            }
          }
          anomalyHour += random.nextInt(24)
        } else {
          stations.foreach { s =>
            generateDataAndOutput(s)
          }
        }
      }
      println(s"Completed generating ${linesCount * 2} records into a file $file")
    } finally {
      if (writer != null) {
        writer.close()
      }
    }

    def produceWeatherData(station: String, iteration: Int, anomalyOpt: Option[Anomaly]): String = {
      val actualDateMillis = Duration(startDateMillis, TimeUnit.MILLISECONDS).plus(Duration(iteration, TimeUnit.HOURS)).toMillis
      val actualDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        .format(new Date(actualDateMillis))

      val hourOfDay = iteration % 24

      val defaultTemp = hourOfDay match {
        case h if h <= 6 => tempBase + h
        case h if h > 6 && h <= 12 => tempBase + h + math.floor(random.nextDouble * 3).toInt
        case h if h > 12 && h <= 16 => tempBase + h - 2 + math.floor(random.nextDouble * 3).toInt
        case h if h > 16 && h <= 18 => tempBase + h - 6 - math.floor(random.nextDouble * 3).toInt
        case h => tempBase * 2 - h - 6 - math.floor(random.nextDouble * 2).toInt
      }
      val defaultHumidity = hourOfDay match {
        case h if h <= 6 => humidityBase - hourOfDay - math.floor(random.nextDouble * 10).toInt
        case h if h > 6 && h <= 12 => humidityBase - hourOfDay - math.floor(random.nextDouble * 5).toInt
        case h if h > 12 && h <= 16 => humidityBase - hourOfDay
        case h if h > 16 && h <= 18 => humidityBase - hourOfDay + math.floor(random.nextDouble * 10).toInt
        case _ => humidityBase - math.floor(hourOfDay / 3 * 2) + math.floor(random.nextDouble * 10).toInt
      }
      val defaultVisibility: Int = hourOfDay match {
        case h if h <= 6 => visibilityBase - math.floor(random.nextDouble * 2).toInt
        case h if h > 6 && h <= 12 => visibilityBase - math.floor(random.nextDouble * 3).toInt
        case h if h > 12 && h <= 16 => visibilityBase - math.floor(random.nextDouble * 2).toInt
        case h if h > 16 && h <= 18 => visibilityBase - math.floor(random.nextDouble * 3).toInt
        case _ => visibilityBase - math.floor(random.nextDouble * 2).toInt
      }
      val defaultWindSpeed = hourOfDay match {
        case h if h <= 8 => windSpeedBase + math.floor(random.nextDouble * 4)
        case h if h > 8 && h <= 20 => windSpeedBase + math.floor(random.nextDouble * 4 + random.nextInt(3))
        case _ => windSpeedBase + math.floor(random.nextDouble * 2)
      }

      val allFields = anomalyOpt match {
        case None =>
          val actualTemp = defaultTemp
          val actualHumidity = defaultHumidity
          val actualVisibility = defaultVisibility
          val actualWindSpeed = defaultWindSpeed
          (actualTemp, actualHumidity.toInt, actualVisibility, actualWindSpeed.toInt)

        case Some(anomaly) => anomaly match {
          case TemperatureA =>
            val actualTemp = abnormalTemp(random.nextInt(abnormalTemp.size))
            val actualHumidity = defaultHumidity
            val actualVisibility = defaultVisibility
            val actualWindSpeed = defaultWindSpeed
            println(s"Abnormal temperature: $actualTemp")
            (actualTemp.toInt, actualHumidity.toInt, actualVisibility, actualWindSpeed.toInt)

          case HumidityA =>
            val actualTemp = defaultTemp
            val actualHumidity = abnormalHumidity(random.nextInt(abnormalHumidity.size))
            val actualVisibility = defaultVisibility
            val actualWindSpeed = defaultWindSpeed
            println(s"Abnormal humidity: $actualHumidity")
            (actualTemp, actualHumidity, actualVisibility, actualWindSpeed.toInt)

          case VisibilityA =>
            val actualTemp = defaultTemp
            val actualHumidity = defaultHumidity
            val actualVisibility = abnormalVisibility(random.nextInt(abnormalVisibility.size))
            val actualWindSpeed = defaultWindSpeed
            println(s"Abnormal visibility: $actualVisibility")
            (actualTemp, actualHumidity.toInt, actualVisibility.toInt, actualWindSpeed.toInt)

          case WindA =>
            val actualTemp = defaultTemp
            val actualHumidity = defaultHumidity
            val actualVisibility = defaultVisibility
            val actualWindSpeed = abnormalWind(random.nextInt(abnormalWind.size))
            println(s"Abnormal wind speed: $actualWindSpeed")
            (actualTemp, actualHumidity.toInt, actualVisibility, actualWindSpeed)
        }
      }

      val (actualTemp: Int, actualHumidity: Int, actualVisibility: Int, actualWindSpeed: Int) = allFields
      s"""{"station":"$station","date":"$actualDate","hourly_temperature":${actualTemp.toInt},"hourly_humidity":${actualHumidity.toInt},"hourly_visibility":${actualVisibility.toInt},"hourly_wind_speed":${actualWindSpeed.toInt}}"""
        .stripMargin
    }
  }


}

