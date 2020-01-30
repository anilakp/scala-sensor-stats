package test.luxoft
import java.io.{File, IOException}
import scala.io.Source
import scala.util.{Try, Success, Failure}
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.immutable.ListMap

object ReportData {
    case class Measurement(sensorId: String, value: String)
}

class SensorStatistics {
    val generalInfo = MutableMap[String, Int]("totalFiles" -> 0, "totalCount" -> 0, "failedCount" -> 0)
    val sensorsInfo = MutableMap[String, (Int, Int, Long, Long, Long)]()

    private def minUpdate(min: Int, value: Int): Int = {
        if (min == -1) value else {
            if (value < min) value else min
        }
    }

    private def sumUpdate(sum: Long, value: Long): Long = {
         sum + value
    }

    private def maxUpdate(max: Int, value: Int): Int = {
        if (value > max) value else max
    }

    def sensorUpdate(sensorId: String, value: Option[Int]) = {
        sensorsInfo.updateWith(sensorId) ({
            case Some(current) => {
                if (value.isDefined) {
                    Some((
                        minUpdate(current._1, value.get),
                        maxUpdate(current._2, value.get),
                        sumUpdate(current._3, value.get.toLong),
                        current._4 + 1,
                        current._5
                    ))
                } else {
                    Some(current._1, current._2, current._3, current._4 + 1, current._5 + 1)
                }
            }
            case None => {
                if (value.isEmpty) {
                    Some((-1,-1, 0, 1, 1))
                } else {
                    Some((value.get, value.get, value.get.toLong, 1, 0))
                }
            }
        })

        generalInfo.updateWith("totalCount") ({
            case Some(current) => Some(current+1)
            case None => Some(0)
        })

        if (value.isEmpty) {
            generalInfo.updateWith("failedCount") ({
                case Some(current) => Some(current+1)
                case None => Some(0)
            })
        }
    }

    def mergeWith(stats: SensorStatistics): SensorStatistics = {
        // Maybe using scalaz and/or Semigroup from Cats framework could do the trick
        // During merge we update min, max and rest of information from sensorsInfo
        // sum + sumCount will be used at final stage to calculate average
        //
        // val merged = generalInfo ++ ps.generalInfo.map {
        //    case (key,count) => key -> (count + generalInfo.getOrElse(key,0)) }

        // val report = s"""
        // |Num of processed files: ${merged("totalFiles")}
        // |Num of processed measurements: ${merged("totalCount")}
        // |Num of failed measurements: ${merged("failedCount")}""".stripMargin
        // println(report)
        //
        stats
    }

    def formatValue(value: Long): String = {
        if (value > -1)
            value.toString
        else
            "Nan"
    }

    def print() = {
        val report = s"""
        |Num of processed files: ${generalInfo("totalFiles")}
        |Num of processed measurements: ${generalInfo("totalCount")}
        |Num of failed measurements: ${generalInfo("failedCount")}
        |
        |Sensors with highest avg humidity:
        |
        |sensor-id,min,max,sum,count,failed""".stripMargin

        println(report)

        for ((k,v) <- ListMap(sensorsInfo.toSeq.sortWith(_._2._2 > _._2._2):_*)) {
            println(s"${k}, ${formatValue(v._1)}, ${formatValue(v._2)}, ${v._3}, ${v._4}, ${v._5}")
        }
    }
}

object SensorStats {

  def getListOfReportsData(path: String): List[File] = {
   val dir = new File(path)
   val extensions = List("csv", "CSV")
   if (dir.exists && dir.isDirectory) {
    val csvFiles = dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
    if (csvFiles.size == 0) {
        throw new Exception("no .csv files found")
    }
    csvFiles
   }
   else {
    throw new IOException("provided path does not point to a valid directory")
   }
  }

  def main(args : Array[String]) {
      try {
        val dataFiles = getListOfReportsData(args(0))
        val totalStats = new SensorStatistics
        for (dataFile <- dataFiles) {
            val stats = new SensorStatistics
            val bufferedSource = io.Source.fromFile(dataFile)
            for (line <- bufferedSource.getLines.drop(1)) {
                val cols = line.split(",").map(_.trim)
                val data = ReportData.Measurement(cols(0),cols(1))
                val someValue: Option[Int] = Option(data.value).flatMap(_.toIntOption)
                stats.sensorUpdate(cols(0), someValue)
            }
            bufferedSource.close
            stats.print

            totalStats.mergeWith(stats)
        }

      }
      catch {
        case exc: Exception => println(s"failed to process reports from: ${args(0)}. Reason: ${exc.getMessage}")
      }
  }

}
