package kalina.cloud
import java.io.{File, IOException}
import scala.io.Source
import scala.util.{Try, Success, Failure}
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.immutable.ListMap

object ReportData {
    case class Measurement(sensorId: String, value: String)
}

object Statistics {
    private val generalInfo = MutableMap[String, Long]("totalFiles" -> 0, "totalCount" -> 0, "failedCount" -> 0)
    private val sensorsInfo = MutableMap[String, (Int, Int, Int)]()

    def add() = {
        generalInfo.updateWith("totalFiles") ({
            case Some(current) => Some(current+1)
            case None => Some(0)
        })
    }

    private def minUpdate(min: Int, value: Int): Int = {
        if (min == -1) value else {
            if (value < min) value else min
        }
    }

    private def avgUpdate(avg: Int, value: Int): Int = {
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // this is wrong calculation of average (found during final tests)
    // 
    // in order to fix it redesign is required:
    //
    // 1. instead of calculating min/max/avg on fly we should generate stats map per file
    // 2. perform map merge/combine process, which will reduce the sensor data min/max and
    // 3. calculate properly the average
    // 4. the process of sorting values can be kept as it is now
    //
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        if (avg != -1) (avg+value)/2 else value
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
                        avgUpdate(current._2, value.get),
                        maxUpdate(current._3, value.get)
                    ))
                } else {
                    Some(current)
                }
            }
            case None => {
                if (value.isEmpty) {
                    Some((-1,-1,-1))
                } else {
                    Some((value.get, value.get, value.get))
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

    def formatValue(value: Int): String = {
        if (value > -1)
            value.toString
        else
            "Nan"
    }

    def print() = {
        val report = s"""Num of processed files: ${generalInfo("totalFiles")}
        |Num of processed measurements: ${generalInfo("totalCount")}
        |Num of failed measurements: ${generalInfo("failedCount")}
        |
        |Sensors with highest avg humidity:
        |
        |sensor-id,min,avg,max""".stripMargin

        println(report)

        for ((k,v) <- ListMap(sensorsInfo.toSeq.sortWith(_._2._2 > _._2._2):_*)) {
            println(s"${k}, ${formatValue(v._1)}, ${formatValue(v._2)}, ${formatValue(v._3)}")
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
        val stats = Statistics
        for (dataFile <- dataFiles) {
            stats.add
            val bufferedSource = io.Source.fromFile(dataFile)
            for (line <- bufferedSource.getLines.drop(1)) {
                val cols = line.split(",").map(_.trim)
                val data = ReportData.Measurement(cols(0),cols(1))
                val someValue: Option[Int] = Option(data.value).flatMap(_.toIntOption)
                stats.sensorUpdate(cols(0), someValue)
            }
            bufferedSource.close
        }

        stats.print
      }
      catch {
        case exc: Exception => println(s"failed to process reports from: ${args(0)}. Reason: ${exc.getMessage}")
      }
  }

}
