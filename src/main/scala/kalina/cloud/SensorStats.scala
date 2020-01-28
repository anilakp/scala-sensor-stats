package kalina.cloud
import java.io.{File, IOException}
import scala.io.Source
import scala.util.{Try, Success, Failure}
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.immutable.ListMap

/**
 * @author Piotr KALINA (piotr@kalina.cloud)
 */

object ReportData {
    case class Measurement(sensorId: String, value: String)
}

object Statistics {
    private val generalInfo = MutableMap[String, Long]("totalFiles" -> 0, "totalCount" -> 0, "failedCount" -> 0)
    private val sensorsInfo = MutableMap[String, (Int, Int, Int)]()

    def newFile() = {
        generalInfo.updateWith("totalFiles") ({
            case Some(current) => Some(current+1)
            case None => Some(0)
        })
    }

    private def minUpdate(min: Int, value: Int): Int = {
        if (value < min) {
            value
        } else {
            min 
        }
    }

    private def maxUpdate(max: Int, value: Int): Int = {
        if (value > max) {
            value
        } else {
            max
        }
    }

    private def avgUpdate(avg: Int, value: Int): Int = {
        (avg+value)/2
    }

    def sensorsUpdate(sensorId: String, value: Option[Int]) = {
        sensorsInfo.updateWith(sensorId) ({
            case Some(current) => {
                if (value.isDefined) {
                    Some((minUpdate(current._1, value.get), maxUpdate(current._2, value.get), avgUpdate(current._3, value.get)))
                } else {
                    Some(current)
                }
            }
            case None => {
                if (value.isEmpty) {
                    Some((0,0,0))
                } else {
                    Some((value.get, value.get, value.get))
                }
            }
        })
    }

    def generalInfoUpdate(count: Long, failed: Long) = {
        generalInfo.updateWith("totalCount") ({
            case Some(current) => Some(current+count)
            case None => Some(0)
        })

        generalInfo.updateWith("failedCount") ({
            case Some(current) => Some(current+failed)
            case None => Some(0)
        })
    }

    def print() = {
        val report = s"""```
        |Num of processed files: ${generalInfo("totalFiles")}
        |Num of processed measurements: ${generalInfo("totalCount")}
        |Num of failed measurements: ${generalInfo("failedCount")}""".stripMargin
        println(report)
        println(s"""
        |Sensors with highest avg humidity:
        |
        |sensor-id,min,avg,max""".stripMargin)

        for ((k,v) <- ListMap(sensorsInfo.toSeq.sortWith(_._2._3 > _._2._3):_*)) {
            // TODO: convert 0 to NaN
            println(s"${k}, ${v._1}, ${v._2}, ${v._3}")
        }
        println("```")
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
            stats.newFile
            val bufferedSource = io.Source.fromFile(dataFile)
            for (line <- bufferedSource.getLines.drop(1)) {
                val cols = line.split(",").map(_.trim)
                val data = ReportData.Measurement(cols(0),cols(1))
                val someValue: Option[Int] = Option(data.value).flatMap(_.toIntOption)
                stats.sensorsUpdate(cols(0), someValue)

                try {
                    data.value.toLong
                    stats.generalInfoUpdate(1,0)
                } catch {
                    case e: Exception => { stats.generalInfoUpdate(1,1) }
                }
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
