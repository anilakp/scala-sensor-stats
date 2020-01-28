package kalina.cloud
import java.io.{File, IOException}
import scala.io.Source
import scala.util.{Try, Success, Failure}
import scala.collection.mutable.{Map => MutableMap}

/**
 * @author Piotr KALINA (piotr@kalina.cloud)
 */

object ReportData {
    case class Measurement(sensorId: String, value: String)
}

object Statistics {
    case class SensorStats(min: Int, max: Int, avg: Int)

    private val generalInfo = MutableMap[String, Long]("totalFiles" -> 0, "totalCount" -> 0, "failedCount" -> 0)
    private val sensorsInfo = MutableMap[String, SensorStats]()

    def newFile() = {
        generalInfo.updateWith("totalFiles") ({
            case Some(current) => Some(current+1)
            case None => Some(0)
        })
    }

    private def minUpdate(min: Int, value: Int): Int = {
        if (value < min) {
            value
        }
        min
    }

    private def maxUpdate(max: Int, value: Int): Int = {
        if (value > max) {
            value
        }
        max
    }

    private def avgUpdate(avg: Int, value: Int): Int = {
        (avg+value)/2
    }

    def sensorsUpdate(sensorId: String, value: Option[Int]) = {
        sensorsInfo.updateWith(sensorId) ({
            case Some(current) => {
                if (value.isEmpty) {
                    Some(SensorStats(0,0,0))
                }
                else {
                    Some(SensorStats(minUpdate(current.min, value.get),
                        maxUpdate(current.max, value.get), avgUpdate(current.avg, value.get)))
                }
            }
            case None => {Some(SensorStats(value.get, value.get, value.get))}
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
        val report = s"""Num of processed files: ${generalInfo("totalFiles")}
        |Num of processed measurements: ${generalInfo("totalCount")}
        |Num of failed measurements: ${generalInfo("failedCount")}""".stripMargin
        println(report)

        for ((k,v) <- sensorsInfo) {
            println(s"${k}, ${v.min}, ${v.max}, ${v.avg}")
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
            stats.newFile
            val bufferedSource = io.Source.fromFile(dataFile)
            for (line <- bufferedSource.getLines.drop(1)) {
                val cols = line.split(",").map(_.trim)
                val data = ReportData.Measurement(cols(0),cols(1))
                stats.sensorsUpdate(cols(0), Option(data.value).flatMap(_.toIntOption))

                try {
                    data.value.toLong
                    stats.generalInfoUpdate(1,0)
                } catch {
                    case e: Exception => { stats.generalInfoUpdate(1,1) }
                }

                println(s"${data.sensorId}|${data.value}")
            }
            bufferedSource.close
        }

        stats.print
      }
      catch {
        case exc: Exception => println(s"failed to get list of reports from: ${args(0)}. Reason: ${exc.getMessage}")
      }
  }

}
