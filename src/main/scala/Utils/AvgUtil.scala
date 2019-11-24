package Utils

import java.text.SimpleDateFormat

object AvgUtil {
  def caculateRqt(start: String, endTime: String): Long = {

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")

    val st = dateFormat.parse(start.substring(0, 17)).getTime
    val et = dateFormat.parse(endTime).getTime

    et - st
  }
}
