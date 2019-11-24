package log_clean

import org.apache.commons.lang.math.NumberUtils


object logsUtils {
  def start(log:String):Boolean={
    if (log == null) return false
    if (!log.trim.startsWith("{") || !log.trim.endsWith("}")) return false

    return true
  }
  def event(log:String):Boolean={
    // 1 切割
    val logContents = log.split("\\|")

    // 2 校验
    if (logContents.length != 2) return false

    //3 校验服务器时间
    if (logContents(0).length != 13 || !NumberUtils.isDigits(logContents(0))) return false

    // 4 校验json
    if (!logContents(1).trim.startsWith("{") || !logContents(1).trim.endsWith("}")) return false

    return true


  }

}
