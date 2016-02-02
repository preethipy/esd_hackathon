import java.io
import java.text.SimpleDateFormat
import java.util.Date

import java.util.{Locale, Date}

import akka.actor.FSM.LogEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.HashMap
import scala.io.Source
import scala.reflect.io.File
import scala.util.matching.Regex

/**
 * Created by preethi on 10/10/15.
 */

object LogAnalysis {
  val conf = new SparkConf()
  //conf.setMaster("spark://localhost.localdomain:7077")
  conf.setMaster("local[2]")
  conf.setAppName("first spark")
  val sc = new SparkContext(conf)


    def main(args: Array[String]) {

    val filename = "/home/preethi/edrive/esd_hackathon/pv_add_cmds/log_conf.txt"
    /*for (line <- Source.fromFile(filename).getLines()) {
      parseConfFile(line, "pv")
    }
*/
      sc.parallelize(Source.fromFile(filename).getLines().map(parseConfFile).toSeq);


    var node1Map = nodeLvlMap("node1")
    val node1CountMap = node1Map map { case (k, v) ⇒ k → (v.count()) }
    node1CountMap foreach {case (key, value) => println (key + "-->" + value)}

      nodeLvlMap foreach{case(key,value) =>
        println(key.toUpperCase())
        value foreach{case(key,value) =>
          println(key)
          println(value.count())
          value.collect().foreach(println)
        }
      }

    var node2Map = nodeLvlMap("node2")
    val node2CountMap = node2Map map { case (k, v) ⇒ k → (v.count()) }
    node2CountMap foreach {case (key, value) => println (key + "-->" + value)}

    var node3Map = nodeLvlMap("node3")
    val node3CountMap = node3Map map { case (k, v) ⇒ k → (v.count()) }
    node3CountMap foreach {case (key, value) => println (key + "-->" + value)}

    var node4Map = nodeLvlMap("node4")
    val node4CountMap = node4Map map { case (k, v) ⇒ k → (v.count()) }
    node4CountMap foreach {case (key, value) => println (key + "-->" + value)}




      var ioscliMap = layerLvlMap("ioscli_global.trace")
      val ioscliCountMap = ioscliMap map { case (k, v) ⇒ k → (v.count()) }
      ioscliCountMap foreach {case (key, value) => println (key + "-->" + value)}

      var cfgMap = layerLvlMap("cfglog")
      val cfgCountMap = cfgMap map { case (k, v) ⇒ k → (v.count()) }
      cfgCountMap foreach {case (key, value) => println (key + "-->" + value)}

      var syslogcaa = layerLvlMap("syslog.caa")
      val syslogCountMap = syslogcaa map { case (k, v) ⇒ k → (v.count()) }
      syslogCountMap foreach {case (key, value) => println (key + "-->" + value)}

      var pool = layerLvlMap("pool.log")
      val poolCountMap = pool map { case (k, v) ⇒ k → (v.count()) }
      poolCountMap foreach {case (key, value) => println (key + "-->" + value)}

      var viocommand = layerLvlMap("vioCmd.log")
      val viocmdCountMap = viocommand map { case (k, v) ⇒ k → (v.count()) }
      viocmdCountMap foreach {case (key, value) => println (key + "-->" + value)}





//    var layerLvlMap = nodeLvlMap("vioCmd.log")

  }


  val EMPTY = new LogEntry("ignore", "ignore", "ignore")


  var startIocliTracing = false;
  def verifyLinesIocliTimeRange(line: String, timing: EntryExitTiming, cmd: String): Boolean = {
    if(line.contains("Command "+cmd+"  Started at ")) {
      startIocliTracing = true
    } else if(line.contains("Command "+cmd+" Ended at ")) {
      startIocliTracing = false
    } else if(startIocliTracing) {
      return true;
    }
    return false
  }

  def containsError(line: String): Boolean = {
    val regex = new Regex("rc\\s+=\\s+[^0]\\d+")
    val lineAccurance = regex findFirstIn line
    if(lineAccurance != None) {
      return true
    }
    return false
  }

  def analyzeIoscli(logEntry: LogEntry, timing: EntryExitTiming, cmd: String): Unit = {
    val format = new java.text.SimpleDateFormat(" MMM  dd yyyy, EEE, hh:mm:ss")
    var startTracing = false
    val inputFile: RDD[String] = sc.textFile(logEntry.absolutePath, 1).cache()
    val filesToBeTraced: RDD[String] = inputFile.filter(line => verifyLinesIocliTimeRange(line, timing, cmd))

    val errorIndicationline: RDD[String] = filesToBeTraced.filter(line => containsError(line))
    errorIndicationline.foreach(println)

    LogAnalaysisDetails("",errorIndicationline,logEntry.nodeName,logEntry.layerFile)
  }

  def errorInPool(line: String): Boolean = {
    val regex = new Regex("rc=[^0]\\s+")
    val nonZerolineaccurance = regex findFirstIn line
    if(nonZerolineaccurance != None) {

      return true
    }
    return false
  }

  def PoolCmdDateTimeFilter(line: String, startTime: Date, endTime: Date): Boolean = {

    val format = new java.text.SimpleDateFormat("s")
    val regex = new Regex(":\\d{10}:")
    var firstAccurance = regex findFirstIn line
    if (firstAccurance != None) {
      val dateStamp = format.parse(firstAccurance.get.split(":").toList(1))
      if (dateStamp.equals(startTime) || dateStamp.equals(endTime) || (dateStamp.after(startTime) && dateStamp.before(endTime))) {
        return true
      }
    }
    return false
  }

  def analyzepool(logEntry: LogEntry,timing: EntryExitTiming, cmd: String): Unit = {


    val absolutePath: RDD[String] = sc.textFile(logEntry.absolutePath, 1).cache()

    val filteredLinesByTime: RDD[String] = absolutePath.filter(line => PoolCmdDateTimeFilter(line, timing.startTime, timing.endTime))

    val poolErrorLines: RDD[String] = filteredLinesByTime.filter(line => errorInPool(line))

    LogAnalaysisDetails("", poolErrorLines,logEntry.nodeName,logEntry.layerFile)

  }

  def parseLogEntry(logentry: LogEntry,timing: EntryExitTiming, cmd: String): Unit = {

    logentry.layerFile match {
      case "pool.log" => analyzepool(logentry, timing, cmd)
      case "ioscli_global.trace" => analyzeIoscli(logentry, timing, cmd)
      case "vioCmd.log" => analyzeviocmd(logentry, timing)
      case "syslog.caa" => analyzesyscaa(logentry, timing)
      case "cfglog" => analyzeCfgLog(logentry)
      case default =>

    }
  }

  def analyzeCfgLog(logEntry: LogEntry): Unit = {
    val errorLines: RDD[String] = filterErrorCfgLog(logEntry.absolutePath, sc)
    LogAnalaysisDetails("", errorLines,logEntry.nodeName,logEntry.layerFile)
    println(""+" "+errorLines+ " "+ logEntry.nodeName + "  "+logEntry.layerFile)
  }

  def filterErrorCfgLog(filepath: String, sc: SparkContext): RDD[String] ={
    //    println("-->filterVioLogBetweenTime. startDateTime:", startDateTime , "endDateTime:", endDateTime)

    val inputFile: RDD[String] = sc.textFile(filepath, 1).cache()
    val filteredlines: RDD[String] = inputFile.filter(line => filterCfglogErrors(line))
    //    filteredlines.foreach(println)
    return filteredlines

  }


  def filterCfglogErrors(eachline: String): Boolean = {

    var ret = false

    val caa_error_pattern = new Regex("Error")
    val line = caa_error_pattern findFirstIn eachline

    if(line!=None){
      ret = true
    }
    return ret

  }

  def vioCmdDateTimeFilter(violine: String, startDateTime: Date, endDateTime: Date): Boolean = {

    //    println("-->vioCmdDateTimeFilter")

    var ret = false

    val date_pattern = new Regex("(Jan|Feb|March|April|May|June|July|Aug|Sep|Oct|Nov|Dec)(\\s+)(\\d{1,2})(\\s+)(\\d{4})," +
      "\\s+(\\d{2}):(\\d{2}):(\\d{2})")
    val vioSdf = new SimpleDateFormat("MMM d yyyy, HH:mm:ss", Locale.ENGLISH)
    val line = date_pattern findFirstIn violine
    if(line!=None){

      val logLineDate = vioSdf.parse(line.get)
      if(logLineDate.equals(startDateTime) || logLineDate.equals(endDateTime) || (logLineDate.after(startDateTime) && logLineDate.before(endDateTime))){
        ret = true
        //        println(true)
      }
    }
    //    println("<--vioCmdDateTimeFilter. ret:",ret)

    return ret

  }

  def nonZeroRc(eachline: String): Boolean = {

    var ret = false

    val rc_nonzero_pattern = new Regex("rc\\s+[^0]\\d+")
    val line = rc_nonzero_pattern findFirstIn eachline

    if(line!=None){
      ret = true
    }
    return ret

  }

  def filterVioLogBetweenTime(filepath: String, sc: SparkContext, startDateTime: Date, endDateTime: Date): RDD[String] ={
    //    println("-->filterVioLogBetweenTime. startDateTime:", startDateTime , "endDateTime:", endDateTime)

    val inputFile: RDD[String] = sc.textFile(filepath, 1).cache()
    val filteredlines: RDD[String] = inputFile.filter(line => vioCmdDateTimeFilter(line, startDateTime, endDateTime))
    //    filteredlines.foreach(println)
    return filteredlines

  }

  def filterErrorVioLog(filteredlines: RDD[String]): RDD[String] ={
    //    println("-->filterVioLogBetweenTime. startDateTime:", startDateTime , "endDateTime:", endDateTime)
    val nonZeroRClines: RDD[String] = filteredlines.filter(line => nonZeroRc(line))
    //    nonZeroRClines.foreach(println)
    return nonZeroRClines

  }
  def analyzeviocmd(logEntry: LogEntry,timing: EntryExitTiming): Unit = {
    //    println("Analyzing pool"+ timing)
    val filteredlines: RDD[String] = filterVioLogBetweenTime(logEntry.absolutePath, sc, timing.startTime, timing.endTime)
    val errLines: RDD[String] = filterErrorVioLog(filteredlines)
    LogAnalaysisDetails("", errLines,logEntry.nodeName,logEntry.layerFile)
  }


  def analyzesyscaa(logEntry: LogEntry,timing: EntryExitTiming): Unit = {
    val filteredlines: RDD[String] = filterSyscaaBetweenTime(logEntry.absolutePath, sc, timing.startTime, timing.endTime)
    val syscaaErrorLines: RDD[String] = filterErrorSyscaaLog(filteredlines)
    LogAnalaysisDetails("", syscaaErrorLines,logEntry.nodeName,logEntry.layerFile)
    println(""+" "+filteredlines+ " "+ logEntry.nodeName + "  "+logEntry.layerFile)
  }


  def filterSyscaaBetweenTime(filepath: String, sc: SparkContext, startDateTime: Date, endDateTime: Date): RDD[String] ={
    //    println("-->filterVioLogBetweenTime. startDateTime:", startDateTime , "endDateTime:", endDateTime)

    val inputFile: RDD[String] = sc.textFile(filepath, 1).cache()
    val filteredlines: RDD[String] = inputFile.filter(line => syscaaDateTimeFilter(line, startDateTime, endDateTime))
    //    filteredlines.foreach(println)
    return filteredlines

  }

  def syscaaDateTimeFilter(syslogLine: String, startDateTime: Date, endDateTime: Date): Boolean = {

    //    println("-->vioCmdDateTimeFilter")

    var ret = false

    val date_pattern = new Regex("(Jan|Feb|March|April|May|June|July|Aug|Sep|Oct|Nov|Dec)(\\s+)(\\d{1,2})(\\s+)" +
      "(\\d{2}):(\\d{2}):(\\d{2})")
    val syssmd = new SimpleDateFormat("MMM d HH:mm:ss", Locale.ENGLISH)
    val covertedStartTime = syssmd.parse(syssmd.format(startDateTime))
    val covertedEndTime = syssmd.parse(syssmd.format(endDateTime))
    val line = date_pattern findFirstIn syslogLine
    if(line!=None){

      var logLineDate = syssmd.parse(line.get)

      if(logLineDate.equals(covertedStartTime) || logLineDate.equals(covertedEndTime) || (logLineDate.after(covertedStartTime) && logLineDate.before(covertedEndTime))){
        ret = true
      }
    }
    //    println("<--vioCmdDateTimeFilter. ret:",ret)

    return ret

  }


  def filterErrorSyscaaLog(filteredlines: RDD[String]): RDD[String] ={
    //    println("-->filterVioLogBetweenTime. startDateTime:", startDateTime , "endDateTime:", endDateTime)
    val caaErrors: RDD[String] = filteredlines.filter(line => filterCaaErrors(line))
    //    nonZeroRClines.foreach(println)
    //    print
    return caaErrors

  }

  def filterCaaErrors(eachline: String): Boolean = {

    var ret = false

    val caa_error_pattern = new Regex("caa:err")
    val line = caa_error_pattern findFirstIn eachline

    if(line!=None){
      ret = true
    }
    return ret

  }


  def parseMainFile(file: io.File, command: String): EntryExitTiming = {

    val absoluteFile: RDD[String] = sc.textFile(file.getAbsolutePath, 1).cache()
    //val absoluteFile = file.getAbsoluteFile()
    val fileName = file.getName()
    val nodeName = fileName.split("\\.").toList.last

    var entryTime = new Date()
    var exitTime = new Date()
    val format = new java.text.SimpleDateFormat(" MMM  dd yyyy, EEE, hh:mm:ss")

    val filteredlinesStart: RDD[String] = absoluteFile.filter(line => line.contains("Command "+command+"  Started at "))
    val filteredlinesExit: RDD[String] = absoluteFile.filter(line => line.contains("Command "+command+" Ended at "))


    if(filteredlinesStart.count()!=0) {
      entryTime = format.parse(filteredlinesStart.first().split("Started at").toList.last.split(">>>>>>").toList.last)
    }
    if(filteredlinesExit.count()!=0) {
      exitTime = format.parse(filteredlinesExit.first().split("Ended at").toList.last.split(">>>>>>").toList.last)
    }

    println(entryTime + "***************" + exitTime)


    new EntryExitTiming(nodeName, command, entryTime, exitTime)
  }

  def parseConfFile(logLocation: String): LogEntry = {

    val cmd = "pv"
    val filesInLocation = new java.io.File(logLocation).listFiles()
    var timing = new EntryExitTiming("", "", new Date(), new Date())
    filesInLocation.toList.foreach(mainFile => if (mainFile.getName.contains("ioscli")) {
      timing = parseMainFile(mainFile, cmd)
    })

    for (file <- filesInLocation) {
      val filename = file.getName()
      val nodeName = filename.split("\\.").toList.last
      val logentry = new LogEntry(filename.replaceAll(("\\." + nodeName), ""), nodeName, file.getAbsolutePath)
      parseLogEntry(logentry, timing, cmd);
    }
    new LogEntry("sucess", "success", "success")

  }

  class LogEntry(layer: String, node: String, filePath: String) extends Serializable {
    var layerFile: String = layer
    val nodeName: String = node
    val absolutePath: String = filePath
  }

  class EntryExitTiming(nodeName: String, commandName: String, entryTime: Date, exitTime: Date) extends Serializable {
    var node: String = nodeName;
    var command: String = commandName;
    var startTime: Date = entryTime;
    var endTime: Date = exitTime;
    //println(node+ " " + commandName+ " "+ entryTime+" "+ exitTime)

  }

  var layerDetailsMap = new HashMap[String,RDD[String]]()
  var nodeLvlMap = new HashMap[String,Map[String,RDD[String]]]()

  var nodeDetailsMap = new HashMap[String,RDD[String]]()
  var layerLvlMap = new HashMap[String,Map[String,RDD[String]]]()
  def LogAnalaysisDetails(errorTimeStamp: String, errorString: RDD[String], nodeName: String, layer: String){
    layerDetailsMap += (layer -> errorString)
    nodeLvlMap+= (nodeName -> layerDetailsMap)

    if(!layerLvlMap.contains(layer)){
      nodeDetailsMap += (nodeName -> errorString)
      layerLvlMap+= (layer -> nodeDetailsMap)
    }else{
      var nodeMap = layerLvlMap(layer) // (nodeName -> errorString)
      nodeMap += (nodeName -> errorString)
      layerLvlMap+= (layer -> nodeMap)
    }
  }

}