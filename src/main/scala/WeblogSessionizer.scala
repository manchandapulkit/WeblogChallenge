import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.nio.file.{Paths, Files}
import scala.collection.mutable.ListBuffer

object WebLog_Session {
  
private val sessionInterval = 900000L //setting the session interval for 15 mins = 900000 ms

 def main(args: Array[String]):Unit = {
  if(args.size == 0) {
    	println("FilePath Argument missing!")   //checking if the arguments is passed
  	  System.exit(1)
  }
  //val thefilePath = "/Users/pulkit/server/data/2015_07_22_mktplace_shop_web_log_sample.log"
  val thefilePath = args(0)  
  if ( !Files.exists(Paths.get(thefilePath)) ) {
  	println("The file " + thefilePath + " does not exist." ) // if the path is correct
  	System.exit(1)
  }

  val sparkConf = new SparkConf().setAppName("WeblogSessionizer").setMaster("local") 
  val sparkContext = new SparkContext(sparkConf) //Initializing the spark context
  val textFile = sparkContext.textFile(thefilePath)
  
  /*creating a RDD of the logfile and 
   * taking only two fields clientIp and timestamp (in milliseconds)*/
  
  val ipTimestampRDD = textFile.map { lines => 
    val line = lines.split(" ")
    val timestamp = line(0)
    val clientIp = line(2)
    	(clientIp.substring(0, clientIp.indexOf(":")), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    .parse(timestamp.substring(0, timestamp.indexOf('.')).replace('T', ' ')).getTime)
		}
 
  /* sorting and grouping the RDD on timestamp and clientIP*/
  
  //val sortAndgroupedRDD= ipTimestampRDD.sortBy(_._2).groupByKey
	val sortAndgroupedRDD= ipTimestampRDD.sortBy(_._2).groupByKey
  
  /*calling the method getSessionHitsAndTime on each row of the rdd
   * the method returns List[(String,Long)] i.e list of maps 
   * The returned RDD of List is flattened to RDD of (Strig,Long)
   */
  val resultRDD = sortAndgroupedRDD.map(x=>
    getSessionHitsAndTime(x._1,x._2))
    .flatMap(x=>x.toList)

	 //persisting the RDD as certain operations will be performed
  resultRDD.persist()

  //Tasks
	//Determine the average session time
  val noOfSessions = resultRDD.count
  val totalSessionTime = resultRDD.values.sum()
  val avgSessionTime=totalSessionTime / (noOfSessions *60 *1000.0)
	
  /*Determine unique URL visits per session.
   * To clarify, count a hit to a unique URL only once per session.
   */
  val noOfUniqueVisits= resultRDD.filter(x=>x._2==0L).count()
	
  //Find the most engaged users, ie the IPs with the longest session times
  val mostEngagedUsers= resultRDD.sortBy(x=>x._2, false, 4).take(10)
  
  
  println(f"The average session time(minutes) = $avgSessionTime%2.2f")
  println(s"The no Of Unique visits = $noOfUniqueVisits")
  println("The most engaged users : " )
  mostEngagedUsers.foreach(x=> println(x))
  resultRDD.unpersist()
  sparkContext.stop()
  }


/*This  method is being called on every row of the RDD
 * each row has a client IP and Iterable/group of the timestamps
 * the list of timestamps is traversed and the sessions are 
 * calculated based on the timestamps.
 * If the difference between the interval of first hit and second hit is 
 * more than 15 minutes, a new session is counted.
 * the session hits and time are put in a list of maps 
 * and is returned to form another RDD.
 */
  def getSessionHitsAndTime(clientIp:String,timestampLst: Iterable [Long])  = {
  
   var timestampList = (timestampLst.toBuffer += Long.MaxValue) 
   var listOfMap= new ListBuffer[(String,Long)]
    if (timestampList.isEmpty) {
    }
    //var sessionMap:LinkedHashMap[Long,Long]= LinkedHashMap()
    if (timestampLst.size == 1 ) {
      
    }else if (timestampLst.size == 2) {		
      
      listOfMap += (clientIp + "-" + timestampLst.head -> 0L)
    }else {
      var head = timestampLst.head
      for (List(left,right) <- timestampLst.sliding(2)) {
		    if (right - left > sessionInterval) {
		      listOfMap += (clientIp + "-" + left -> (left - head)) 
        head = right
      }
    }
  }
listOfMap
 }
}
