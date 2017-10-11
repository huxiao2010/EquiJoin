import org.apache.spark.{SparkConf, SparkContext}

object SparkJoin {
  def main(args: Array[String]) {
    val start = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("Spark-Join")
    val sc = new SparkContext(conf)
    val n = 1000000
    
    val alpha = 1.2
    val directory = "/data-zipf/alpha" + alpha.toString
    val path1 = directory + "/data1-" + n.toString + ".csv"
    val path2 = directory + "/data2-" + n.toString + ".csv"

    val data1 = sc.textFile(path1).map(s => s.substring(1, s.length()-1).split(", ")).map(x => (x(0), x(1)))
    val data2 = sc.textFile(path2).map(s => s.substring(1, s.length()-1).split(", ")).map(x => (x(0), x(1)))
    val join_result = data1.join(data2)
    val count = join_result.count()
    val finish = System.currentTimeMillis()
    
    println("Spark-Join Algorithm alpha " + alpha.toString + "-" + n.toString + ": ")
    println("COUNT: " + count.toString)
    println("TIME: " + (finish - start).toString)

    sc.stop()
  }
}

