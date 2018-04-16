//import org.apache.spark.rdd.RDD
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf

package VR {

  import com.fasterxml.jackson.databind.`type`.ArrayType
  import org.apache.spark.sql.types.IntegerType

  object Test {
    def main(args: Array[String]): Unit = {
//      val fileInput = args(0)
//      val fileOutput = args(1)
//      val fileTemp = args(2)
      val (fileInput, fileOutput, fileTemp) = if (args.length == 0)  ("data/input", "data/output", "/tmp") else (args(0), args(1), args(2))

      val conf = new SparkConf().setAppName("Association Rules").setMaster("local[2]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.network.timeout", "3000")
//      val sc = new SparkContext(conf)
//      val originData = sc.textFile(fileInput + "/D.dat", 18)
      val spark = SparkSession.builder().config(conf).getOrCreate()
      import spark.implicits._

      val rawDataFull = spark.read.textFile(fileInput + "/D.dat")

      val transactions = rawDataFull
        .sample(false, 0.001, 810L)
        .map(s => s.trim.split(' ').map(x => x.toInt))
        .toDF("items")
//      val transactionsFull: RDD[Array[Int]] = originData.map(s => s.trim.split(' ').map(x => x.toInt))
//      val transactions:RDD[Array[Int]] = transactionsFull.sample(false, 0.001, 810L).cache()
      val model = new FPGrowth()
        .setMinSupport(0.092).setNumPartitions(9).fit(transactions)
      val tmp = model.freqItemsets.cache()
      val hh = tmp.map(row=>row(0))

      println("fuck")
      println(tmp.first()(0))
//      println(tmp.first()(0).asInstanceOf[Array[Integer]])
      println(tmp.dtypes(0))
      println(tmp.dtypes(1))
      println("fuck")
    //  println(tmp.first()(1).isInstanceOf[Long])
    //  model.freqItemsets.map(x => x.items.reverse.mkString(" "))
    //   .sortBy(x => x).saveAsTextFile(fileOutput + "/freqList_sample.dat")

    //  val answerData = sc.textFile(fileInput + "/D-answer.dat").map(x=>x.trim.split(" ").map(x=>x.toInt)).map(x=>(x.take(x.length-1),x.last))
    //  val freqItemss = sc.textFile(fileInput + "/freq.dat").collect().map(x=>x.trim.split(" ").map(x=>x.toInt))
    //  val freqItems = freqItemss(0)
    //  val itemsWithFreq = model.freqItemsets.map(x => (x.items.toList, x.freq)).collect()
    //  val itemsWithFreqMap = itemsWithFreq.toMap
    //  var root: RulesTree = RuleNode(0, 0.0, Nil)
    //  for ((items, son) <- itemsWithFreq) {
    //    if (items.length > 1) {
    //      items.foreach(
    //        x => {
    //          val mother = items diff List(x)
    //          root = root.insert(mother, x, 1.0 * son / itemsWithFreqMap(mother))
    //        }
    //      )
    //    }
    //  }
    //  val tree = sc.broadcast(root)
    //  val userData = sc.textFile(fileInput + "/U.dat", 300)
    //  val users = userData.map(s => s.trim.split(' ').map(x => x.toInt)).map(x => x.intersect(model.freqItems))
     users.map(x => tree.value.find(x.toSet)._1).saveAsTextFile(fileOutput + "/U.dat")
    }
  }
}
