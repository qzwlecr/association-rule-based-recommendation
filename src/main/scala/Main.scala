package AR

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val fileInput = args(0)
    val fileOutput = args(1)
    val fileTemp = args(2)
    val sc = new SparkContext(new SparkConf().setAppName("Association Rules"))
    val originData = sc.textFile(fileInput + "/D.dat")

    val transactions: RDD[Array[String]] = originData.map(s => s.trim.split(' '))
    val model = new FPGrowth().setMinSupport(0.092).setNumPartitions(3).run(transactions)
    val freqItems = model.persist()
    val AAnswer = freqItems.sortBy(x => x.items.toString)
    AAnswer.saveAsTextFile(fileOutput + "/D.dat")
    print("Count = %d", freqItems.count())

    //val itemsWithfreq = freqItems.map(s => (s.items.toString, s.freq)).persist()
  }
}
