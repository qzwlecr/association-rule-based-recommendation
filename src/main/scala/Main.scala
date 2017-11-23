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

    val transactions: RDD[Array[Int]] = originData.map(s => s.trim.split(' ').map(x=>x.toInt))
    val freqItems = new FPGrowth().setMinSupport(0.092).run(transactions)
    freqItems.persist()
    freqItems.saveAsTextFile(fileOutput + "/D.dat")

    printf("Count = %d", freqItems.count())

    //val itemsWithfreq = freqItems.map(s => (s.items.toString, s.freq)).persist()
  }
}
