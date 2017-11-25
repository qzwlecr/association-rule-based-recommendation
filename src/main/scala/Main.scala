package AR

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val fileInput = args(0)
    val fileOutput = args(1)
    val fileTemp = args(2)
    val sc = new SparkContext(new SparkConf().setAppName("Association Rules"))
    //    sc.setLogLevel("WARN")
    val originData = sc.textFile(fileInput + "/D.dat", 30)
    val transactions: RDD[Array[Int]] = originData.map(s => s.trim.split(' ').map(x => x.toInt))
    //    println(s"transactions sample: ")
    //    val sample = transactions.takeSample(false, 10)
    //    for(i <- 0 to 9){
    //      for(j <- 0 to sample(i).length - 1){
    //        print(" "+sample(i)(j))
    //      }
    //      println()
    //    }
    val freqItems = new FPGrowth().setMinSupport(0.092).setNumPartitions(30).run(transactions)
    freqItems.persist()
    //    freqItems.saveAsTextFile(fileOutput + "/D.dat")

    println(s"Count = ${freqItems.count()}")

    //    val itemsWithfreq = freqItems.map(s => (s.items.toString, s.freq)).persist()
  }
}
