import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

package AR {

  object Main {
    def main(args: Array[String]): Unit = {
      val fileInput = args(0)
      val fileOutput = args(1)
      val fileTemp = args(2)
      val sc = new SparkContext(new SparkConf().setAppName("Association Rules").set("spark.default.parallelism", "240"))
      val originData = sc.textFile(fileInput + "/D.dat",480)

      val transactions: RDD[Array[String]] = originData.map(s => s.trim.split(' '))
      val model = new FPGrowth().setMinSupport(0.092).setNumPartitions(480).run(transactions)
      val freqItems = model.freqItemsets.persist()

      print("Count = %d", freqItems.count())

      //val itemsWithfreq = freqItems.map(s => (s.items.toString, s.freq)).persist()
    }
  }

}
