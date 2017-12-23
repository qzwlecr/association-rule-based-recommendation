import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

package AR {

  object Main {
    def main(args: Array[String]): Unit = {
      val fileInput = args(0)
      val fileOutput = args(1)
      val fileTemp = args(2)
      val sc = new SparkContext(new SparkConf().setAppName("Association Rules"))
      val originData = sc.textFile(fileInput + "/D.dat",1800)

      val transactions: RDD[Array[Int]] = originData.map(s => s.trim.split(' ').map(x =>x.toInt))
      val model = new FPGrowth().setMinSupport(0.092).setNumPartitions(300).run(transactions)
      val freqItems = model.freqItemsets
      freqItems.saveAsTextFile(fileOutput+"/D.dat")

      //val itemsWithfreq = freqItems.map(s => (s.items.toString, s.freq)).persist()
    }
  }

}
