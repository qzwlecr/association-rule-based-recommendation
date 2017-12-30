import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

package AR {

  object Main {
    def main(args: Array[String]): Unit = {
      val fileInput = args(0)
      val fileOutput = args(1)
      val fileTemp = args(2)
      val conf = new SparkConf().setAppName("Association Rules")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.driver.maxResultSize", "0")

      conf.registerKryoClasses(Array(classOf[FPTree]))
      val sc = new SparkContext(conf)
      val originData = sc.textFile(fileInput + "/D.dat", 300)

      val transactions: RDD[Array[Int]] = originData.map(s => s.trim.split(' ').map(x => x.toInt)).cache()
      val model = new FPGrowth().setMinSupport(0.15).setNumPartitions(300).run(transactions)
      println(s"${model._2.count()}")

      val items = model._1.zipWithIndex.toMap
      val itemsWithFreq = model._2.map(x => (x.items.sortBy(x=>items(x)).toList, x.freq)).collect()
      val itemsWithFreqMap = itemsWithFreq.toMap

      var root: RulesTree = Node(-1, 0.0, Nil)
      for ((items, son) <- itemsWithFreq) {
        if (items.length > 1) {
          items.foreach(
            x => {
              val mother = items diff List(x)
              root = root.insert(mother, x, 1.0 * son / itemsWithFreqMap(mother))
            }
          )
        }
      }

      val tree = sc.broadcast(root)
      val userData = sc.textFile(fileInput + "/U.dat", 200)
      val users = userData.map(s => s.trim.split(' ').map(x => x.toInt)).map(x => x.intersect(model._1))
      val answer = users.map(x => tree.value.find(x.toSet)).saveAsTextFile(fileOutput + "/U.dat")

    }
  }

}
