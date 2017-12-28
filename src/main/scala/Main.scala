import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

package AR {

  object Main {
    def main(args: Array[String]): Unit = {
      val fileInput = args(0)
      val fileOutput = args(1)
      val fileTemp = args(2)
      val sc = new SparkContext(new SparkConf().setAppName("Association Rules").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
      val originData = sc.textFile(fileInput + "/D.dat", 1800)

      val transactions: RDD[Array[Int]] = originData.map(s => s.trim.split(' ').map(x => x.toInt))
      val freqItems = new FPGrowth().setMinSupport(0.092).setNumPartitions(900).run(transactions)
      freqItems._2.saveAsTextFile(fileOutput + "/D.dat")
      val itemsWithFreq = freqItems._2.map(x => (x.items.toList, x.freq)).collect()
      val itemsWithFreqMap = itemsWithFreq.toMap
//Rules Tree Test:

      var root: RulesTree = Node(-1, 0, Nil)
//      val root2 = root1.insert(Nil,4,0.5)
//      val root3 = root2.insert(List(1,2,3),5,0.3)
//      val root4 = root3.insert(List(1,2,5),6,0.4)
//      val root5 = root4.insert(List(1,4,3),7,0.1)
//      println(root5.find(Set(1,2,3,4,5,6,7)))
//End;
      for ((items, son) <- itemsWithFreq) {
        items.foreach(
          x => {
            val mother = items diff List(x)
            root = root.insert(mother, x, 1.0 * itemsWithFreqMap(mother) / son)
          }
        )
      }

      val tree = sc.broadcast(root)
      val userData = sc.textFile(fileInput + "/U.dat", 200)

    }
  }

}
