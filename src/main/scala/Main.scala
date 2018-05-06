import org.apache.spark.{SparkConf, SparkContext}
package AR {

  import org.apache.spark.storage.StorageLevel

  object Main {
    def main(args: Array[String]): Unit = {
      val fileInput = "data/input"
      val fileOutput = "data/output"
      val fileTemp = "data/tmp"
      val conf = new SparkConf().setAppName("Association Rules")
        .setMaster("local[2]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.network.timeout", "3000")
        .set("spark.kryoserializer.buffer.max", "2047m")
        .set("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:ThreadStackSize=2048 -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+CMSParallelRemarkEnabled -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75")
      conf.registerKryoClasses(Array(classOf[FPTree], classOf[FPGrowth]))
      val sc = new SparkContext(conf)
      val originData = sc.textFile(fileInput + "/D.dat").sample(false, 0.02, 810L)

      val transactions = originData.map(
        _.trim.split(' ').map(_.toInt)
      ).cache()
      val model = new FPGrowth()
        .setMinSupport(0.092)
        .run(transactions)
      model.freqItemsets.map(
        _.items.reverse.mkString(" ")
      ).sortBy(x => x)
        .saveAsTextFile(fileOutput + "/D.dat")
      //val answerData = sc.textFile(fileInput + "/D-answer.dat").map(x=>x.trim.split(" ").map(x=>x.toInt)).map(x=>(x.take(x.length-1),x.last))
      //val freqItemss = sc.textFile(fileInput + "/freq.dat").collect().map(x=>x.trim.split(" ").map(x=>x.toInt))
      //val freqItems = freqItemss(0)
//      val itemsWithFreq = model.freqItemsets.map(
//        x => (x.items.toList, x.freq)
//      ).collect()
//      val itemsWithFreqMap = itemsWithFreq.toMap
//
//      var root: RulesTree = RuleNode(0, 0.0, Nil)
//      for ((items, son) <- itemsWithFreq) {
//        if (items.length > 1) {
//          items.foreach(
//            x => {
//              val mother = items diff List(x)
//              root = root.insert(mother, x, 1.0 * son / itemsWithFreqMap(mother))
//            }
//          )
//        }
//      }
//
//      val tree = sc.broadcast(root)
//      val userData = sc.textFile(fileInput + "/U.dat")
//      val users = userData.map(
//        _.trim.split(' ').map(_.toInt)
//      ).map(
//        _.intersect(model.freqItems)
//      )
//      users.map(
//        x => tree.value.find(x.toSet)._1
//      ).saveAsTextFile(fileOutput + "/U.dat")
//
    }
  }

}
