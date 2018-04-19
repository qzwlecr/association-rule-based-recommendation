package AR {

  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.{SparkConf, SparkContext}

  object Main {
    def main(args: Array[String]): Unit = {
      val (fileInput, fileOutput, fileTemp) =
        if (args.length == 0)
          ("data/input", "data/output", "data/tmp")
        else
          (args(0), args(1), args(2))

      val conf = new SparkConf().setAppName("Association Rules")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.network.timeout", "3000")
        .set("spark.kryoserializer.buffer.max", "2047m")
        .set("spark.executor.extraJavaOptions", "-XX:ThreadStackSize=2048 -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+CMSParallelRemarkEnabled -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75")
        .set("spark.hadoop.validateOutputSpecs", "false")
        .set("spark.local.dir", "/tmp")
        .registerKryoClasses(Array(classOf[FPTree], classOf[FPGrowth], classOf[FPTreeMap], classOf[RFPTree]))
      val sc = new SparkContext(conf)
      sc.setCheckpointDir(fileTemp)
      val originData = sc.textFile(fileInput + "/D.dat", 576)

      val transactions = originData.map {
        _.trim
          .split(' ')
          .map(_.toInt)
      }.persist(StorageLevel.MEMORY_AND_DISK_SER)

      transactions.checkpoint()

      val model = new FPGrowth()
        .setMinSupport(0.092)
        .run(transactions)

      model.freqItemsets.map {
        _.items.reverse
      }.sortBy(x => x).map{
        _.mkString(" ")
      }.saveAsTextFile(fileOutput + "/D.dat")

      val itemsWithFreq = model.freqItemsets.map(
        x => (x.items.toList, x.freq)
      ).collect()
      val itemsWithFreqMap = itemsWithFreq.toMap

      var root: RulesTree = RuleNode(0, 0.0, Nil)
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
      val userData = sc.textFile(fileInput + "/U.dat")
      val users = userData.map {
        _.trim
          .split(' ')
          .map(_.toInt)
          .intersect(model.freqItems)
      }
      users.map(
        eachUser => tree.value.find(eachUser.toSet)._1
      ).saveAsTextFile(fileOutput + "/U.dat")
    }
  }

}
