package AR

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.collection.mutable
import scala.reflect.ClassTag

class FPGrowth(private var minSupport: Double, private var numPartitions: Int) extends Serializable {

  def this() = this(0.092, -1)

  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  def run[Item: ClassTag](data: RDD[Array[Item]]): RDD[FreqItemSet[Item]] = {
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, minCount, partitioner)
    genFreqItemsets(data, minCount, freqItems, partitioner)
  }

  private def genFreqItems[Item: ClassTag](data: RDD[Array[Item]],
                                           minCount: Long,
                                           partitioner: Partitioner): Array[Item] = {
    data.flatMap { x => x }
      .map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .collect()
      .sortBy(-_._2)
      .map(_._1)
  }

  private def genFreqItemsets[Item: ClassTag](data: RDD[Array[Item]],
                                              minCount: Long,
                                              freqItems: Array[Item],
                                              partitioner: Partitioner): RDD[FreqItemSet[Item]] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    val condTransactions = data.flatMap(transaction => genCondTransactions(transaction, itemToRank, partitioner))
    val tree = condTransactions.aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2))
    tree.flatMap { case (part, tree) => tree.extract(minCount, x => partitioner.getPartition(x) == part) }
      .map { case (ranks, count) =>
        new FreqItemSet[Item](ranks.map(i => freqItems(i)).toArray, count)
      }
  }

  private def genCondTransactions[Item: ClassTag](transaction: Array[Item],
                                                  itemToRank: Map[Item, Int],
                                                  partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val answer: mutable.Map[Int, Array[Int]] = mutable.Map.empty
    val filtered = transaction.flatMap(itemToRank.get)
    scala.util.Sorting.quickSort(filtered)
    for (i <- 0 until filtered.length by -1) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!answer.contains(part)) {
        answer(part) = filtered.slice(0, i + 1)
      }
    }
    answer
  }
}


