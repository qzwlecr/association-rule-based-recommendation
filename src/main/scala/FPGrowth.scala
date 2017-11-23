package AR

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.collection.mutable

class FPGrowth(private var minSupport: Double = 0.092, private var numPartitions: Int = -1) extends Serializable {

  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  def run(data: RDD[Array[Int]]): RDD[FreqItemSet] = {
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toInt
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, minCount, partitioner)
    printf("First level of Items = %d\n",freqItems.size)
    genFreqItemsets(data, minCount, freqItems, partitioner)
  }

  private def genFreqItems(data: RDD[Array[Int]],
                           minCount: Int,
                           partitioner: Partitioner): Array[Int] = {
    data.flatMap { x => x }
      .map(v => (v, 1))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
      .map(_._1)
  }

  private def genFreqItemsets(data: RDD[Array[Int]],
                              minCount: Int,
                              freqItems: Array[Int],
                              partitioner: Partitioner): RDD[FreqItemSet] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }
      .aggregateByKey(new FPTree, partitioner.numPartitions) (
        (tree, transaction) => tree.add(transaction, 1),
        (tree1, tree2) => tree1.merge(tree2)
      )
      .flatMap {
      case (part, tree) =>
        tree.extract(minCount, x => partitioner.getPartition(x) == part)
    }
      .map {
      case (ranks, count) =>
        new FreqItemSet(ranks.map(i => freqItems(i)).toArray, count)
    }
  }

  private def genCondTransactions(transaction: Array[Int],
                                  itemToRank: Map[Int, Int],
                                  partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val answer: mutable.Map[Int, Array[Int]] = mutable.Map.empty
    val filtered = transaction.flatMap(itemToRank.get)
    java.util.Arrays.sort(filtered)
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

