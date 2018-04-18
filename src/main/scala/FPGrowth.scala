/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package AR

import java.{util => ju}

import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth._
import org.apache.spark.rdd.RDD
//import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner}


import scala.collection.JavaConverters._
import scala.collection.mutable
//import scala.reflect.ClassTag

/**
  * Model trained by [[FPGrowth]], which holds frequent itemsets.
  *
  * @param freqItemsets frequent itemset, which is an RDD of `FreqItemset`
  */


class FPGrowthModel(
    val freqItemsets: RDD[FreqItemset[Int]],
    val freqItems: Array[Int])
  extends Serializable {
  /**
    * Generates association rules for the `Item`s in [[freqItemsets]].
    *
    * @param confidence minimal confidence of the rules produced
    */

  def generateAssociationRules(confidence: Double): RDD[AssociationRules.Rule[Int]] = {
    val associationRules = new AssociationRules()
    associationRules.run(freqItemsets)
  }
}


/**
  * A parallel FP-growth algorithm to mine frequent itemsets. The algorithm is described in
  * <a href="http://dx.doi.org/10.1145/1454008.1454027">Li et al., PFP: Parallel FP-Growth for Query
  * Recommendation</a>. PFP distributes computation in such a way that each worker executes an
  * independent group of mining tasks. The FP-Growth algorithm is described in
  * <a href="http://dx.doi.org/10.1145/335191.335372">Han et al., Mining frequent patterns without
  * candidate generation</a>.
  *
  * @param minSupport    the minimal support level of the frequent pattern, any pattern that appears
  *                      more than (minSupport * size-of-the-dataset) times will be output
  * @param numPartitions number of partitions used by parallel FP-growth
  * @see <a href="http://en.wikipedia.org/wiki/Association_rule_learning">
  *      Association rule learning (Wikipedia)</a>
  *
  */
class FPGrowth private(
                        private var minSupport: Double,
                        private var numPartitions: Int) extends Serializable {

  /**
    * Constructs a default instance with default parameters {minSupport: `0.3`, numPartitions: same
    * as the input data}.
    *
    */
  def this() = this(0.3, -1)

  /**
    * Sets the minimal support level (default: `0.3`).
    *
    */
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0.0 && minSupport <= 1.0,
      s"Minimal support level must be in range [0, 1] but got $minSupport")
    this.minSupport = minSupport
    this
  }

  /**
    * Sets the number of partitions used by parallel FP-growth (default: same as input data).
    *
    */
  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got $numPartitions")
    this.numPartitions = numPartitions
    this
  }


  /**
    * Computes an FP-Growth model that contains frequent itemsets.
    *
    * @param data input data set, each element contains a transaction
    * @return an [[FPGrowthModel]]
    *
    */
  def run(data: RDD[Array[Int]]): FPGrowthModel = {

    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, minCount, partitioner)
    val freqItemsets = genFreqItemsets(data, minCount, freqItems, partitioner)
    // core calculation
    new FPGrowthModel(freqItemsets, freqItems)
  }

  /**
    * Generates frequent items by filtering the input data using minimal support level.
    *
    * @param minCount    minimum count for frequent itemsets
    * @param partitioner partitioner used to distribute items
    * @return array of frequent pattern ordered by their frequencies
    */
  private def genFreqItems(
                            data: RDD[Array[Int]],
                            minCount: Long,
                            partitioner: Partitioner): Array[Int] = {
    data.flatMap { t => t }
      .map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
      .map(_._1)
  }

  /**
    * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
    *
    * @param data        transactions
    * @param minCount    minimum count for frequent itemsets
    * @param freqItems   frequent items
    * @param partitioner partitioner used to distribute transactions
    * @return an RDD of (frequent itemset, count)
    */
  private def genFreqItemsets(
                               data: RDD[Array[Int]],
                               minCount: Long,
                               freqItems: Array[Int],
                               partitioner: Partitioner): RDD[FreqItemset[Int]] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }
//      .aggregateByKey(new FPTreeMap, partitioner.numPartitions)(
      .aggregateByKey(new FPTree, partitioner.numPartitions)(
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2))
//      .map{x => (x._1, x._2.toFPTree)}
      .flatMap { case (part, tree) =>
        tree.extract(minCount, x => partitioner.getPartition(x) == part)
      }.map { case (ranks, count) =>
      new FreqItemset(ranks.sorted.map(i => freqItems(i)).toArray, count)
    }
  }

  /**
    * Generates conditional transactions.
    *
    * @param transaction a transaction
    * @param itemToRank  map from item to their rank
    * @param partitioner partitioner used to distribute transactions
    * @return a map of (target partition, conditional transaction)
    */
  private def genCondTransactions(
                                                   transaction: Array[Int],
                                                   itemToRank: Map[Int, Int],
                                                   partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.flatMap(itemToRank.get)
    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }
}

object FPGrowth {

  /**
    * Frequent itemset.
    *
    * @param items items in this itemset.
    *              Java users should call `FreqItemset.javaItems` instead.
    * @param freq  frequency
    * @tparam Item item type
    *
    */
  class FreqItemset[Item](
                           val items: Array[Item],
                           val freq: Long) extends Serializable {
    /**
      * Returns items in a Java List.
      *
      */
    def javaItems: java.util.List[Item] = {
      items.toList.asJava
    }
    override def toString: String = {
      s"${items.mkString("{", ",", "}")}: $freq"
    }
  }
}

