package AR

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class FPTree extends Serializable {

  private val root: Node = new Node(null)

  private val summaries: mutable.Map[Int, Summary] = mutable.Map.empty

  def transaction: Iterator[(List[Int], Int)] = getTransactions(root)

  def merge(other: FPTree): FPTree = {
    other.transaction.foreach { case (t, c) =>
      add(t, c)
    }
    this
  }

  def add(t: Iterable[Int], count: Int = 1): FPTree = {
    var curr = root
    curr.count += count
    t.foreach { item =>
      val summary = summaries.getOrElseUpdate(item, new Summary)
      summary.count += count
      val child = curr.children.getOrElseUpdate(item, {
        val newNode = new Node(curr, item)
        summary.nodes += newNode
        newNode
      })
      child.count += count
      curr = child
    }
    this
  }

  def extract(minCount: Int, validateSuffix: Int => Boolean = _ => true): Iterator[(List[Int], Int)] = {
    summaries.iterator.flatMap {
      case (item, summary) =>
        if (validateSuffix(item) && summary.count >= minCount) {
          Iterator.single((item :: Nil, summary.count)) ++
            project(item).extract(minCount).map {
              case (t, c) =>
                (item :: t, c)
            }
        } else {
          Iterator.empty
        }
    }

  }

  private def project(suffix: Int): FPTree = {
    val tree = new FPTree
    if (summaries.contains(suffix)) {
      val summary = summaries(suffix)
      summary.nodes.foreach {
        node =>
          var t = List.empty[Int]
          var curr = node.parent
          while (curr.parent != null) {
            t = curr.item :: t
            curr = curr.parent
          }
          tree.add(t, node.count)
      }
    }
    tree
  }

  private def getTransactions(node: Node): Iterator[(List[Int], Int)] = {
    var count = node.count
    node.children.iterator.flatMap { case (item, child) =>
      getTransactions(child).map { case (t, c) =>
        count -= c
        (item :: t, c)
      }
    } ++ {
      if (count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }

  class Node(val parent: Node,
             var item: Int = 0,
             val children: mutable.Map[Int, Node] = mutable.Map.empty[Int, Node],
             var count: Int = 0) extends Serializable

  class Summary(val nodes: ListBuffer[Node] = ListBuffer.empty[Node], var count: Int = 0) extends Serializable

}


