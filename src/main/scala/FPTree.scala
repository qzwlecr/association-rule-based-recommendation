package AR

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class FPTree[T] extends Serializable {

  val root: Node[T] = new Node[T](null)
  val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty
  val transaction = getTransactions(root)
  var count: Long = 0

  def merge(other: FPTree[T]): FPTree[T] = {
    if (other.count > count) {
      transaction.foreach {
        case (t, c) =>
          add(t, c)
      }
      other.count += count
      other
    } else {
      other.transaction.foreach {
        case (t, c) =>
          add(t, c)
      }
      count += other.count
      this
    }
  }

  def add(t: Iterable[T], count: Long = 1L): this.type = {
    var curr = root
    curr.count += count
    t.foreach {
      item =>
        val summary = summaries.getOrElseUpdate(item, new Summary)
        summary.count += count
        val child = curr.children.getOrElseUpdate(item, {
          var newNode = new Node(curr, item)
          summary.nodes += newNode
          newNode
        })
        child.count += count
        curr = child
    }
    this
  }

  def extract(minCount: Long, validateSuffix: T => Boolean = _ => true): Iterator[(List[T], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item) && summary.count >= minCount) {
        Iterator.single((item :: Nil, summary.count)) ++
          project(item).extract(minCount).map { case (t, c) => (item :: t, c) }
      } else {
        Iterator.empty
      }
    }

  }

  private def project(suffix: T): FPTree[T] = {
    val tree = new FPTree[T]
    if (summaries.contains(suffix)) {
      val summary = summaries(suffix)
      summary.nodes.foreach {
        node =>
          var t = List.empty[T]
          var curr = node.parent
          while (!curr.isRoot) {
            t = curr.item :: t
            curr = curr.parent
          }
          tree.add(t, node.count)
          tree.count += node.count
      }
    }
    tree
  }

  private def getTransactions(node: Node[T]): Iterator[(List[T], Long)] = {
    var count = node.count
    node.children.iterator.flatMap {
      case (item, child) =>
        getTransactions(child).map {
          case (t, c) =>
            count -= c
            (item :: t, c)
        }
    } ++ {
      if (count > 0) {
        Iterator.single(Nil, count)
      } else {
        Iterator.empty
      }
    }
  }

  class Node[T](val parent: Node[T],
                        var item: T = ???,
                        val children: mutable.Map[T, Node[T]] = mutable.Map.empty[T, Node[T]],
                        var count: Long = 0L)
    extends Serializable {

    def isRoot = parent == null

    def isLeaf = children == null
  }

  class Summary[T](val nodes: ListBuffer[Node[T]] = ListBuffer.empty[Node[T]], var count: Long = 0L) extends Serializable

}


