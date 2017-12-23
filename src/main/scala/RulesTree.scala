
import scala.collection.immutable.Nil

abstract class RulesTree() {
  def insert(keys: List[Int], rhs: Int, conf: Double): RulesTree = {
    this match {
      case Empty() =>
        keys match {
          case Nil => Leaf(rhs, conf)
          case h :: t => Node(None, List((h, Empty().insert(t, rhs, conf))))
        }
      case Node(v, children) =>
        keys match {
          case Nil => Node(v, children ++ List((rhs, Leaf(rhs, conf))))
          case h :: t =>
            val child = findInList(h, children)
            child match {
              case Empty() => Node(v, children ++ List((h, child.insert(t, rhs, conf))))
              case _ => Node(v, (children diff List(h, child)) ++ List((h, child.insert(t, rhs, conf))))
            }
        }
    }
  }

  def findInList(key: Int, l: List[(Int, RulesTree)]): RulesTree = {
    l match {
      case Nil => Empty()
      case (`key`, t) :: _ => t
      case _ :: t => findInList(key, t)
    }
  }

  def find(keys: Set[Int]):Double = {
    this match{
      case Empty() => 0.0
      case Node(v, children) =>
      v match {
        case Some(key) if keys.contains(key) => children.aggregate(0.0)(
          (last,child)=>math.max(last,child._2.find(keys)),
          math.max
        )
        case None => children.aggregate(0.0)(
          (last,child)=>math.max(last,child._2.find(keys)),
          math.max
        )
      }
      case Leaf(v, conf) if !keys.contains(v) => conf
    }
  }
}

case class Node(key: Option[Int], children: List[(Int, RulesTree)]) extends RulesTree

case class Leaf(rhs: Int, conf: Double) extends RulesTree

case class Empty() extends RulesTree
