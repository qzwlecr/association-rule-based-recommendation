package AR

abstract class RulesTree() {
  def insert(keys: List[Int], rhs: Int, conf: Double): RulesTree = {
    this match {
      case Empty =>
        keys match {
          case Nil => Leaf(rhs, conf)
          case h :: t => Node(h, List(Empty.insert(t, rhs, conf)))
        }

      case Node(v, children) =>
        keys match {
          case Nil => Node(v, children ++ List(Leaf(rhs, conf)))
          case h :: tail =>
            findInList(h, Nil, children) match {
              case None =>
                val newList = List(Empty.insert(tail, rhs, conf))
                Node(v, Node(h, newList) :: children)
              case Some((left, child, right)) => Node(v, left ++ List(child.insert(tail, rhs, conf)) ++ right)
            }
        }
      case Leaf(_, _) => throw new RuntimeException("fuck you")
    }
  }

  def findInList(target: Int, visited_l: List[RulesTree], l: List[RulesTree]): Option[(List[RulesTree], RulesTree, List[RulesTree])] = {
    l match {
      case Nil => None
      case Node(key, children) :: tail =>
        println("cmp", key, target)
        if (key == target)
          Some(visited_l, Node(key, children), tail)
        else
          findInList(target, Node(key, children) :: visited_l, tail)
      case t :: tail => findInList(target, t :: visited_l, tail)
    }
  }

  def find(keys: Set[Int]): Double = {
    this match {
      case Node(v, children) =>
          v match {
            case key if key == -1 || keys.contains(key) => children.aggregate(0.0)(
              (last, child) => math.max(last, child.find(keys)),
              math.max
            )
            case _ => 0.0
          }
      case Leaf(v, conf) if !keys.contains(v) => conf
      case _ => 0.0
    }
  }
}

case class Node(key: Int, children: List[RulesTree]) extends RulesTree

case class Leaf(rhs: Int, conf: Double) extends RulesTree

case object Empty extends RulesTree