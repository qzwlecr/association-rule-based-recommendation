package AR

import math.max

abstract class RulesTree() {
  def insert(keys: List[Int], rhs: Int, conf: Double): RulesTree = {
    this match {
      case Empty =>
        keys match {
          case Nil => Leaf(rhs, conf)
          case h :: t => Node(h, conf, List(Empty.insert(t, rhs, conf)))
        }

      case Node(v, current_conf, children) =>

        val max_conf = max(current_conf, conf)
        keys match {
          case Nil => Node(v, max_conf, children ++ List(Leaf(rhs, conf)))
          case h :: tail =>
            findInList(h, Nil, children) match {
              case None =>
                val newList = List(Empty.insert(tail, rhs, conf))
                Node(v, max_conf, Node(h, conf, newList) :: children)
              case Some((left, child, right)) => Node(v, max_conf, left ++ List(child.insert(tail, rhs, conf)) ++ right)
            }
        }
      case Leaf(_, _) => throw new RuntimeException("fuck you")
    }
  }

  def findInList(target: Int, visited_l: List[RulesTree], l: List[RulesTree]): Option[(List[RulesTree], RulesTree, List[RulesTree])] = {
    l match {
      case Nil => None
      case tree :: tail => {
        tree match {
          case Node(key, _, _) if key == target => Some(visited_l, tree, tail)
          case _ => findInList(target, tree :: visited_l, tail)
        }
      }
    }
  }
  def find_helper(keys: Set[Int], found_conf: Double): Double = {
    this match {
      case Node(v, max_conf, children) if max_conf >= found_conf =>
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
  def find(keys: Set[Int]): Double = {find_helper(keys, 0.0)}
}

case class Node(key: Int, max_conf: Double, children: List[RulesTree]) extends RulesTree

case class Leaf(rhs: Int, conf: Double) extends RulesTree

case object Empty extends RulesTree