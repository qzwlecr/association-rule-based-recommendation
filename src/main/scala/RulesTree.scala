package AR

import scala.math.max

abstract class RulesTree() {
  type Goods = (Int, Double)

  def insert_helper(keys: List[Int], good: Goods): RulesTree = {
    val (rhs, conf) = good
    this match {
      case Empty =>
        keys match {
          case Nil => RuleLeaf(rhs, conf)
          case h :: t => RuleNode(h, conf, List(Empty.insert_helper(t, good)))
        }
      case RuleNode(v, current_conf, children) =>
        val max_conf = max(current_conf, conf)
        keys match {
          case Nil => RuleNode(v, max_conf, node_insert_to_list(children, RuleLeaf(rhs, conf)))
          case head :: tail =>
            find_in_list(head, Nil, children) match {
              case None =>
                val newTree = RuleNode(head, max_conf, Nil).insert_helper(tail, good)
                RuleNode(v, max_conf, node_insert_to_list(children, newTree))
              case Some((left, child, right)) =>
                val newTree = child.insert_helper(tail, good)
                RuleNode(v, max_conf, node_insert_to_list(left.reverse ++ right, newTree))
            }
        }
      //case Leaf(_, _) => throw new RuntimeException("fuck you")
    }
  }

  def insert(keys: List[Int], rhs: Int, conf: Double): RulesTree = {
    insert_helper(keys, (rhs, conf))
  }

  def node_insert_to_list(sorted: List[RulesTree], item: RulesTree): List[RulesTree] = {
    def get_conf = (tree: RulesTree) => tree match {
      case RuleNode(_, cur_conf, _) => cur_conf
      case RuleLeaf(_, _) => 1 // search Leaves first
      //case _ => throw new RuntimeException("fuck you again")
    }

    val item_conf = get_conf(item)
    sorted match {
      case head :: tail if get_conf(head) > item_conf => head :: node_insert_to_list(tail, item)
      case others => item :: others
    }
  }

  def find_in_list(target: Int, visited_l: List[RulesTree], l: List[RulesTree])
  : Option[(List[RulesTree], RulesTree, List[RulesTree])] = {
    l match {
      case Nil => None
      case tree :: tail => {
        tree match {
          case RuleNode(key, _, _) if key == target => Some(visited_l, tree, tail)
          case _ => find_in_list(target, tree :: visited_l, tail)
        }
      }
    }
  }

  def find_helper(keys: Set[Int], found: Goods): Goods = {
    val (_, found_conf) = found
    val good_max = (a: Goods, b: Goods) => if (if (a._2 == b._2) a._1 < b._1 else a._2 > b._2 ) a else b
    this match {
      case RuleNode(v, max_conf, children) if max_conf >= found_conf =>
        v match {
          case key if key == 0 || keys.contains(key) => children.aggregate(found)(
            (last, child) => child.find_helper(keys, last),
            good_max
          )
          case _ => found
        }
      case RuleLeaf(v, conf) if !keys.contains(v) => good_max(found, (v, conf))
      case _ => found
    }
  }

  def find(keys: Set[Int]): Goods = {
    find_helper(keys, (0, 0.0))
  }
}

case class RuleNode(key: Int, max_conf: Double, children: List[RulesTree]) extends RulesTree

case class RuleLeaf(rhs: Int, conf: Double) extends RulesTree

case object Empty extends RulesTree