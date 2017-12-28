package AR

class FreqItemSet(val items: Array[Int], val freq: Int) extends Serializable {
  override def toString: String = {
    s"${items.mkString("{", ",", ")")}: $freq"
  }
}
