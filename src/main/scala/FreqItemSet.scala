package AR

class FreqItemSet[Item](val items: Array[Item], val freq: Long) extends Serializable {
  override def toString: String = {
    s"${items.mkString("{", ",", ")")}: $freq"
  }
}
