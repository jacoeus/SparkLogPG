import scala.collection.mutable.ListBuffer

case class ParserNode(is_variable: Boolean = false,
                      end: Boolean = false,
                      optional: Boolean = false,
                      token: String = "",
                      count: Long = 0,
                      PF: Double = 0.0,
                      node_id: Int = 0,
                      children: ListBuffer[ParserNode] = ListBuffer()) {
  override def toString: String = {
    var children_string = ""
    for (child <- children) {
      children_string += " " + child.node_id.toString
    }
    s"""Node:$node_id $token $count [$children_string ]"""
  }

  override def hashCode(): Int = node_id
}