object SparkLogPG {
  def main(args: Array[String]): Unit = {
    val logTree = new LogTree
    logTree.connectES("168.168.168.69", "3127", "elastic", "aJihEzvRQk6BDbxrWEmA")
    println("connect to elasticsearch successfully")
    val query: String =
      s"""
         |{
         |    "query": {
         |        "match": {
         |            "kubernetes.pod.uid": "21415b50-831d-4c02-9819-88d4066c64af"
         |        }
         |    }
         |
         |}
         |""".stripMargin
    println("Start build tree")
    val root: ParserNode = logTree.buildTree("filebeat-*/", query)
    println("Start print tree")
    logTree.printTree(root)
  }
}