import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class LogTree {
  val THETA1 = 0.1 // Threshold for branches [0, 1]
  val THETA2 = 0.9 // Threshold for single child nodes [0, 1]
  val THETA3 = 0.9 // threshold for multiple child nodes [0, 1]
  val THETA4 = 0.1 // Threshold for optional nodes [0, 1]
  val THETA5 = 0.9 // Threshold for nodes with few lines [0, 1]
  val THETA6 = 0.001 // Threshold for optional nodes in branches [0, 1]
  val MAX_DEPTH = 10
  val id2node: mutable.Map[Int, ParserNode] = mutable.Map()
  var sc: Option[SparkContext] = None
  var curr_node_id: Int = 0

  def connectES(nodes: String, port: String, user: String, pass: String): Unit = {
    val conf = new SparkConf().setAppName("writeEs").setMaster("local[*]")
      .set("es.nodes", nodes) // elasticsearch访问ip
      .set("es.port", port) // elasticsearch访问port
      .set("es.net.http.auth.user", user) // elasticsearch访问用户名
      .set("es.net.http.auth.pass", pass) // elasticsearch访问密码
      .set("es.nodes.wan.only", "true")
    sc = Option(new SparkContext(conf))
  }

  def buildTree(resource: String, query: String): ParserNode = {
    val esRDD = sc.get.esRDD(resource, query).cache()
    val root = ParserNode(count = esRDD.count())
    id2node += (0 -> root)

    var tokenRDD = esRDD.map { doc =>
      //      val log_line = doc._2("message").toString.split("\\s+", 3)
      val timestamp_reg = "" // 对一些日志存在的时间戳进行匹配
      val log_line = doc._2("message").toString
      val ignore_char = "{}\""
      val message_token_list = log_line.filterNot(c => ignore_char.indexOf(c) >= 0).split("[:,/#*\\s]+")
      //      (log_line(0), message_token_list.mkString("+"))
      (ListBuffer(0), message_token_list.to[ListBuffer])
    }.cache()
    //    val max_token_len = esRDD.reduce((x, y) => Math.max(x._2.length, y._2.length))
    //    val depth = 0
    curr_node_id = 1
    var new_tokenRDD = tokenRDD
    var depth = 0
    while (new_tokenRDD.count() > 0 && depth <= MAX_DEPTH){
      depth += 1
      val token2PF = calculatePF(new_tokenRDD)
      val token2node = calculateNode(token2PF)
      tokenRDD = new_tokenRDD
      // 先过滤已经结束的日志行
      //      new_tokenRDD.take(2).foreach(esTuple => println(esTuple._1 + " " + esTuple._2.mkString("_")))
      new_tokenRDD = tokenRDD.filter(x => x._2.nonEmpty && token2node.contains((x._1, x._2.head))).map {
        case (pre, rest_token_list) =>
          pre.append(token2node((pre, rest_token_list.head)))
          (pre, rest_token_list.slice(1, rest_token_list.length))
      } .cache()
      tokenRDD.unpersist()
    }
    root
  }

  def calculatePF(tokenRDD: RDD[(ListBuffer[Int], ListBuffer[String])]): mutable.Map[(ListBuffer[Int], String), (Int, Double, Long)] = {
    // 计算当前列各token的数量
    val next_token_count = tokenRDD.map{
      case (pre, tokens) =>
        var rest_tokens = tokens
        var curr_token = "*"
        if (tokens.nonEmpty) {
          curr_token = tokens.head
          rest_tokens = tokens.slice(1, tokens.length)
        } else {
          rest_tokens = tokens.slice(1, tokens.length)
        }
        ((pre, curr_token), rest_tokens)
    }.countByKey()
    // 建立之前所有节点和当前token到（父节点id,PF值,当前token数量）的映射
    val token2PF: mutable.Map[(ListBuffer[Int], String), (Int, Double, Long)] = mutable.Map()
    // 防止同一层出现的相同token搞混，将之前出现的所有token作为key
    // 计算下一列各token的PF，都过id2node(k._1.last).count获取父节点对应日志的数量
    for ((k, v) <- next_token_count) {
      token2PF+= (k -> (k._1.last, v.doubleValue() / id2node(k._1.last).count, v))
    }
    token2PF
  }

  def calculateNode(token2PF: mutable.Map[(ListBuffer[Int], String), (Int, Double, Long)]): mutable.Map[(ListBuffer[Int], String), Int] = {
    // 建立节点前缀到对应所有当前层token及其各个指标的映射
    val pre2metrics: mutable.Map[ListBuffer[Int], ListBuffer[(String, Int, Double, Long)]] = mutable.Map()
    for ((pre, token) <- token2PF.keys){
      val metrics = token2PF((pre, token))
      if (pre2metrics.contains(pre)) {
        pre2metrics(pre).append((token, metrics._1, metrics._2, metrics._3))
      } else {
        pre2metrics += (pre -> ListBuffer((token, metrics._1, metrics._2, metrics._3)))
      }
    }
    // 建立之前所有节点和当前token到token所分配节点的映射
    val token2node: mutable.Map[(ListBuffer[Int], String), Int] = mutable.Map()

    // 对每一个分支的根据PF建立下一层的Node
    for ((pre, metrics_list) <- pre2metrics){
      var sum_frequency1: Double = 0
      var sum_frequency2: Double = 0
      var sum_count:Long = 0
      val list1: ListBuffer[(ListBuffer[Int], String)] = ListBuffer()
      val list2: ListBuffer[(ListBuffer[Int], String)] = ListBuffer()
      //        val list_failed_elem: ListBuffer[(ListBuffer[Int], String)] = ListBuffer()
      for (metrics <- metrics_list) {
        if (metrics._3 > THETA1) {
          sum_frequency1 += metrics._3
          list1.append((pre, metrics._1))
        } else {
          sum_frequency2 += metrics._3
          //            list_failed_elem += k
        }
        sum_count += metrics._4
        list2.append((pre, metrics._1))
      }
      assignNode(sum_frequency1, sum_frequency2, sum_count, list1, list2, token2PF, token2node, pre)
    }
    token2node
  }

  def assignNode(sum_frequency1: Double, sum_frequency2: Double, sum_count:Long,
                 list1: ListBuffer[(ListBuffer[Int], String)], list2: ListBuffer[(ListBuffer[Int], String)],
                 token2PF: mutable.Map[(ListBuffer[Int], String), (Int, Double, Long)],
                 token2node: mutable.Map[(ListBuffer[Int], String), Int], pre: ListBuffer[Int]): Unit = {
    if (list1.isEmpty || (list1.length == 1 && token2PF(list1.head)._2 <= THETA2) || (list1.length > 1 && sum_frequency1 <= THETA3)) {
      val new_node = ParserNode(is_variable = true, node_id = curr_node_id, count = sum_count, token = "VAR")
      //        token_node_map(list1.head) = curr_node_id
      for (k <- list2) {
        token2node(k) = curr_node_id
      }
      id2node += (curr_node_id -> new_node)
      id2node(pre.last).children.append(new_node)
      curr_node_id += 1
    } else if (list1.length == 1) {
      // 如果是将要结束的日志行频率超过theta1，判断频率超过theta4认为对应节点可选，超过theta5认为对应节点直接结束
      if (list1.head._2 == "*") {
        val new_node = if (token2PF(list1.head)._2 > THETA4) {
          ParserNode(token = list1.head._2, node_id = curr_node_id, optional = true, count = token2PF(list1.head)._3)
        } else if (token2PF(list1.head)._2 > THETA5) {
          ParserNode(token = list1.head._2, node_id = curr_node_id, end = true, count = token2PF(list1.head)._3)
        } else {
          ParserNode(token = list1.head._2, node_id = curr_node_id, count = token2PF(list1.head)._3)
        }
        token2node(list1.head) = curr_node_id
        id2node += (curr_node_id -> new_node)
        id2node(token2PF(list1.head)._1).children.append(new_node)
        curr_node_id += 1
      } else {
        val new_node = ParserNode(token = list1.head._2, node_id = curr_node_id, count = token2PF(list1.head)._3)
        token2node(list1.head) = curr_node_id
        id2node += (curr_node_id -> new_node)
        id2node(token2PF(list1.head)._1).children.append(new_node)
        curr_node_id += 1
      }
    } else if (list1.length > 1) {
      for (token <- list1) {
        // 如果存在将要结束的日志行频率超过theta1，判断频率超过theta4认为对应节点可选，超过theta5认为对应节点直接结束
        if (token._2 == "*") {
          val new_node = if (token2PF(token)._2 > THETA4) {
            ParserNode(token = token._2, node_id = curr_node_id, optional = true, count = token2PF(token)._3)
          } else if (token2PF(token)._2 > THETA5) {
            ParserNode(token = token._2, node_id = curr_node_id, end = true, count = token2PF(token)._3)
          } else {
            ParserNode(token = token._2, node_id = curr_node_id, count = token2PF(token)._3)
          }
          token2node(token) = curr_node_id
          id2node += (curr_node_id -> new_node)
          id2node(token2PF(list1.head)._1).children.append(new_node)
          curr_node_id += 1
        } else {
          val new_node = ParserNode(token = token._2, node_id = curr_node_id, count = token2PF(token)._3)
          token2node(token) = curr_node_id
          id2node += (curr_node_id -> new_node)
          id2node(token2PF(list1.head)._1).children.append(new_node)
          curr_node_id += 1
        }
      }
      if (sum_frequency2 > THETA6) {}
    }
  }

  def printTree(root: ParserNode): Unit = {
    // 层序遍历树的每一层的节点及其相关信息
    val queue1 = new scala.collection.mutable.Queue[ParserNode]
    val queue2 = new scala.collection.mutable.Queue[ParserNode]
    var level: Int = 0
    queue1 += root
    while (queue1.nonEmpty || queue2.nonEmpty) {
      while(queue1.nonEmpty){
        val head = queue1.dequeue()
        if (head.children.nonEmpty){
          println(level, head.toString)
        }
        if (head.children.nonEmpty){
          for (child <- head.children){
            queue2.+= (child)
          }
        }
      }
      level += 1
      while(queue2.nonEmpty){
        val head = queue2.dequeue()
        if (head.children.nonEmpty){
          println(level, head.toString)
        }

        if (head.children.nonEmpty){
          for (child <- head.children){
            queue1.+= (child)
          }
        }
      }
      level += 1
    }
    //    // 更新RDD
    //    esRDD.take(10).foreach(esTuple => println(esTuple._1 + " " + esTuple._2.mkString("_")))
  }
}
