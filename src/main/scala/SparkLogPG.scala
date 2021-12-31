import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

object SparkLogPG {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("writeEs").setMaster("local[*]")
      .set("es.nodes", "168.168.168.69") // elasticsearch访问ip
      .set("es.port", "5626") // elasticsearch访问port
      .set("es.net.http.auth.user", "elastic") // elasticsearch访问用户名
      .set("es.net.http.auth.pass", "3dpzlHS9slaGhhulo2sN") // elasticsearch访问密码
      .set("es.nodes.wan.only", "true")
    val sc = new SparkContext(conf)
    val query =
      """
        |{
        |    "query": {
        |        "match": {
        |            "kubernetes.pod.uid": "21415b50-831d-4c02-9819-88d4066c64af"
        |        }
        |    }
        |}
        |""".stripMargin
    val theta1 = 0.1 // Threshold for branches [0, 1]
    val theta2 = 0.9 // Threshold for single child nodes [0, 1]
    val theta3 = 0.9 // threshold for multiple child nodes [0, 1]
    val theta4 = 0.1 // Threshold for optional nodes [0, 1]
    val theta5 = 0.9 // Threshold for nodes with few lines [0, 1]
    val theta6 = 0.001 // Threshold for optional nodes in branches [0, 1]
    val esRDD = sc.esRDD("filebeat-*/", query).cache()
    val root_count = esRDD.count()
    val root = ParserNode(count = root_count)
    val id2node: mutable.Map[Int, ParserNode] = mutable.Map(0 -> root)

    var tokenRDD = esRDD.map { doc =>
      //      val log_line = doc._2("message").toString.split("\\s+", 3)
      val timestamp_reg = "" // 对一些日志存在的时间戳进行匹配
      val log_line = doc._2("message").toString
      val ignore_char = "{}\""
      val message_token_list = log_line.filterNot(c => ignore_char.indexOf(c) >= 0).split("[:,/#*\\s]+")
      //      (log_line(0), message_token_list.mkString("+"))
      (ListBuffer(0), message_token_list.to[ListBuffer])
    }.cache()
    esRDD.unpersist()
    //    val max_token_len = esRDD.reduce((x, y) => Math.max(x._2.length, y._2.length))
    //    val depth = 0
    var curr_node_id = 1
    var new_tokenRDD = tokenRDD
    var flag = true
    val max_depth = 10
    var depth = 0
    while (flag && depth <= max_depth){
      depth += 1
      // 建立之前所有节点和当前token到（父节点id,PF值,当前token数量）的映射
      val token_PF_map: mutable.Map[(ListBuffer[Int], String), (Int, Double, Long)] = mutable.Map()
      // 计算当前列各token的数量
      val next_token_count = new_tokenRDD.map{
        case (pre, token_list) =>
          var rest_token_list = token_list
          var curr_token = "*"
          if (token_list.nonEmpty) {
            curr_token = token_list.head
            rest_token_list = token_list.slice(1, token_list.length)
          } else {
            rest_token_list = token_list.slice(1, token_list.length)
          }
          ((pre, curr_token), rest_token_list)
      }.countByKey()
      // 防止同一层出现的相同token搞混，将之前出现的所有token作为key
      // 计算下一列各token的PF，都过id2node(k._1.last).count获取父节点对应日志的数量
      for ((k, v) <- next_token_count) {
        token_PF_map += (k -> (k._1.last, v.doubleValue() / id2node(k._1.last).count, v))
      }
      // 建立之前所有节点和当前token到token所分配节点的映射
      val token_node_map: mutable.Map[(ListBuffer[Int], String), Int] = mutable.Map()
      // 建立节点前缀到对应所有当前层token及其各个指标的映射
      val pre_list_map: mutable.Map[ListBuffer[Int], ListBuffer[(String, Int, Double, Long)]] = mutable.Map()
      for ((pre, token) <- token_PF_map.keys){
        val metrics = token_PF_map((pre, token))
        if (pre_list_map.contains(pre)) {
          pre_list_map(pre).append((token, metrics._1, metrics._2, metrics._3))
        } else {
          pre_list_map += (pre -> ListBuffer((token, metrics._1, metrics._2, metrics._3)))
        }
      }
      // 对每一个分支的根据PF建立下一层的Node
      for ((pre, metrics_list) <- pre_list_map){
        var sum_frequency1: Double = 0
        var sum_frequency2: Double = 0
        var sum_count:Long = 0
        val list1: ListBuffer[(ListBuffer[Int], String)] = ListBuffer()
        val list2: ListBuffer[(ListBuffer[Int], String)] = ListBuffer()
        //        val list_failed_elem: ListBuffer[(ListBuffer[Int], String)] = ListBuffer()
        for (metrics <- metrics_list) {
          if (metrics._3 > theta1) {
            sum_frequency1 += metrics._3
            list1.append((pre, metrics._1))
          } else {
            sum_frequency2 += metrics._3
            //            list_failed_elem += k
          }
          sum_count += metrics._4
          list2.append((pre, metrics._1))
        }

        if (list1.isEmpty || (list1.length == 1 &&  token_PF_map(list1.head)._2 <= theta2) || (list1.length > 1 && sum_frequency1 <= theta3)) {
          val new_node = ParserNode(is_variable = true, node_id = curr_node_id, count = sum_count, token = "VAR")
          //        token_node_map(list1.head) = curr_node_id
          for (k <- list2) {
            token_node_map(k) = curr_node_id
          }
          id2node += (curr_node_id -> new_node)
          id2node(pre.last).children.append(new_node)
          curr_node_id += 1
        } else if (list1.length == 1) {
          // 如果是将要结束的日志行频率超过theta1，判断频率超过theta4认为对应节点可选，超过theta5认为对应节点直接结束
          if (list1.head._2 == "*") {
            val new_node = if (token_PF_map(list1.head)._2 > theta4) {
              ParserNode(token = list1.head._2, node_id = curr_node_id, optional = true, count = token_PF_map(list1.head)._3)
            } else if (token_PF_map(list1.head)._2 > theta5) {
              ParserNode(token = list1.head._2, node_id = curr_node_id, end = true, count = token_PF_map(list1.head)._3)
            } else {
              ParserNode(token = list1.head._2, node_id = curr_node_id, count = token_PF_map(list1.head)._3)
            }
            token_node_map(list1.head) = curr_node_id
            id2node += (curr_node_id -> new_node)
            id2node(token_PF_map(list1.head)._1).children.append(new_node)
            curr_node_id += 1
          } else {
            val new_node = ParserNode(token = list1.head._2, node_id = curr_node_id, count = token_PF_map(list1.head)._3)
            token_node_map(list1.head) = curr_node_id
            id2node += (curr_node_id -> new_node)
            id2node(token_PF_map(list1.head)._1).children.append(new_node)
            curr_node_id += 1
          }
        } else if (list1.length > 1) {
          for (token <- list1) {
            // 如果存在将要结束的日志行频率超过theta1，判断频率超过theta4认为对应节点可选，超过theta5认为对应节点直接结束
            if (token._2 == "*") {
              val new_node = if (token_PF_map(token)._2 > theta4) {
                ParserNode(token = token._2, node_id = curr_node_id, optional = true, count = token_PF_map(token)._3)
              } else if (token_PF_map(token)._2 > theta5) {
                ParserNode(token = token._2, node_id = curr_node_id, end = true, count = token_PF_map(token)._3)
              } else {
                ParserNode(token = token._2, node_id = curr_node_id, count = token_PF_map(token)._3)
              }
              token_node_map(token) = curr_node_id
              id2node += (curr_node_id -> new_node)
              id2node(token_PF_map(list1.head)._1).children.append(new_node)
              curr_node_id += 1
            } else {
              val new_node = ParserNode(token = token._2, node_id = curr_node_id, count = token_PF_map(token)._3)
              token_node_map(token) = curr_node_id
              id2node += (curr_node_id -> new_node)
              id2node(token_PF_map(list1.head)._1).children.append(new_node)
              curr_node_id += 1
            }
          }
          if (sum_frequency2 > theta6) {}
        }
      }

      //      println(token_PF_map.toString())
      //      println(pre_list_map.toString())
      //      println(token_node_map.toString())
      //      println(id2node.toString())

      tokenRDD = new_tokenRDD
      // 先过滤已经结束的日志行
      //      new_tokenRDD.take(2).foreach(esTuple => println(esTuple._1 + " " + esTuple._2.mkString("_")))
      new_tokenRDD = tokenRDD.filter(x => x._2.nonEmpty && token_node_map.contains((x._1, x._2.head))).map {
        case (pre, rest_token_list) =>
          pre.append(token_node_map((pre, rest_token_list.head)))
          (pre, rest_token_list.slice(1, rest_token_list.length))
      } .cache()
      if (new_tokenRDD.count() == 0) flag = false
      tokenRDD.unpersist()
    }
    //    println(root)
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