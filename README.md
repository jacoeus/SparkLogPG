# SparkLogPG
项目主要尝试了AECID-PG算法的Spark并行化实现

主要实现了通过elasticsearch-spark获取存储在elasticsearch上的的kubernetes各pod日志信息，建立对应的esRDD。通过对[AECID-PG](https://github.com/ait-aecid/aecid-parsergenerator) 这一基于树的日志解析器生成算法进行并行化设计并在spark实现来进行云原生环境下大量日志的解析树生成，供日志异常检测和进一步的日志分析