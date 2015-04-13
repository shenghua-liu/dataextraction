import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io.StringWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object SubGraph {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    //去除重复项数据
    // remove_repeating_vertice(sc);
    // remove_repeating_edges(sc);

    //补充 id
    joinTable(sc);
  }

  def remove_repeating_vertice(sc: SparkContext) {
    val inputFile = "file:///Users/zhyueqi/WorkSpace/spark/data/post_test.csv"
    val outputFolder = "file:///Users/zhyueqi/WorkSpace/spark/data/primary_post_test"
    val outputFile = "file:///Users/zhyueqi/WorkSpace/spark/data/single_post_test.csv"
    val lines = sc.textFile(inputFile)
    val lineTrips = lines.map(line => line.split(",").map(elem => elem.trim))
    // lineTrips.persist()
    val combined = combine_a(lineTrips);
    println(combined.count());
    writeToFile(combined, outputFolder);
    merge(outputFolder, outputFile)
  }

  def remove_repeating_edges(sc: SparkContext) {
    val inputFile = "file:///Users/zhyueqi/WorkSpace/spark/data/reRelation_test.csv"
    val outputFolder = "file:///Users/zhyueqi/WorkSpace/spark/data/primary_reRelation_test"
    val outputFile = "file:///Users/zhyueqi/WorkSpace/spark/data/single_reRelation_test.csv"
    val lines = sc.textFile(inputFile)
    val lineTrips = lines.map(line => line.split(",").map(elem => elem.trim))
    // lineTrips.persist()
    val combined = combine_b(lineTrips);
    println(combined.count());
    writeToFile(combined, outputFolder);
    merge(outputFolder, outputFile)
  }

  def joinTable(sc: SparkContext) {
    val post_file = "file:///Users/zhyueqi/WorkSpace/spark/data/single_post_test.csv"
    val relation_file = "file:///Users/zhyueqi/WorkSpace/spark/data/single_reRelation_test.csv"
    val joined_file_folder = "file:///Users/zhyueqi/WorkSpace/spark/data/recover"
    val joined_file_file = "file:///Users/zhyueqi/WorkSpace/spark/data/recover.csv"
    val lines_post = sc.textFile(post_file)
    val lines_relation = sc.textFile(relation_file)

    val rows_post = lines_post.map(x => x.split(",").map(ele => ele.trim))
    val rows_relation = lines_relation.map(x => x.split(",").map(ele => ele.trim))

    val pair_post = rows_post.map(x => (x(1), x(0))); //名字，id

    val pair_relation = rows_relation.map(x => (x(0), x)) //MD5,全信息

    val filter_relation_l = rows_relation.filter(x => x(1) == "0") //过滤出head 0
    val pair_relation_l = filter_relation_l.map(x => (x(2), x(0))) //名字，MD5
    val fill_result_l = pair_relation_l.leftOuterJoin(pair_post).map {
      case (key, (a, b)) => (a, b.getOrElse("0")) //md5,id
    }
    val final_l = pair_relation.leftOuterJoin(fill_result_l).map {
      case (md5, (info, id)) =>
        info(1) = id.getOrElse("0")
        (md5, info)
    }
    val filter_relation_r = rows_relation.filter(x => x(3) == "0") //过滤出 tail 0
    val pair_relation_r = filter_relation_r.map(x => (x(4), x(0))) //名字，MD5
    val fill_result_r = pair_relation_r.leftOuterJoin(pair_post).map {
      case (key, (a, b)) => (a, b.getOrElse("0")) //md5,id
    }
    val result = final_l.leftOuterJoin(fill_result_r).map {
      case (md5, (info, id)) =>
        info(3) = id.getOrElse("0")
        info.mkString(",")
    }
    writeToFile(result, joined_file_folder)
    merge(joined_file_folder,joined_file_file)
  }

  /**
   * 2562810123,1句实话,1
   */
  def combine_a(arg: RDD[Array[String]]): RDD[String] = {
    val keyValue = arg.map(x => (x(0), (x(1), x(2).toInt)))
    val combined = keyValue.reduceByKey((a, b) => (a._1, a._2 + b._2))
    combined.map(x => Array(x._1, x._2._1, x._2._2.toString).mkString(","))
  }
  /**
   * 02ba52da045f03d1c38d296520733a51,0,1句实话,1364258151,风声水早起,1
   */
  def combine_b(arg: RDD[Array[String]]): RDD[String] = {
    val keyValue = arg.map(x => (x(0), (x(1), x(2), x(3), x(4), x(5).toInt)))
    val combined = keyValue.reduceByKey((a, b) => (a._1, a._2, a._3, a._4, a._5 + b._5))
    combined.map(x => Array(x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._5.toString).mkString(","))
  }

  def writeToFile(arg: RDD[String], outputFile: String) {
    arg.saveAsTextFile(outputFile)
  }
  /**
   * 合并文件
   */
  def merge(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }
}

