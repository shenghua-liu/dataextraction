import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.graphx.impl.{ EdgePartitionBuilder, GraphImpl }
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.graphx._
import com.typesafe.config._
import simplelib._

object SubGraph {

    // Load our own config values from the default location, application.conf
    val conf = ConfigFactory.load()

    val context = new SimpleLibContext()

    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Components")
        val sc = new SparkContext(conf)

        if (args.length < 1) {
            println("【参数一】运行程序编号，【参数...】");
            return
        }

        def task(x: String) = x match {
            case "1" => {
                if (args.length >= 3) {
                    remove_repeating_vertice(sc, args(1), args(2))
                }
            }
            case "2" => {
                if (args.length >= 3) {
                    remove_repeating_edges(sc, args(1), args(2))
                }
            }
            case "3" => {
                if (args.length >= 4) {
                    // joinTable(sc, args(1), args(2), args(3))
                    recover_id(sc, args(1), args(2), args(3))
                }
            }
            case "4" => {
                if (args.length == 3) {
                    connectedComponents(sc, args(1), args(2))
                }
                if (args.length == 4) {
                    connectedComponents(sc, args(1), args(2), args(3))
                }
            }
            case _ => {
                println("【1】：清理节点；【2】：清理边；【3】：补充Id；【4】：求子图 （填入【】中数字）")
            }
        }

        task(args(0));
    }

    def remove_repeating_vertice(sc: SparkContext, src: String, dst: String) {

        val lines = sc.textFile(src)
        val lineTrips = lines.map(line => line.split(",").map(elem => elem.trim))
        val combined = combine_a(lineTrips);
        // println(combined.count());
        writeToFile(combined, dst);
        merge(dst, dst + ".csv")
    }

    def remove_repeating_edges(sc: SparkContext, src: String, dst: String) {
        val lines = sc.textFile(src)
        val lineTrips = lines.map(line => line.split(",").map(elem => elem.trim))
        val combined = combine_b(lineTrips);
        // println(combined.count());
        writeToFile(combined, dst);
        merge(dst, dst + ".csv")
    }

    def recover_id(sc: SparkContext, src_vertice: String, src_relation: String, dst: String) {
        val lines_post = sc.textFile(src_vertice).distinct(100)
        val lines_relation = sc.textFile(src_relation).distinct(100)
        val vertices_in_post = lines_post.map(x => x.split(",").map(ele => ele.trim)).map(x => (nameHash(x(1)), x(0))) //VertexId,String(id)
        val lines_arr = lines_relation.map(x => x.split(",").map(ele => ele.trim)).cache()
        val triplets = lines_arr.map(x => Edge(nameHash(x(2)), nameHash(x(4)), (x(0), x(5)))) //Edge(Vid,Vid,(MD5,count))
        val vertices_dirty = lines_arr.flatMap {
            case arr =>
                List((nameHash(arr(2)), (arr(1), arr(2))), (nameHash(arr(4)), (arr(3), arr(4)))) //(VertexId,(id,name))
        }
        val vertices_in_edges = vertices_dirty.reduceByKey((a, b) => a);
        val graph = Graph(vertices_in_edges, triplets, ("MD5", "COUNT")) //default value of vertex
        val recovergraph = graph.outerJoinVertices(vertices_in_post) {
            case (vid, pathVD, postVDOps) =>
                val id = postVDOps.getOrElse(pathVD._1)
                (id, pathVD._2)
        }
        recovergraph.triplets.map(x => Array(x.attr._1, x.srcAttr._1, x.srcAttr._2, x.dstAttr._1, x.dstAttr._2, x.attr._2).mkString(",")).saveAsTextFile(dst)
        merge(dst, dst + ".csv")
    }

    def joinTable(sc: SparkContext, src_vertice: String, src_relation: String, dst: String) {
        val lines_post = sc.textFile(src_vertice).distinct(2)
        val lines_relation = sc.textFile(src_relation).distinct(2)

        val rows_post = lines_post.map(x => x.split(",").map(ele => ele.trim))
        val rows_relation = lines_relation.map(x => x.split(",").map(ele => ele.trim))

        val pair_post = rows_post.map(x => (x(1), x(0))); //名字，id

        val final_l = mergeLeft(rows_relation, pair_post)

        val result = mergeRight(final_l, rows_relation, pair_post)
        writeToFile(result, dst)
        merge(dst, dst + ".csv")
    }

    def mergeLeft(rows_relation: RDD[Array[String]], pair_post: RDD[(String, String)]): RDD[(String, Array[String])] = {
        val pair_relation = rows_relation.map(x => (x(0), x)) //MD5,全信息
        val filter_relation_l = rows_relation.filter(x => x(1) == "0") //过滤出head 0

        if (filter_relation_l.count() != 0) {
            val pair_relation_l = filter_relation_l.map(x => (x(2), x(0))) //名字，MD5

            val fill_result_l = pair_relation_l.leftOuterJoin(pair_post).map {
                case (key, (a, b)) => (a, b.getOrElse("0")) //md5,id
            }
            pair_relation.leftOuterJoin(fill_result_l).map {
                case (md5, (info, id)) =>
                    info(1) = id.getOrElse("0")
                    (md5, info)
            }
        } else {
            pair_relation
        }
    }
    def mergeRight(arg: RDD[(String, Array[String])], rows_relation: RDD[Array[String]], pair_post: RDD[(String, String)]): RDD[String] = {
        val filter_relation_r = rows_relation.filter(x => x(3) == "0") //过滤出 tail 0
        if (filter_relation_r.count() != 0) {
            val pair_relation_r = filter_relation_r.map(x => (x(4), x(0))) //名字，MD5
            val fill_result_r = pair_relation_r.leftOuterJoin(pair_post).map {
                case (key, (a, b)) => (a, b.getOrElse("0")) //md5,id
            }
            arg.leftOuterJoin(fill_result_r).map {
                case (md5, (info, id)) =>
                    info(3) = id.getOrElse("0")
                    info.mkString(",")
            }
        } else {
            arg.map(x => x._2.mkString(","))
        }
    }

    def connectedComponents(sc: SparkContext, src: String, dst: String, verticeFile: String = "") {

        val result = computeAndMerge(sc, src, dst, verticeFile)
        //排序
        val sorted = result.sortBy(x => x._1)
        //格式化输出
        val arrayMap = sorted.map {
            case (vNo, (pNo, member)) =>
                vNo.toString + "," + pNo.toString + "," + member
        }
        arrayMap.saveAsTextFile(dst)
        merge(dst, dst + ".txt")

    }

    def computeAndMerge(sc: SparkContext, src: String, dst: String, verticeFile: String = "") = {
        val edge_tripl = sc.textFile(src)
            .distinct(6)
            .map { x =>
                val arr = x.split(",").map(e => e.trim)
                ((nameHash(arr(2)), arr(2)), (nameHash(arr(4)), arr(4)), x(5).toLong)
            }

        val edges = edge_tripl.map {
            case (src, dst, w) =>
                Edge(src._1, dst._1, w)
        }

        val g = Graph.fromEdges(edges, "")

        val labled_components = ConnectedComponents.run(g)

        val vertices_weight = vertices_vd(sc, verticeFile, labled_components, edge_tripl)

        extractEachComponentByVertice(labled_components, vertices_weight)

    }

    //获取节点属性（名称，发帖数量）//两种模式
    def vertices_vd(sc: SparkContext, mode: String, labled_components: Graph[Long, Long], edge_tripl: RDD[((Long, String), (Long, String), Long)]): RDD[(VertexId, (String, Long))] = mode.length match {
        case 0 => {
            val vertices_dirty: RDD[(VertexId, String)] = edge_tripl.flatMap {
                case (src, dst, w) =>
                    List((src._1, src._2), (dst._1, dst._2))
            }
            val vertices = vertices_dirty.reduceByKey((a, b) => a);
            //从边集文件统计发帖数量（无向）
            val vertices_weight: RDD[(VertexId, Long)] = verticeWeight(labled_components)
            vertices.leftOuterJoin(vertices_weight).map {
                case (id, (name, wOps)) =>
                    (id, (name, wOps.getOrElse(0L)))
            }
        }
        case _ => {
            val vertices_dirty: RDD[(VertexId, String)] = edge_tripl.flatMap {
                case (src, dst, w) =>
                    List((src._1, src._2), (dst._1, dst._2))
            }
            val vertices = vertices_dirty.reduceByKey((a, b) => a);
            //从定点集文件获取属性
            val file = sc.textFile(mode).distinct(200)
            val vertices_weight = verticeWeightFromFile(file)
            vertices.leftOuterJoin(vertices_weight).map {
                case (id, (name, wOps)) =>
                    (id, (name, wOps.getOrElse(0L)))
            }
        }
    }

    // Hash function to assign an Id to each article
    def nameHash(title: String): VertexId = {
        title.toLowerCase.replace(" ", "").hashCode.toLong
    }

    def extractEachComponentByVertice(labled_components: Graph[Long, Long], vertices: RDD[(VertexId, (String, Long))]) = {
        // ? 如果找不到对应的点 ---》 异常信息写入日志
        val merged_vertice = labled_components.vertices.leftOuterJoin(vertices).map {
            case (id, (label, vdOps)) =>
                val vd = vdOps.getOrElse(("Not found", 0L))
                (label, (vd._1, vd._2))
        }

        val membersOfGroup = merged_vertice.reduceByKey {
            case (v1, v2) =>
                if (v2._2 < 0 && v1._2 < 0) {
                    (v1._1 + "," + v2._1, v1._2 + v2._2)
                } else if (v1._2 < 0 && v2._2 >= 0) {
                    (v1._1 + "," + v2._1 + ":" + v2._2, v1._2 - 1)
                } else if (v1._2 >= 0 && v2._2 < 0) {
                    (v2._1 + "," + v1._1 + ":" + v1._2, v2._2 - 1)
                } else {
                    //(v1._2 >= 0 && v2._2 >= 0)
                    (v1._1 + ":" + v1._2 + "," + v2._1 + ":" + v2._2, -2)
                }
        }

        val postOfGroup = merged_vertice.reduceByKey((v1, v2) => (v1._1, v1._2 + v2._2))

        postOfGroup.leftOuterJoin(membersOfGroup).map {
            case (label, (post, memberOps)) =>
                val mOps = memberOps.getOrElse(("missing[ERROR]", -1L))
                ((mOps._2 * (-1L)), (post._2, mOps._1))
        }

    }

    /**
     * 统计邻边权值（无向）
     */
    def verticeWeight(g: Graph[Long, Long]): RDD[(VertexId, Long)] = {
        def sendMsg(ctx: EdgeContext[Long, Long, Long]) = {
            ctx.sendToDst(ctx.attr)
            ctx.sendToSrc(ctx.attr)
        }
        g.aggregateMessages[Long](sendMsg, _ + _)
    }

    /**
     * 从节点文件内读取
     *
     */
    def verticeWeightFromFile(file: RDD[String]): RDD[(VertexId, Long)] = {
        file.map(x => x.split(",").map(e => e.trim)).map(x => (nameHash(x(1)), x(2).toLong))
    }

    def extractEachComponentByEdges(labled_components: Graph[Long, Long]) {
        val groups = labled_components.triplets.groupBy {
            case (edgeTriplet) => edgeTriplet.srcAttr
        }
        //print each group
        groups.foreach {
            case (key, iterator_edgeTriplet) =>
                println("label:" + key)

                println("vertices count:")

                var vertices_num = 0;
                if (iterator_edgeTriplet.size == 1)
                    vertices_num = 2;
                else
                    vertices_num = iterator_edgeTriplet.size * 2 - 1;

                println(vertices_num)

                println("edges:")
                for (i <- iterator_edgeTriplet) {
                    print(i.srcId + "-->" + i.dstId + ",")
                }
                println("")

        }
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
        // val hdfs = FileSystem.get(hadoopConfig)
        val fs = FileSystem.getLocal(hadoopConfig)
        FileUtil.copyMerge(fs, new Path(srcPath), fs, new Path(dstPath), false, hadoopConfig, null)
    }
}

