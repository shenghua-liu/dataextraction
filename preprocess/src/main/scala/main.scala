import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.graphx.impl.{ EdgePartitionBuilder, GraphImpl }
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.graphx._
import com.typesafe.config._
import simplelib._

object SubGraph extends Logging {

    // Load our own config values from the default location, application.conf
    val conf = ConfigFactory.load()

    val context = new SimpleLibContext()

    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Components")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)
        // sc.setCheckpointDir("/tmp")
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
                    //joinTable(sc, args(1), args(2), args(3))
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
            case "5" => {
                if (args.length == 4) {
                    diff(sc, args(1), args(2), args(3))
                }
            }
            case "6" => {
                if (args.length == 3) {
                    components(sc, args(1), args(2))
                }
            }
            case "7" => {
                if (args.length == 3) {
                    hash2minComponents(sc, args(1), args(2))
                }
            }
            case "8" => {
                if (args.length == 5) {
                    hash2min_format_result(sc, args(1), args(2), args(3), args(4))
                }
            }
            case _ => {
                println("【1】：清理节点；【2】：清理边；【3】：补充Id；【4】：求子图 （填入【】中数字）")
            }
        }

        task(args(0))
        sc.stop()
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
        val lines_post = sc.textFile(src_vertice)
        val lines_relation = sc.textFile(src_relation)
        val vertices_in_post = lines_post.map(x => x.split(",").map(ele => ele.trim)).map(x => (nameHash(x(1)), x(0))) //VertexId,String(id)
        val lines_arr = lines_relation.map(x => x.split(",").map(ele => ele.trim))
        val triplets = lines_arr.map(x => Edge(nameHash(x(2)), nameHash(x(4)), (x(0), x(5)))) //Edge(Vid,Vid,(MD5,count))
        val vertices_dirty = lines_arr.flatMap {
            case arr =>
                List((nameHash(arr(2)), (arr(1), arr(2))), (nameHash(arr(4)), (arr(3), arr(4)))) //(VertexId,(id,name))
        }
        val vertices_in_edges = vertices_dirty.reduceByKey((a, b) => a);
        val graph = Graph(vertices_in_edges, triplets, ("MD5", "COUNT")) //default value of vertex
        val recovergraph = graph.outerJoinVertices(vertices_in_post) {
            case (vid, pathVD, postVDOps) =>
                // val id = postVDOps.getOrElse(pathVD._1)
                // (id, pathVD._2)
                postVDOps match {
                    case Some(x) => (x, pathVD._2, true)
                    case None => (pathVD._1, pathVD._2, false)
                }

        }
        recovergraph.triplets.map(x => Array(x.attr._1, x.srcAttr._1, x.srcAttr._2, x.dstAttr._1, x.dstAttr._2, x.attr._2).mkString(",")).saveAsTextFile(dst + "/recover_reRelation")
        recovergraph.vertices.filter(x => x._2._3 == false).map(x => Array(x._2._1, x._2._2, 0).mkString(",")).saveAsTextFile(dst + "/recover_bad_post")
        merge(dst + "/recover_reRelation", dst + "/recover_reRelation.csv")
        merge(dst + "/recover_bad_post", dst + "/recover_bad_post.csv")

    }

    def joinTable(sc: SparkContext, src_vertice: String, src_relation: String, dst: String) {
        val lines_post = sc.textFile(src_vertice)
        val lines_relation = sc.textFile(src_relation)

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
            .distinct(600)
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

    def components(sc: SparkContext, src: String, dst: String) {
        logWarning("components !!!")
        val edge_tripl = sc.textFile(src)
            .distinct(300)
            .map { x =>
                val arr = x.split(",").map(e => e.trim)
                ((nameHash(arr(2)), arr(2)), (nameHash(arr(4)), arr(4)), x(5).toLong)
            }
        logWarning("load file")

        val edges = edge_tripl.map {
            case (src, dst, w) =>
                Edge(src._1, dst._1, w)
        }

        logWarning("edges")
        val vertices_dirty: RDD[(VertexId, String)] = edge_tripl.flatMap {
            case (src, dst, w) =>
                List((src._1, src._2), (dst._1, dst._2))
        }

        logWarning("start component")
        val graph = Graph(vertices_dirty, edges, "")
        val vertices = graph.vertices
        val g = ConnectedComponents.run(graph)
        logWarning("end component")
        val result = g.vertices.leftOuterJoin(vertices).map {
            case (vid, (label, nameOps)) =>
                val name = nameOps.getOrElse(vid)
                Array(name, label).mkString(",")
        }

        result.saveAsTextFile(dst)
        merge(dst, dst + ".csv")
    }

    def hash2minComponents(sc: SparkContext, src: String, dst: String) {
        logWarning("hash2min")
        val edge_tripl = sc.textFile(src)
            // .distinct(300)
            .map { x =>
                val arr = x.split(",").map(e => e.trim)
                ((nameHash(arr(2)), arr(2)), (nameHash(arr(4)), arr(4)), x(5).toLong)
            }
        val edges_pre = edge_tripl.map {
            case (src, dst, w) =>
                Edge(src._1, dst._1, w)
        }

        val edges = edges_pre.filter(x => x.srcId != x.dstId)

        val graph = Graph.fromEdges(edges, "")
        val init_vertices = graph.aggregateMessages[List[VertexId]](
            triplet => {
                triplet.sendToSrc(List(triplet.dstId))
                triplet.sendToDst(List(triplet.srcId))
            },
            (a, b) => {
                List.concat(a, b)
            })
        var cluster: VertexRDD[(Int, List[VertexId])] = init_vertices.mapValues((vid, nbs) => (1, nbs ::: List(vid))).persist(MEMORY_AND_DISK)
        //val maxInteration = Integer.max
        var iteration = 1
        var newCluster: VertexRDD[(Int, List[VertexId])] = null
        //remove node whose cluster's length is 1
        var tmp: VertexRDD[(Int, List[VertexId])] = cluster

        var continue: Boolean = true
        var preCount: Long = 0
        while (iteration < 100 && continue) {
            logWarning("hash2min iteration :" + iteration)
            val mark = iteration
            //send C(v) to min
            val union_a_map = tmp.map {
                case (vid, nbs) =>
                    val minVertex = nbs._2.min
                    (minVertex, nbs._2)
            }
            val union_a = union_a_map.reduceByKey((a, b) => a ::: b)

            // logWarning("union_a count" + union_a.count())
            //send {Vmin} to nbs
            val union_b_map = tmp.flatMap {
                case (vid, nbs) =>
                    val minVertex = nbs._2.min
                    for (child <- nbs._2) yield (child, List(minVertex))
            }
            val union_b = union_b_map.reduceByKey((a, b) => a ::: b)
            // logWarning("union_b count" + union_b.count())
            //merge msg
            val union = union_a.fullOuterJoin(union_b).map {
                case (target, (aOps, bOps)) =>
                    val a = aOps.getOrElse(List())
                    val b = bOps.getOrElse(List())
                    (target, a ::: b)
            }
            // logWarning("union count" + union.count())
            //update vertex
            newCluster = cluster.leftJoin(union) {
                (vid, old, newOps) =>
                    newOps match {
                        case None => old
                        case Some(x) =>
                            val y = x.distinct
                            if (y.length == 1) {
                                if (old._2.length == 1 && old._2.head == y.head)
                                    old
                                else
                                    (mark + 1, y)
                            } else {
                                (mark + 1, y)
                            }
                    }
            }
            tmp.unpersist()
            cluster.unpersist()
            newCluster.persist(MEMORY_AND_DISK)
            cluster = newCluster
            union_a.unpersist()
            union_b.unpersist()
            union.unpersist()
            //cluster.checkpoint()
            //those unchanged are filtered
            tmp = cluster.filter(x => x._2._1 == (mark + 1)).persist(MEMORY_AND_DISK)
            val count = tmp.count()
            if (count == preCount) {
                tmp.unpersist()
                continue = false
            } else {
                preCount = count
            }

            // val it = tmp.toLocalIterator
            // while (it.hasNext) {
            //     var item = it.next()
            //     logWarning("item:" + item)
            // }

            logWarning("tmp count " + count)
            iteration = iteration + 1
        }

        cluster.map(x => Array(x._1.toString, x._2._2.mkString(",")).mkString(",")).saveAsTextFile(dst)
        merge(dst, dst + ".csv")
    }

    def hash2min_format_result(sc: SparkContext, verticePath: String, edgePath: String, componentPath: String, dst: String) {
        logWarning("format components !!!")
        val edge_tripl = sc.textFile(edgePath)
            .map { x =>
                val arr = x.split(",").map(e => e.trim)
                ((nameHash(arr(2)), arr(2)), (nameHash(arr(4)), arr(4)), x(5).toLong)
            }
        val vertices_dirty: RDD[(VertexId, String)] = edge_tripl.flatMap {
            case (src, dst, w) =>
                List((src._1, src._2), (dst._1, dst._2))
        }
        val vertices = vertices_dirty.reduceByKey((a, b) => a);
        //从定点集文件获取属性
        val vertice_file = sc.textFile(verticePath)
        val vertices_weight_file = verticeWeightFromFile(vertice_file)
        val vertices_weight = vertices.leftOuterJoin(vertices_weight_file).map {
            case (id, (name, wOps)) =>
                (id, (name, wOps.getOrElse(0L)))
        }
        val component_pre: RDD[(Long, Long)] = sc.textFile(componentPath).flatMap {
            x =>
                val arr = x.split(",").map(e => e.trim)
                val label = arr(0).toLong
                val head = arr(1).toLong
                if (label <= head) {
                    for (i <- 1 to arr.length - 1) yield (arr(i).toLong, label)
                } else {
                    List((0L, 0L))
                }

        }
        val component = component_pre.filter(x => x._2 != 0L)
        val group_piece = component.leftOuterJoin(vertices_weight).map {
            case (node, (label, attrOps)) =>
                val attr = attrOps.getOrElse(("None", 0L))
                //label,(name,count)
                (label, attr._1 + ":" + attr._2)
        }
        group_piece.persist(MEMORY_AND_DISK)
        val group_count: RDD[(Long, Long)] = group_piece.mapValues(_ => 1L).reduceByKey(_ + _)
        val group_member: RDD[(Long, String)] = group_piece.combineByKey(
            (v: String) => List(v), //create combiner
            (c: List[String], v: String) => v :: c, //combine c and v
            (c1: List[String], c2: List[String]) => c1 ::: c2 //combine c and c
            ).leftOuterJoin(group_count).map {
                case (k, (c, countOps)) =>
                    val count = countOps.getOrElse(0L)
                    (count, c.mkString(","))
            }
        group_member.sortBy(x => x._1).map(x => Array(x._1, x._2).mkString(",")).saveAsTextFile(dst)
        group_piece.unpersist()
        merge(dst, dst + ".csv")

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
        file.map(x => x.split(",").map(e => e.trim)).map(x => (nameHash(x(1)), x(2).toLong)).reduceByKey((a, b) => a + b)
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
        val keyValue = arg.filter(x => false == (x(1) == null || x(1).isEmpty)).map(x => (x(0), (x(1), x(2).toInt)))
        val combined = keyValue.reduceByKey((a, b) => (a._1, a._2 + b._2))
        combined.map(x => Array(x._1, x._2._1, x._2._2.toString).mkString(","))
    }
    /**
     * 02ba52da045f03d1c38d296520733a51,0,1句实话,1364258151,风声水早起,1
     */
    def combine_b(arg: RDD[Array[String]]): RDD[String] = {
        val keyValue = arg.filter(x => false == (x(2) == null || x(2).isEmpty || x(4) == null || x(4).isEmpty)).map(x => (x(0), (x(1), x(2), x(3), x(4), x(5).toInt)))
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

    /**
     *   0 代表 b没有 a有
     *   -1代表 a没有 b有
     *   >1代表 a中重复的数据
     */
    def diff(sc: SparkContext, fa: String, fb: String, saving: String) {
        val post_a = sc.textFile(fa).map(x => x.split(",").map(x => x.trim)).map(x => (x(1), 1))
        val post_b = sc.textFile(fb).map(x => x.split(",").map(x => x.trim)).map(x => (x(1), 1)).reduceByKey((a, b) => a)
        val join = post_a.fullOuterJoin(post_b).map {
            case (id, (aOps, bOps)) =>
                aOps match {
                    case None => (id, -1)
                    case Some(x) =>
                        bOps match {
                            case None => (id, 0)
                            case Some(y) => (id, y)
                        }
                }
        }
        join.reduceByKey((a, b) => a + b).filter(x => x._2.toInt != 1).map(x => Array(x._1, x._2).mkString(",")).saveAsTextFile(saving)
        merge(saving, saving + ".csv")
    }
}

