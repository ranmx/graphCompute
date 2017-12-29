package correlator.main

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

object Launch {
    private val APPNAME = "CorrelatorLaunch"
    private val logger = LoggerFactory.getLogger(classOf[CorrelatorLaunch])
    //private static String RELATIONTABLE = "select id_fosunid, relates from tmp_fosunid_relation LATERAL VIEW explode(id_fosunid_map) relatesTable AS relates, v limit 10";
    private val PROFILE_TABLE = "SELECT `id_uid_fosunid`,`ip_ibi_name`,`ip_ibi_gender`,`ip_ibi_birth`,`ip_ibi_age`,`ip_ibi_decade`,`ip_ibi_national`,`ip_ibi_stage`,`ip_ibi_married`,`ip_ibi_address`,`ip_ibi_province`,`ip_ibi_city`,`ip_devc_os`,`source` FROM `individual_profile`"
    private val RELATION_TABLE = "SELECT `id_fosunid`, `relates` FROM `fosunid_relation` LATERAL VIEW EXPLODE(id_fosunid_map) relatesTable AS relates, v"
    private val FID_TABLE = "SELECT `id_fosunid` as `fid` FROM `super_id`"
    private val FULLRELATIONDATA_TABLE = "IndividualProfileJoined"
    private val DATE = "201607"

    def main(args: Array[String]): Unit = {
        logger.info(APPNAME + " START ...")
        val sparkConfig = new SparkConf
        sparkConfig.setAppName(APPNAME)
        val sparkSession = SparkSession.builder().config(sparkConfig).enableHiveSupport.getOrCreate()
        import sparkSession.implicits._
        try { // Source Data Frame from HIVE
            sparkSession.sql("use fonovadb")
            val profilePairRdd = sparkSession.sql(PROFILE_TABLE).rdd.map((row: Row) => new Tuple2[String, Row](row.getString(0), row))
            val dataDf = sparkSession.sql(FID_TABLE)
            val indexDf = dataDf.withColumn("index", monotonically_increasing_id)
            logger.warn("indexDf: ")
            indexDf.printSchema()
            indexDf.show(20, false)
            System.out.println(indexDf.count)

            val indexPairRdd = indexDf.rdd.map((row: Row) => new Tuple2[Long, String](row.getLong(1), row.getString(0)))
            var relationDf: Dataset[Row]  = sparkSession.sql(RELATION_TABLE)
            logger.warn("relationDf: ")
            relationDf.printSchema()
            relationDf.show(20, false)

            relationDf = relationDf
              .join(indexDf, col("id_fosunid").equalTo(col("fid")))
              .withColumnRenamed("index", "id")
              .drop("fid")
              .join(indexDf, col("relates").equalTo(col("fid")))
              .withColumnRenamed("index", "relate")

            relationDf = relationDf
              .select(col("id"), col("relate"))
              .withColumn("property", monotonically_increasing_id)

            logger.warn("relationDf: ")
            relationDf.show(20, false)

            val edges = relationDf.map((row: Row) => new Edge[String](row.getLong(0), row.getLong(1), "1"))

            val graph = Graph.fromEdges(edges.rdd, "1" , StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)

            val graphRdd = graph.triplets.map((triplet: EdgeTriplet[String, String]) => "src: " + triplet.srcAttr + "dst: " + triplet.dstAttr + "attr: " + triplet.attr)

            graphRdd.take(10).foreach((s) => System.out.println(s))

            val vertexRdd = graph.ops.connectedComponents.vertices
            logger.warn("vertexRdd: " + vertexRdd.take(10).toString)

            val vertexPairRdd = vertexRdd.map((vertex: Tuple2[VertexId, VertexId]) => (vertex._1.asInstanceOf[Long], vertex._2.asInstanceOf[Long]))
            logger.warn("vertexPairRdd: " + vertexPairRdd.take(10).toString)

            val connectedFidPairRdd = vertexPairRdd.join(indexPairRdd).values.map((vertex: Tuple2[Long, String]) => vertex)
            logger.warn("connectedFidPairRdd: " + connectedFidPairRdd.take(10).toString)

            val groupConnectedFidPairRdd = connectedFidPairRdd.groupByKey
            val fidRelatesRdd = connectedFidPairRdd.join(groupConnectedFidPairRdd).values
            logger.warn("fidRelatesRdd: " + fidRelatesRdd.take(10).toString)

            val flatFidRelatesPairRdd = fidRelatesRdd.map((v: Tuple2[String, Iterable[String]]) => v).flatMapValues((v: Iterable[String]) => v)
            logger.warn("flatFidRelatesPairRdd: " + flatFidRelatesPairRdd.take(1).toString)

            val flatFidRelatesDataRdd = flatFidRelatesPairRdd.map((p: Tuple2[String, String]) => p.swap).join(profilePairRdd).values
            logger.warn("flatFidRelatesDataRdd: " + flatFidRelatesDataRdd.take(10).toString)

            flatFidRelatesDataRdd.foreach((v: Tuple2[String, Row]) => {
                def foo(v: Tuple2[String, Row]) = {
                    val map = new util.HashMap[String, String]
                    val row = v._2
                    val fieldnames = row.schema.fieldNames
                    var i = 1
                    while (i < row.size) {
                        if (row.getAs(i) != null) {
                            map.put(DATE + ":" + fieldnames(i) + "." + row.getString(0), row.getAs(i))
                        }
                        {i += 1; i - 1}
                    }

                    if (!map.isEmpty) { //  HBaseOperator.newInstance().insert(FULLRELATIONDATA_TABLE, v._1, map);
                        logger.warn("map: " + map.toString)
                    }}
                    foo(v)
                })
        }
            catch {
                case e: Exception =>
                    e.printStackTrace()
                    logger.error("Spark Job <" + APPNAME + "> Exception!")
            }
            finally {
                logger.info(APPNAME + " END ...")
            }
        }
    }
