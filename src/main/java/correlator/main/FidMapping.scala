package correlator.main

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ranmx on 2018/1/4.
  */

object FidMapping {

    private val logger = LoggerFactory.getLogger(classOf[FidMapping])

    val RELATION_TABLE_SQL = "SELECT `key` as  `fid` , `relate_fid` as `relate_fid` FROM fosundb.hbase_dp_Relation LATERAL VIEW EXPLODE(related_fid) relatesTable AS relate_fid, v"
    val PERSON_TABLE_SQL = "SELECT `key` as `fid` FROM fosundb.hbase_dp_rawdata_globalperson TABLESAMPLE"

    val sparkConf: SparkConf = new SparkConf().setAppName("Fid Mapping")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = spark.sparkContext

    def main (args: Array[String]): Unit ={
        val personDf = spark.sql(PERSON_TABLE_SQL).repartition(1000)
        val relateDf = spark.sql(RELATION_TABLE_SQL).repartition(1000)
        val vertexPersion: RDD[(VertexId, Int)] = personDf.rdd.mapPartitions((iterator: Iterator[Row]) => {
            val array: ArrayBuffer[(VertexId, Int)] = ArrayBuffer[(VertexId, Int)]()
            while (iterator.hasNext){
                val row = iterator.next()
                val vertexId: Long = java.lang.Long.parseLong(row.getString(0))
                array.+=:(vertexId, 1)
            }
            array.iterator
        })

        val edgeRelation: RDD[Edge[Int]] = relateDf.rdd.mapPartitions((iterator: Iterator[Row]) => {
            val array: ArrayBuffer[Edge[Int]] = ArrayBuffer[Edge[Int]]()
            while(iterator.hasNext){
                val row = iterator.next()
                val srcFid: Long = java.lang.Long.parseLong(row.getString(0))
                val dstFid: Long = java.lang.Long.parseLong(row.getString(1))
                array.+=:(new Edge(srcFid, dstFid, 1))
            }
            array.iterator
        })

        val graph = Graph(vertexPersion, edgeRelation, 1,StorageLevel.DISK_ONLY, StorageLevel.DISK_ONLY)
        val connectedFids =  graph.ops.connectedComponents()

        val connectedFidsPairRdd =  connectedFids.vertices.map((vertex: Tuple2[VertexId, VertexId]) => (vertex._2.asInstanceOf[Long], ArrayBuffer(vertex._1.asInstanceOf[Long])))

        val connectedLists =  connectedFidsPairRdd.reduceByKey((a,b) => a.++:(b))

        connectedLists.saveAsTextFile("/user/fosundb/test/ranmxTest/graph/")

    }
}

class FidMapping {}