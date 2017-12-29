package correlator.main;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Tuple2;
import scala.reflect.ClassTag$;

public class CorrelatorLaunch {
	private static String APPNAME = "CorrelatorLaunch";
	private static Logger logger = LoggerFactory.getLogger(CorrelatorLaunch.class);
	
	//private static String RELATIONTABLE = "select id_fosunid, relates from tmp_fosunid_relation LATERAL VIEW explode(id_fosunid_map) relatesTable AS relates, v limit 10";
	private static String PROFILE_TABLE = 
			"SELECT `id_uid_fosunid`,`ip_ibi_name`,`ip_ibi_gender`,`ip_ibi_birth`,`ip_ibi_age`,`ip_ibi_decade`,`ip_ibi_national`,`ip_ibi_stage`,`ip_ibi_married`,`ip_ibi_address`,`ip_ibi_province`,`ip_ibi_city`,`ip_devc_os`,`source` FROM `individual_profile`";
	private static String RELATION_TABLE = "SELECT `id_fosunid`, `relates` FROM `fosunid_relation` LATERAL VIEW EXPLODE(id_fosunid_map) relatesTable AS relates, v";
	private static String FID_TABLE = "SELECT `id_fosunid` as `fid` FROM `super_id`";
	private static String FULLRELATIONDATA_TABLE = "IndividualProfileJoined";
	private static String DATE = "201607";
	
	public static void main(String[] args) {
		
		logger.info(APPNAME + " START ...");
		SparkConf sparkConfig = new SparkConf();
		sparkConfig.setAppName(APPNAME);
        SparkSession sparkSession = SparkSession.builder().config(sparkConfig).enableHiveSupport().getOrCreate();

		try {
			// Source Data Frame from HIVE
			sparkSession.sql("use fonovadb");
			
			JavaPairRDD<String, Row> profilePairRdd = 
					sparkSession.sql(PROFILE_TABLE)
					           .javaRDD()
					           .mapToPair(row -> new Tuple2<>(row.getString(0),row));
			
			Dataset<Row> dataDf = sparkSession.sql(FID_TABLE);
			Dataset<Row> indexDf = dataDf.withColumn("index", monotonically_increasing_id());
            logger.warn("indexDf: " );
			indexDf.printSchema();
			indexDf.show(20, false);
			System.out.println(indexDf.count());
			JavaPairRDD<Long, String> indexPairRdd = indexDf.javaRDD().mapToPair(row->
			                             new Tuple2<>(row.getLong(1), row.getString(0)));
			
			
			Dataset<Row> relationDf = sparkSession.sql(RELATION_TABLE);
            logger.warn("relationDf: " );
			relationDf.printSchema();
			relationDf.show(20, false);
			
			relationDf = relationDf.join(indexDf, col("id_fosunid").equalTo(col("fid")))
			                       .withColumnRenamed("index", "id")
			 			           .drop("fid")
			                       .join(indexDf, col("relates").equalTo(col("fid")))
			                       .withColumnRenamed("index", "relate");
			
			relationDf = relationDf.select(col("id"), col("relate"))
					               .withColumn("property", monotonically_increasing_id());

            logger.warn("relationDf: " );
			relationDf.show(20, false);
			
			JavaRDD<Edge<String>> edges = relationDf.javaRDD().map(
					                (row) -> {
					                return new Edge<String>(row.getLong(0), row.getLong(1), "1");}
			);
			
		    Graph<String,String> graph = Graph.fromEdges(edges.rdd(),
					                                     "1", 
					                                     StorageLevel.MEMORY_AND_DISK(), 
					                                     StorageLevel.MEMORY_AND_DISK(),
					                                     ClassTag$.MODULE$.apply(String.class),
					                                     ClassTag$.MODULE$.apply(String.class));

		    VertexRDD<Object> vertexRdd = graph.ops().connectedComponents().vertices();
		    logger.warn("vertexRdd: " + vertexRdd.take(10).toString());

		    JavaPairRDD<Long, Long> vertexPairRdd = vertexRdd.toJavaRDD()
		    		                                     .mapToPair((vertex)->
		    		                                      {return new Tuple2<Long, Long>((long)vertex._1, (long)vertex._2);});
			logger.warn("vertexPairRdd: " + vertexPairRdd.take(10).toString());
			
			JavaPairRDD<Long, String> connectedFidPairRdd = vertexPairRdd.join(indexPairRdd)
			             .values()
			             .mapToPair((vertex)->{return vertex;});

			logger.warn("connectedFidPairRdd: " + connectedFidPairRdd.take(10).toString());
			
			JavaPairRDD<Long, Iterable<String>> groupConnectedFidPairRdd = connectedFidPairRdd.groupByKey();
			
			JavaRDD<Tuple2<String, Iterable<String>>> fidRelatesRdd = connectedFidPairRdd.join(groupConnectedFidPairRdd).values();
			logger.warn("fidRelatesRdd: " + fidRelatesRdd.take(10).toString());

			JavaPairRDD<String, String> flatFidRelatesPairRdd = fidRelatesRdd.mapToPair((v)->{return v;})
					                                                         .flatMapValues((v)->{return v;});
			logger.warn("flatFidRelatesPairRdd: " + flatFidRelatesPairRdd.take(1).toString());

			JavaRDD<Tuple2<String, Row>> flatFidRelatesDataRdd
			                           = flatFidRelatesPairRdd.mapToPair(p->p.swap())
					                                          .join(profilePairRdd)
					                                          .values();
			logger.warn("flatFidRelatesDataRdd: " + flatFidRelatesDataRdd.take(10).toString());

			flatFidRelatesDataRdd.foreach(v -> {
			                      Map<String, String> map=new HashMap<String, String>();
			                      Row row = v._2;
			                      String [] fieldnames = row.schema().fieldNames();
			                      for(int i = 1; i<row.size(); i++){
			                    	  if(row.getAs(i) == null)
			                    		  continue;
			                          map.put(DATE+":"+fieldnames[i]+"."+row.getString(0), row.getAs(i));
			                      }
			                      if(map.isEmpty()==false)
//			                          HBaseOperator.newInstance().insert(FULLRELATIONDATA_TABLE, v._1, map);
									  logger.warn("map: " + map.toString());
			});
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Spark Job <" + APPNAME + "> Exception!");
		} finally {
			logger.info(APPNAME + " END ...");
		}
	}
}
