package correlator.main;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import correlator.util.HBaseOperator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag$;

public class Zjtest {
	private static String APPNAME = "Zjtest Data Correlator";
	private static Logger logger = LoggerFactory.getLogger(Zjtest.class);
	
	//private static String RELATIONTABLE = "select id_fosunid, relates from tmp_fosunid_relation LATERAL VIEW explode(id_fosunid_map) relatesTable AS relates, v limit 10";
	private static String PROFILE_TABLE = "SELECT `id_uid_fosunid`,`ip_ibi_province`,`ip_ibi_age`,`ip_ibi_city` FROM `individual_profile`";
	private static String HIVE_TABLE = "SELECT `id_uid_fosunid` FROM `individual_profile`";
	private static String MYSQL_URL = "jdbc:mysql://10.151.2.2:33061/test";
	private static String DATE = "201607";

	private static Properties CONN_PROPERTIES = null;

	static {
		CONN_PROPERTIES = new Properties();
		CONN_PROPERTIES.put("user", "dmp_uat_fonova");
		CONN_PROPERTIES.put("password", "dmp_uat_fonova");
		CONN_PROPERTIES.put("useUnicode", "true");
		CONN_PROPERTIES.put("characterEncoding", "UTF-8");
	}
	
	public static void main(String[] args) {
		
		logger.info(APPNAME + " START ...");
		SparkConf sparkConfig = new SparkConf();
		sparkConfig.setAppName(APPNAME);
		SparkSession sparkSession = SparkSession.builder().config(sparkConfig).enableHiveSupport().getOrCreate();
		SparkContext sparkContext = sparkSession.sparkContext();
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkContext);

		try {
			// Source Data Frame from HIVE
			sparkSession.sql("use fonovadb");
			Dataset<Row> dataDf = sparkSession.sql(HIVE_TABLE);
			
			JavaPairRDD<String, Row> profilePairRdd = 
					sparkSession.sql(PROFILE_TABLE)
					           .javaRDD()
					           .mapToPair(row->
			                   {return new Tuple2<String, Row>(row.getString(0),row);});
//			System.out.println("------------profilePairRdd----------");
//			for(Object pair: profilePairRdd.top(10)){
//				System.out.println(pair);
//			}
			
			
			Dataset<Row> indexDf = dataDf.select(col("id_uid_fosunid").as("fid"))
			                          .withColumn("index", monotonically_increasing_id());
			indexDf.printSchema();
			indexDf.show(20, false);
			System.out.println(indexDf.count());
			JavaPairRDD<Long, String> indexPairRdd = indexDf.javaRDD().mapToPair(row->
			                            {return new Tuple2<>(row.getLong(1), row.getString(0));});
			
			
			Dataset<Row>relationDf = sparkSession.read().jdbc(MYSQL_URL, "zjtest", CONN_PROPERTIES);
			relationDf.printSchema();
			relationDf.show(20, false);
			
			relationDf = relationDf.join(indexDf, col("text").equalTo(col("fid")))
			          .withColumnRenamed("index", "id")
			          .drop("fid")
			          .join(indexDf, col("secert").equalTo(col("fid")))
			          .withColumnRenamed("index", "relates");
			
			relationDf = relationDf.select(col("id"), col("relates"))
					               .withColumn("property", monotonically_increasing_id());
			
			relationDf.printSchema();
			relationDf.show(20, false);
			
			
			JavaRDD<Edge<String>> edges = relationDf.javaRDD().map(
					                (row) -> {
					                return new Edge<String>(row.getLong(0), row.getLong(1), "1");}
			);
			System.out.println("------------Edges----------");
			for(Edge<String> edge: edges.take(20)){
				System.out.println(edge);
			}
			
		    Graph<String,String> graph = Graph.fromEdges(edges.rdd(),
					                                     "1", 
					                                     StorageLevel.MEMORY_AND_DISK(), 
					                                     StorageLevel.MEMORY_AND_DISK(),
					                                     ClassTag$.MODULE$.apply(String.class),
					                                     ClassTag$.MODULE$.apply(String.class));
		    VertexRDD<Object> vertexRdd = graph.ops().connectedComponents().vertices();
		    JavaPairRDD<Long, Long> vertexPairRdd = vertexRdd.toJavaRDD()
		    		                                     .mapToPair((vertex)->
		    		                                      {return new Tuple2<Long, Long>((long)vertex._1, (long)vertex._2);});
			System.out.println("------------Vertices----------");
			for (Object vertex: graph.vertices().toJavaRDD().take(20)) {
            	System.out.println(vertex);
			}
			System.out.println("----------vertexPairRdd----------");
			for (Object vertex: vertexPairRdd.take(20)) {
            	System.out.println(vertex);
			}
			
			JavaPairRDD<Long, String> connectedFidPairRdd = vertexPairRdd.join(indexPairRdd)
			             .values()
			             .mapToPair((vertex)->{return vertex;});
			System.out.println("----------connectedFidPairRdd----------");
			for (Object vertex: connectedFidPairRdd.take(20)) {
            	System.out.println(vertex);
			}
			
			JavaPairRDD<Long, Iterable<String>> groupConnectedFidPairRdd = connectedFidPairRdd.groupByKey();
			System.out.println("----------groupConnectedFidPairRdd----------");
			for (Object vertex: groupConnectedFidPairRdd.take(20)) {
            	System.out.println(vertex);
			}
			
			JavaRDD<Tuple2<String, Iterable<String>>> fidRelatesRdd = connectedFidPairRdd.join(groupConnectedFidPairRdd).values();
			System.out.println("*************fidRelatesRdd************");
			for (Object vertex: fidRelatesRdd.take(20)) {
            	System.out.println(vertex);
			}

			JavaPairRDD<String, String> flatFidRelatesPairRdd = fidRelatesRdd.mapToPair((v)->{return v;})
					                                                         .flatMapValues((v)->{return v;});
			System.out.println("----------flatFidRelatesPairRdd----------");
			for (Object v: flatFidRelatesPairRdd.take(20)) {
            	System.out.println(v);
			}
			
			JavaRDD<Tuple2<String, Row>> flatFidRelatesDataRdd
			                           = flatFidRelatesPairRdd.mapToPair(p->p.swap())
					                                          .join(profilePairRdd)
					                                          .values();
			System.out.println("----------flatFidRelatesDataRdd----------");
			for (Object v: flatFidRelatesDataRdd.take(20)) {
            	System.out.println(v);
			}
			
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
			                          HBaseOperator.newInstance().insert("zjdatatable", v._1, map);
			;});
			
			
			
			
			
			//=================================================================================
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Spark Job <" + APPNAME + "> Exception!");
		} finally {
		    sc.close();
		}
		logger.info(APPNAME + " END ...");
		
	}
}
