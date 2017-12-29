/*
Owner: Zhu Jie

PreCondition:
	HBase> disable 'zj:IndividualProfileJoined'
	       drop 'zj:IndividualProfileJoined'
	       create 'zj:IndividualProfileJoined', '201607'
	Hive> show create table individual_profile_joined;
	      select count(*) from individual_profile_joined;
	Hive> show create table fosunid_relation;
	      select count(*) from fosunid_relation;
	
Inputs:
	Hive: fonovadb.individual_profile
          fonovadb.super_id
          fonovadb.fosunid_relation
      
Outputs:
	HBase: zj:IndividualProfileJoined

PostContidion:
	Hive> select count(*) from individual_profile_joined;
	      ==
	      select count(*) from fosunid_relation;
		
*/

package correlator.main;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import correlator.util.HBaseOperator;
import java.util.HashMap;
import java.util.Map;
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

public class DataCorrelatorLaunch {
	private static String APPNAME = "Data Correlator";
	private static Logger logger = LoggerFactory.getLogger(DataCorrelatorLaunch.class);
	
	private static String PROFILE_TABLE = "SELECT * FROM individual_profile";
	private static String RELATION_TABLE = 
			"SELECT "
			    + "`id_fosunid`, "
			    + "`relates` "
		  + "FROM "
			    + "`fosunid_relation` "
		  + "LATERAL VIEW EXPLODE(id_fosunid_map) relatesTable "
		  + "AS relates, v";
	private static String FID_TABLE = "SELECT `id_fosunid` as `fid`, `source` FROM `super_id`";
	private static String FULLRELATIONDATA_TABLE = "IndividualProfileJoined";
	private static String COLUMNFAMILY = "201607";
	private static String HIVEDB = "use fonovadb";
	
	public static void main(String[] args) {
		
		logger.info(APPNAME + " START ...");
		SparkConf sparkConfig = new SparkConf();
		sparkConfig.setAppName(APPNAME);
		SparkSession spark = SparkSession.builder().config(sparkConfig).enableHiveSupport().getOrCreate();
		SparkContext sparkContext = spark.sparkContext();
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkContext);
		
		try {
			// Source Data Frame from HIVE
			spark.sql(HIVEDB);
			
			JavaPairRDD<String, Row> profilePairRdd = 
					spark.sql(PROFILE_TABLE)
					           .withColumnRenamed("source", "datasource")
					           .javaRDD()
					           .mapToPair(row ->
			                              new Tuple2<>(row.getString(0),row));
			System.out.println("------------profilePairRdd----------");
			System.out.println(profilePairRdd.count());
//			for(Object pair: profilePairRdd.top(10)){
//				System.out.println(pair);
//			}
			
			Dataset<Row> fidDf = spark.sql(FID_TABLE);
			Dataset<Row> indexDf = fidDf.withColumn("index", monotonically_increasing_id())
					                 .select(col("index"),
					                		 col("fid"));
			System.out.println("------------indexDf----------");
			System.out.println(indexDf.count());
			indexDf.printSchema();
			indexDf.show(20, false);

			JavaPairRDD<Long, String> indexPairRdd
			                        = indexDf.javaRDD()
			                                 .mapToPair(row ->
			                                            new Tuple2<>(row.getLong(0), row.getString(1)));
			
			Dataset<Row> relationDf = spark.sql(RELATION_TABLE);
			System.out.println("------------relationDf Before----------");
			System.out.println(relationDf.count());
			relationDf.printSchema();
			relationDf.show(20, false);
			relationDf = relationDf.join(indexDf, col("id_fosunid").equalTo(col("fid")))
			                       .withColumnRenamed("index", "id")
			 			           .drop("fid")
			                       .join(indexDf, col("relates").equalTo(col("fid")))
			                       .withColumnRenamed("index", "relate");
			relationDf = relationDf.select(col("id"), col("relate"));
			System.out.println("------------relationDf After----------");
			System.out.println(relationDf.count());
			relationDf.printSchema();
			relationDf.show(20, false);
			
			
			JavaRDD<Edge<String>> edges 
			                    = relationDf.javaRDD()
			                                .map(row -> 
			                                     new Edge<String>(row.getLong(0), row.getLong(1), "1"));
			System.out.println("------------Edges----------");
			System.out.println(edges.count());
			for(Edge<String> edge: edges.take(20)){
				System.out.println(edge);
			}
			
		    Graph<String, String> graph = Graph.fromEdges(edges.rdd(),
					                                      "1", 
					                                      StorageLevel.MEMORY_AND_DISK(), 
					                                      StorageLevel.MEMORY_AND_DISK(),
					                                      ClassTag$.MODULE$.apply(String.class),
					                                      ClassTag$.MODULE$.apply(String.class));
		    VertexRDD<Object> vertexRdd = graph.ops().connectedComponents().vertices();
		    JavaPairRDD<Long, Long> vertexPairRdd 
		                          = vertexRdd.toJavaRDD()
		    		                         .mapToPair(vertex -> 
		    		                                    new Tuple2<>((long)vertex._1, (long)vertex._2));
			System.out.println("------------Vertices----------");
			System.out.println(graph.vertices().count());
			for (Object vertex: graph.vertices().toJavaRDD().take(20)) {
            	System.out.println(vertex);
			}
			System.out.println("----------vertexPairRdd----------");
			System.out.println(vertexPairRdd.count());
			for (Object vertex: vertexPairRdd.take(20)) {
            	System.out.println(vertex);
			}
			
			JavaPairRDD<Long, String> connectedFidPairRdd
			                        = vertexPairRdd.join(indexPairRdd)
			                                        .values()
			                                        .mapToPair(vertex -> vertex);
			System.out.println("----------connectedFidPairRdd----------");
			System.out.println(connectedFidPairRdd.count());
			for (Object vertex: connectedFidPairRdd.take(20)) {
            	System.out.println(vertex);
			}
			
			JavaPairRDD<Long, Iterable<String>> groupConnectedFidPairRdd = connectedFidPairRdd.groupByKey();
			System.out.println("----------groupConnectedFidPairRdd----------");
			System.out.println(groupConnectedFidPairRdd.count());
			for (Object vertex: groupConnectedFidPairRdd.take(20)) {
            	System.out.println(vertex);
			}
			
			JavaRDD<Tuple2<String, Iterable<String>>> fidRelatesRdd
			                                        = connectedFidPairRdd.join(groupConnectedFidPairRdd).values();
			System.out.println("*************fidRelatesRdd************");
			System.out.println(fidRelatesRdd.count());
			for (Object vertex: fidRelatesRdd.take(20)) {
            	System.out.println(vertex);
			}

			JavaPairRDD<String, String> flatFidRelatePairRdd 
			                          = fidRelatesRdd.mapToPair(v -> v)
					                                 .flatMapValues(v-> v);
			System.out.println("----------flatFidRelatePairRdd----------");
			System.out.println(flatFidRelatePairRdd.count());
			for (Object v: flatFidRelatePairRdd.take(20)) {
            	System.out.println(v);
			}
			
			JavaRDD<Tuple2<String, Row>> flatFidRelateDataRdd
			                           = flatFidRelatePairRdd.mapToPair(p -> p.swap())
					                                         .join(profilePairRdd)
					                                         .values();
			System.out.println("----------flatFidRelateDataRdd----------");
			System.out.println(flatFidRelateDataRdd.count());
			for (Object v: flatFidRelateDataRdd.take(20)) {
            	System.out.println(v);
			}
			
			
			JavaPairRDD<String, String> fidSourcePairRdd 
			                          = fidDf.javaRDD()
			                                 .mapToPair(row -> 
			                                            new Tuple2<>(row.getString(0), row.getString(1)));
			
			JavaPairRDD<String, Tuple2<Row, String>> flatFidRelateDataSourceRdd
			                                       = flatFidRelateDataRdd.mapToPair(v -> v)
			                                                             .join(fidSourcePairRdd);
			System.out.println("----------flatFidRelateDataSourceRdd----------");
			System.out.println(flatFidRelateDataSourceRdd.count());
			for (Object v: flatFidRelateDataSourceRdd.take(20)) {
            	System.out.println(v);
			}
			
			flatFidRelateDataSourceRdd.foreach(pair -> {
                Map<String, String> map = new HashMap<String, String>();
                String fid = pair._1;
                String source = pair._2._2;
                Row dataRow = pair._2._1;
                String dataFid = dataRow.getString(0);
                String [] fieldnames = dataRow.schema().fieldNames();
                for(int i = 1; i<dataRow.size(); i++){
              	  if(dataRow.getAs(i) == null)
              		  continue;
                    map.put(COLUMNFAMILY+":"+fieldnames[i]+"."+dataFid, dataRow.getAs(i));
                }
                map.put(COLUMNFAMILY+":source", source);
                HBaseOperator.newInstance().insert(FULLRELATIONDATA_TABLE, fid, map);
                });
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Spark Job <" + APPNAME + "> Exception!");
		} finally {
		    sc.close();
		}
		logger.info(APPNAME + " END ...");
		
	}
}
