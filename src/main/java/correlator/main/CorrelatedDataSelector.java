/*
Owner: Zhu Jie

PreCondition:
	Hive> show create table fonovadb.individual_profile_joined;
	      select count(*) from individual_profile_joined;
	
Inputs:
	Hive: fonovadb.individual_profile_joined;
      
Outputs:
	Hive: fonovadb.Individual_Profile_Selected;

PostContidion:
	Hive> select count(*) from individual_profile_selected;
	
*/


package correlator.main;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import correlator.udf.DistinctWithoutJpush;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CorrelatedDataSelector {
	private static String APPNAME = "Correlated Data Selector";
	private static Logger logger = LoggerFactory.getLogger(CorrelatedDataSelector.class);
	
	private static String JOINED_PROFILE_TABLE = "SELECT * FROM `individual_profile_joined`";
	private static String OUTPUTTABLE = "Individual_Profile_Selected";
	private static String HIVEDB = "use fonovadb";

	public static void main(String[] args) {
		
		logger.info(APPNAME + " START ...");
		SparkConf sparkConfig = new SparkConf();
		sparkConfig.setAppName(APPNAME);
		SparkSession sparkSession = SparkSession.builder().config(sparkConfig).enableHiveSupport().getOrCreate();
		SparkContext sparkContext = sparkSession.sparkContext();
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkContext);
		
		try {
			// UDF Registration
			sparkSession.udf().register("DistinctWithoutJpush", new DistinctWithoutJpush(), DataTypes.createArrayType(DataTypes.StringType));
			// Source Data Frame from HIVE
			sparkSession.sql(HIVEDB);
			Dataset<Row> joinedProfileDf = sparkSession.sql(JOINED_PROFILE_TABLE);
			//System.out.println(joinedProfileDf.count());
			joinedProfileDf.printSchema();
			
			Dataset<Row> qualifiedProfileDf = joinedProfileDf.select(
					col("id_uid_fosunid").as("FosunID"),
					callUDF("DistinctWithoutJpush", col("ip_ibi_name"), col("datasource")).as("Name"), 
					callUDF("DistinctWithoutJpush", col("ip_ibi_gender"),col("datasource")).as("Gender"),
					callUDF("DistinctWithoutJpush", col("ip_ibi_birth"), col("datasource")).as("Birth"), 
					callUDF("DistinctWithoutJpush", col("ip_ibi_age"),col("datasource")).as("Age"),
					callUDF("DistinctWithoutJpush", col("ip_ibi_decade"), col("datasource")).as("Decade"), 
					callUDF("DistinctWithoutJpush", col("ip_ibi_national"),col("datasource")).as("National"),
					callUDF("DistinctWithoutJpush", col("ip_ibi_stage"), col("datasource")).as("Stage"), 
					callUDF("DistinctWithoutJpush", col("ip_ibi_married"),col("datasource")).as("Married"),
					callUDF("DistinctWithoutJpush", col("ip_ibi_address"), col("datasource")).as("Address"), 
					callUDF("DistinctWithoutJpush", col("ip_ibi_province"),col("datasource")).as("Province"),
					callUDF("DistinctWithoutJpush", col("ip_ibi_city"), col("datasource")).as("City"), 
					callUDF("DistinctWithoutJpush", col("ip_devc_os"),col("datasource")).as("Device_OS"),
					col("datasource"),
					col("mode"),
					col("tablename"),
					col("source")
					);
			//System.out.println(qualifiedProfileDf.count());
			qualifiedProfileDf.printSchema();
			
			Dataset<Row> singleProfileDf = qualifiedProfileDf.filter("Name is null OR size(Name)<2")
				                                          .filter("Gender is null OR size(Gender)<2")
				                                          .filter("Age is null OR size(Age)<2")
				                                          .filter("Decade is null OR size(Decade)<2")
				                                          .filter("National is null OR size(National)<2")
				                                          .filter("Stage is null OR size(Stage)<2")
				                                          .filter("Married is null OR size(Married)<2")
				                                          .filter("Address is null OR size(Address)<2")
				                                          .filter("Province is null OR size(Province)<2")
				                                          .filter("City is null OR size(City)<2")
				                                          .filter("Device_OS is null OR size(Device_OS)<2");
			//System.out.println(singleProfileDf.count());
			singleProfileDf.printSchema();
			
			//qualifiedProfileDf.write().mode(SaveMode.Overwrite).saveAsTable(OUTPUTTABLE);	
			singleProfileDf.write().mode(SaveMode.Overwrite).saveAsTable(OUTPUTTABLE);
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Spark Job <" + APPNAME + "> Exception!");
		} finally {
		    sc.close();
		}
		logger.info(APPNAME + " END ...");
	}
}
