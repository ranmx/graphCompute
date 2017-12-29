package correlator.udf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.api.java.UDF2;
import scala.collection.JavaConversions;
import scala.collection.Map;

public class DistinctWithoutJpush  implements Serializable, UDF2<Map<String,String>, Map<String,String>, List<String>> {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4324307552565998736L;

	@Override
	public List<String> call(Map<String, String> field, Map<String, String> source) throws Exception {
		
		if(field == null || field.size() == 0)
			return null;
		
		// Single Value
		if(field.size() == 1)
			return JavaConversions.seqAsJavaList(field.values().toSeq());
		
	    //Multiple Values
		java.util.Map<String, String> jField = JavaConversions.mapAsJavaMap(field);
		java.util.Map<String, String> jSource = JavaConversions.mapAsJavaMap(source);
		Set<String> distinctValues = new HashSet<String>();
		
		// Remove Jpush Value When Conflict
		for(String key: jField.keySet()){
			if(jSource.getOrDefault(key, "") != "Jpush"){
			    distinctValues.add(jField.get(key));
			}
		}
		
		return new ArrayList<String>(distinctValues);
				
/*      Set<String> distinctValues = field.values().toSet();
 		Iterator<String> iter = field.keysIterator();
        while(iter.hasNext()){        	
        }
		Buffer<String> buf = new ArrayBuffer<String>();		
		field.values().toSet().copyToBuffer(buf);
        return scala.collection.JavaConversions.asJavaList(buf);*/
	}
}
