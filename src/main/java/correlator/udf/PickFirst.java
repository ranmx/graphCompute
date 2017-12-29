package correlator.udf;

import java.io.Serializable;
import java.util.List;
import org.apache.spark.sql.api.java.UDF1;
import scala.collection.Map;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.Buffer;

public class PickFirst  implements Serializable, UDF1<Map<String,String>, List<String>>{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1704620442742006772L;

	@Override
	public List<String> call(Map<String, String> field) throws Exception {
		
		if(field == null || field.size() == 0)
			return null;
		
		Buffer<String> b = new ArrayBuffer<String>();
		field.values().toSet().copyToBuffer(b);
        return scala.collection.JavaConversions.seqAsJavaList(b);
		
			
//		return field.values().iterator().next();
		
	}
}
