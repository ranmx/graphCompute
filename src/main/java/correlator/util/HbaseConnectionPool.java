package correlator.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HbaseConnectionPool {

  private static Connection connection;

  private static Logger logger = LoggerFactory.getLogger(HbaseConnectionPool.class);

  private HbaseConnectionPool() {};

  private static HbaseConnectionPool hbaseConnectionPool = new HbaseConnectionPool();

  public static HbaseConnectionPool newInstance() {
    return hbaseConnectionPool;
  }

  static {
    Configuration config = HBaseConfiguration.create();
    try {
      connection = ConnectionFactory.createConnection(config);
    } catch (IOException e) {
      logger.error("init hbaseConnectionPool fail |", e);
    }
  }

  public static Connection getConnection() {
    return connection;
  }
}
