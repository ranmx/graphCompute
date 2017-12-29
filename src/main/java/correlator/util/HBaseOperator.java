package correlator.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseOperator {

  private HBaseOperator() {};

  private static HBaseOperator hbaseHelper = new HBaseOperator();

  public static HBaseOperator newInstance() {
    return hbaseHelper;
  }

  /**
   * @Title: getConn
   * @Description: 获取连接
   * @return
   */
  private Connection getConn() {
    return HbaseConnectionPool.getConnection();
  }

  /**
   * @Title: getAdmin
   * @Description: 获取admin
   * @return
   * @throws IOException
   */
  public Admin getAdmin() throws IOException {
    Connection connection = getConn();
    return connection.getAdmin();
  }

  /**
   * @Title: existsTable
   * @Description: 判断表是否存在
   * @param table: 表名
   * @return boolean
   */
  public boolean existsTable(String table) {
    return existsTable(TableName.valueOf(table));
  }

  /**
   * @Title: existsTable
   * @Description: 判断表是否存在
   * @param table: 表名
   * @return boolean
   * @throws IOException
   */
  public boolean existsTable(TableName table) {
    Admin admin = null;
    try {
      admin = getAdmin();
      return admin.tableExists(table);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      close(admin, null, null);
    }
    return false;
  }

  /**
   * @Title: createTable
   * @Description: 创建表
   * @param table: 表名
   * @param colfams: 列族列表表
   * @return boolean
   * @throws IOException
   */
  public void createTable(String table, String... colfams) throws IOException {
    createTable(TableName.valueOf(table), 1, null, colfams);
  }

  /**
   * @Title: createTable
   * @Description: 创建表
   * @param table: 表名
   * @param colfams: 列族列表
   * @return boolean
   * @throws IOException
   */
  public void createTable(TableName table, String... colfams) throws IOException {
    createTable(table, 1, null, colfams);
  }

  /**
   * @Title: createTable
   * @Description: 创建表
   * @param table: 表名
   * @param maxVersions: 最大版本号
   * @param colfams: 列族列表
   * @return boolean
   * @throws IOException
   */
  public void createTable(String table, int maxVersions, String... colfams) throws IOException {
    createTable(TableName.valueOf(table), maxVersions, null, colfams);
  }

  /**
   * @Title: createTable
   * @Description: 创建表
   * @param table: 表名
   * @param maxVersions: 最大版本号
   * @param splitKeys: 分割符数组
   * @param colfams: 列族列表
   * @return boolean
   * @throws IOException
   */
  public void createTable(TableName table, int maxVersions, byte[][] splitKeys, String... colfams)
      throws IOException {
    HTableDescriptor desc = new HTableDescriptor(table);
    for (String cf : colfams) {
      HColumnDescriptor coldef = new HColumnDescriptor(cf);
      coldef.setMaxVersions(maxVersions);
      desc.addFamily(coldef);
    }
    Admin admin = null;
    try {
      admin = getAdmin();
      if (splitKeys != null) {
        admin.createTable(desc, splitKeys);
      } else {
        admin.createTable(desc);
      }
    } finally {
      close(admin, null, null);
    }
  }

  /**
   * @Title: disableTable
   * @Description: 使表失效(删除记录及表结构前要先调用)
   * @param table: 表名
   * @return
   * @throws IOException
   */
  public void disableTable(String table) throws IOException {
    disableTable(TableName.valueOf(table));
  }

  /**
   * @Title: disableTable
   * @Description: 使表失效(删除记录及表结构前要先调用)
   * @param table: 表名
   * @return
   * @throws IOException
   */
  public void disableTable(TableName table) throws IOException {
    Admin admin = null;
    try {
      admin = getAdmin();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      close(admin, null, null);
    }
    admin.disableTable(table);
  }

  /**
   * @Title: dropTable
   * @Description: 删除记录及表结构
   * @param table: 表名
   * @return
   * @throws IOException
   */
  public void dropTable(String table) throws IOException {
    dropTable(TableName.valueOf(table));
  }

  /**
   * @Title: dropTable
   * @Description: 删除记录及表结构
   * @param table: 表名
   * @return
   * @throws IOException
   */
  public void dropTable(TableName table) throws IOException {
    Admin admin = null;
    try {
      admin = getAdmin();
      if (existsTable(table)) {
        if (admin.isTableEnabled(table))
          disableTable(table);
        admin.deleteTable(table);
      }
    } finally {
      close(admin, null, null);
    }
  }


  /**
   * @Title: queryAll
   * @Description: 根据表名查出所有记录
   * @param table: 表名
   * @return
   */
  public List<Map<String, String>> queryAll(String table) {
    List<Map<String, String>> results = new ArrayList<Map<String, String>>();
    Map<String, String> map = null;
    Table t = null;
    try {
      t = getConn().getTable(TableName.valueOf(table));
      ResultScanner scanner = t.getScanner(new Scan());
      for (Result result : scanner) {
        map = new HashMap<String, String>();
        String rowkey = Bytes.toString(result.getRow());
        map.put("rowkey", rowkey);
        for (Cell cell : result.rawCells()) {
          map.put(
              Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                  cell.getQualifierLength()),
              Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        results.add(map);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      close(null, t, null);
    }
    return results;
  }

  /**
   * @Title: queryByRowkeys
   * @Description: 根据行标识符数组查询记录
   * @param table: 表名
   * @param rowkeys: 标识符数组
   * @return
   */
  public List<Map<String, String>> queryByRowkeys(String table, String[] rowkeys) {
    Table tbl = null;
    try {
      tbl = getConn().getTable(TableName.valueOf(table));
      List<Get> gets = new ArrayList<Get>();
      for (String row : rowkeys) {
        Get get = new Get(Bytes.toBytes(row));
        get.setMaxVersions();
        gets.add(get);
      }
      Result[] results = tbl.get(gets);
      return converResults2List(results);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      close(null, tbl, null);
    }
    return null;
  }

  /**
   * @Title: queryByRowkey
   * @Description: 根据行标识符查询记录
   * @param table: 表名
   * @param rowkey: 标识符
   * @return
   */
  public Map<String, String> queryByRowkey(String table, String rowkey) {
    Table tbl = null;
    try {
      tbl = getConn().getTable(TableName.valueOf(table));
      Result results = tbl.get(new Get(rowkey.getBytes()));
      return converResult2Map(results);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      close(null, tbl, null);
    }
    return null;
  }

  /**
   * @Title: converResults2List
   * @Description: 将Result转换成Map
   * @param result: 单个记录
   * @return
   */
  private Map<String, String> converResult2Map(Result result) {
    Map<String, String> map = null;
    map = new HashMap<String, String>();
    String rowkey = Bytes.toString(result.getRow());
    map.put("rowkey", rowkey);
    for (Cell cell : result.rawCells()) {
      map.put(
          Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
              cell.getQualifierLength()),
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    }
    return map;
  }

  /**
   * @Title: converResults2List
   * @Description: 将Results转换成List
   * @param results: 多个记录
   * @return
   */
  private List<Map<String, String>> converResults2List(Result[] results) {
    List<Map<String, String>> list = new ArrayList<Map<String, String>>();
    Map<String, String> map = null;
    for (Result result : results) {
      map = new HashMap<String, String>();
      String rowkey = Bytes.toString(result.getRow());
      map.put("rowkey", rowkey);
      for (Cell cell : result.rawCells()) {
        map.put(
            Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                cell.getQualifierLength()),
            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
      }
      list.add(map);
    }
    return list;
  }

  /**
   * @Title: insert
   * @Description :将Map对象插入到表中
   * @param tableName :表名
   * @param list :要插入的Map对象p
   * @return
   * @throws IOException
   */
  public void insert(String tableName, List<Map<String, String>> list) throws IOException {
    for (Map<String, String> map : list) {
      insert(tableName, map);
    }
  }

  /**
   * @Title: insert
   * @Description: 将Map对象插入到表中
   * @param tableName:表名
   * @param map: 要插入的Map对象p
   * @return
   * @throws IOException
   */
  public void insert(String tableName, Map<String, String> map) throws IOException {
    Table tbl = null;
    List<Put> puts = new ArrayList<Put>();
    try {
      tbl = getConn().getTable(TableName.valueOf(tableName));
      String rowkey = "rowkey";
      Put put = null;
      for (String key : map.keySet()) {
        rowkey = map.get("rowkey");
        if (!key.equals("rowkey")) {
          put = new Put(Bytes.toBytes(rowkey));
          put.addColumn(Bytes.toBytes(key.split(":")[0]), Bytes.toBytes(key.split(":")[1]),
              Bytes.toBytes(map.get(key)));
          puts.add(put);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      close(null, tbl, null);
    }
    tbl.put(puts);
  }

  /**
   * @Title: insert
   * @Description: 插入(多行)
   * @param rowKey: 行标识符
   * @param map: 插入的数据
   * @return
   * @throws IOException
   */
  public void insert(TableName tableName, String rowKey, Map<String, String> map)
      throws IOException {
    Table table = null;
    Put put = null;
    try {
      table = getConn().getTable(tableName);
      Iterator<String> iter = map.keySet().iterator();
      String key = "";
      String[] famAndQual;
      put = new Put(Bytes.toBytes(rowKey));
      while (iter.hasNext()) {
        key = iter.next().toString();
        famAndQual = key.split(":");
        put.addColumn(Bytes.toBytes(famAndQual[0]), Bytes.toBytes(famAndQual[1]),
            Bytes.toBytes(map.get(key)));
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      close(null, table, null);
    }
    table.put(put);
  }

  /**
   * @Title: insert
   * @Description: 插入(多行)
   * @param tableName: 表名
   * @param rowKey: 行标识符
   * @param map: 插入的数据
   * @return
   * @throws IOException
   */
  public void insert(String tableName, String rowKey, Map<String, String> map) throws IOException {
    insert(TableName.valueOf(tableName), rowKey, map);
  }



  /**
   * @Title: close
   * @Description: 关闭admin或table
   * @param admin
   * @param table
   */
  public void close(Admin admin, Table table, Connection conn) {
    if (null != admin) {
      try {
        admin.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (null != table) {
      try {
        table.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (null != conn) {
      try {
        conn.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }



  public List<String> queryAllTables() {
    List<String> all = new ArrayList<String>();
    try {
      HTableDescriptor[] listTables = getConn().getAdmin().listTables();
      for (HTableDescriptor h : listTables) {
        all.add(h.getNameAsString());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return all;
  }
}
