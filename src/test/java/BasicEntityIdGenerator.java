import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;

/**
 * Created by ranmx on 2018/1/12.
 */
public class BasicEntityIdGenerator {

  // id format =>
  // |sequence|timestamp|datacenter
  // 12 | 41 | 10
  private final long timestampbits = 41L;
  private final long datacenterIdBits = 10L;
  private final long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);

  private final long sequenceLeftShift = timestampbits + datacenterIdBits;
  private final long timestampLeftShift = datacenterIdBits;

  private final long twepoch = 1288834974657L;
  private final long datacenterId;


  public BasicEntityIdGenerator() throws Exception {
    datacenterId = getDatacenterId();
    if (datacenterId > maxDatacenterId || datacenterId < 0) {
      throw new RuntimeException("datacenterId > maxDatacenterId");
    }
  }

  public synchronized Long generateId() throws Exception {
    long timestamp = System.currentTimeMillis();
    System.out.println("timestamp: " + timestamp);

    long sequenceLong = (int)(1+Math.random()*4096);
    System.out.println("sequenceLong: " + sequenceLong);
    System.out.println("(-1L << datacenterIdBits): " + (-1L << datacenterIdBits));
    System.out.println("maxDatacenterId: " + maxDatacenterId);
    System.out.println("sequenceLeftShift: " + sequenceLeftShift);
    System.out.println("timestampLeftShift: " + timestampLeftShift);
    System.out.println("datacenterId: " + datacenterId);
    System.out.println("sequenceLong: " + sequenceLong);

    Long id =
      sequenceLong << sequenceLeftShift | ((timestamp - twepoch) << timestampLeftShift)
        | datacenterId;

    System.out.println("sequenceLong << sequenceLeftShift: " + (sequenceLong << sequenceLeftShift));
    System.out.println("(timestamp - twepoch): " + (timestamp - twepoch));
    System.out.println("((timestamp - twepoch) << timestampLeftShift): " + ((timestamp - twepoch) << timestampLeftShift));
    System.out.println(" sequenceLong << sequenceLeftShift | ((timestamp - twepoch) << timestampLeftShift): " + ( sequenceLong << sequenceLeftShift | ((timestamp - twepoch) << timestampLeftShift)));
    System.out.println("generateId: " + id);
    return id;
  }

  private long getDatacenterId() throws Exception {
    InetAddress ip = InetAddress.getLocalHost();
    System.out.println("ip: " + ip);

    NetworkInterface network = NetworkInterface.getByInetAddress(ip);
    long id;
    if (network == null) {
      id = 1;
    } else {
      byte[] mac = network.getHardwareAddress();
      id =((0x00FF & mac[mac.length - 1]) | (0xFF00 & ((mac[mac.length - 2]) << 8))) >> 6;
      System.out.println("mac: " + Arrays.toString(mac));
      System.out.println("mac[mac.length - 1]: " + mac[mac.length - 1]);
      System.out.println("(0x00FF & mac[mac.length - 1]): " + (0x00FF & mac[mac.length - 1]));
      System.out.println("(mac[mac.length - 2]): " + (mac[mac.length - 2]));
      System.out.println("((mac[mac.length - 2]) << 8): " + ((mac[mac.length - 2]) << 8));
      System.out.println("(0xFF00 & ((mac[mac.length - 2]) << 8)): " + (0xFF00 & ((mac[mac.length - 2]) << 8)));
      System.out.println("((0x00FF & mac[mac.length - 1]) | (0xFF00 & ((mac[mac.length - 2]) << 8))): " + ((0x00FF & mac[mac.length - 1]) | (0xFF00 & ((mac[mac.length - 2]) << 8))));
      System.out.println("getDatacenterId: " + id);
    }
    return id;
  }

}
