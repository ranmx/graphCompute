import org.junit.Test;

import java.net.InetAddress;
import java.net.NetworkInterface;

/**
 * Created by ranmx on 2018/1/12.
 */
public class key_test {
  @Test
  public void appTest() {
    try {
      BasicEntityIdGenerator basicEntityIdGenerator = new BasicEntityIdGenerator();
      System.out.println("fid: " + basicEntityIdGenerator.generateId());
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void hashLongTest() {
    System.out.println("aab: " + hash("aab"));
    System.out.println("aaa: " + hash("aaa"));
    System.out.println("aaaa: " + hash("aaaa"));
    System.out.println("bbbb: " + hash("bbbb"));

    System.out.println("--------------------------: " + hash("--------------------------"));

    System.out.println();

  }

  public static long hash(String string) {
    long h = 1125899906842597L; // prime
    int len = string.length();

    for (int i = 0; i < len; i++) {
      h = 31*h + string.charAt(i);
    }

    return h;
  }
}




