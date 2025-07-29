package hs.jjm;

import java.io.IOException;
import java.io.InputStream;

public class Utils {
  public static void readFully(
      InputStream src,
      byte[] dest,
      int destOffset,
      int length)
      throws IOException {
    int read = 0;
    while (read < length) {
      read += src.read(dest, destOffset + read, length - read);
    }
  }
}
