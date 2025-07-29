package hs.jjm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * [MAGIC_NUMBER]: 1 Byte
 * [CP-SN]: 8 Bytes
 * [LENGTH_OF_WAL_ENTRY_CONTENT]: 4 Bytes
 * [WAL_ENTRY_CONTENT]: variable Bytes
 */
public class WALEntry {
  public static final byte MAGIC_NUMBER = 1;

  private final long cpSn;
  private final byte[] content;

  public WALEntry(long cpSn, byte[] content) {
    this.cpSn = cpSn;
    this.content = content;
  }

  public long getCpSn() {
    return cpSn;
  }

  public byte[] getContent() {
    return content;
  }

  public byte[] serialize() {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    final DataOutputStream dout = new DataOutputStream(bout);
    try {
      dout.writeByte(MAGIC_NUMBER);
      dout.writeLong(cpSn);
      dout.writeInt(content.length);
      dout.write(content);
      dout.flush();
    } catch (IOException e) {
      System.err.println("WALEntry.serialize: Impossible to come here");
      e.printStackTrace(System.err);
      System.exit(1);
    }
    return bout.toByteArray();
  }

  public static WALEntry deserialize(final byte[] bytes) {
    if (bytes.length < 13) {
      throw new IllegalArgumentException("WALEntry.deserialize: size is less than 13");
    }
    if (bytes[0] != MAGIC_NUMBER) {
      throw new IllegalArgumentException("WALEntry.deserialize: magic number is not 1");
    }
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    DataInputStream din = new DataInputStream(bin);
    try {
      din.readByte();
      long cpSn = din.readLong();
      int length = din.readInt();
      if (length != bytes.length - 13) {
        throw new IllegalArgumentException("WALEntry.deserialize: length is not correct");
      }
      byte[] content = new byte[length];
      Utils.readFully(din, content, 0, length);
      return new WALEntry(cpSn, content);
    } catch (IOException e) {
      System.err.println("WALEntry.deserialize: Impossible to come here");
      e.printStackTrace(System.err);
      System.exit(1);
      throw new Error();
    }
  }
}
