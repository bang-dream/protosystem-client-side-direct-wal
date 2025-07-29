package hs.jjm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * [MAGIC_NUMBER]: 1 Byte
 * [P-SN]: 8 Bytes
 * [CP-SN]: 8 Bytes
 */
public class PCpMapMsg {
  public static final byte MAGIC_NUMBER = 2;

  private final long pSn;
  private final long cpSn;

  public PCpMapMsg(long pSn, long cpSn) {
    this.pSn = pSn;
    this.cpSn = cpSn;
  }

  public long getPSn() {
    return pSn;
  }

  public long getCpSn() {
    return cpSn;
  }

  public byte[] serialize() {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    final DataOutputStream dout = new DataOutputStream(bout);
    try {
      dout.writeByte(MAGIC_NUMBER);
      dout.writeLong(pSn);
      dout.writeLong(cpSn);
      dout.flush();
    } catch (IOException e) {
      System.err.println("PCpMapMsg.serialize: Impossible to come here");
      e.printStackTrace(System.err);
      System.exit(1);
    }
    return bout.toByteArray();
  }

  public PCpMapMsg deserialize(final byte[] bytes) {
    if (bytes.length != 17) {
      throw new IllegalArgumentException("PCpMapMsg.deserialize: size is not 17");
    }
    if (bytes[0] != MAGIC_NUMBER) {
      throw new IllegalArgumentException("PCpMapMsg.deserialize: magic number is not 2");
    }
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    DataInputStream din = new DataInputStream(bin);
    try {
      din.readByte();
      long pSn = din.readLong();
      long cpSn = din.readLong();
      return new PCpMapMsg(pSn, cpSn);
    } catch (IOException e) {
      System.err.println("PCpMapMsg.deserialize: Impossible to come here");
      e.printStackTrace(System.err);
      System.exit(1);
      throw new Error();
    }
  }
}
