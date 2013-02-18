package com.orientechnologies.orient.core.storage.impl.local.eh;

import java.io.IOException;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.memory.eh.OEHNodeMetadata;

/**
 * @author Andrey Lomakin
 * @since 11.02.13
 */
public class OEHTreeStateStore extends OSingleFileSegment {
  private static final int   RECORD_SIZE   = 256 * OLongSerializer.LONG_SIZE + 3 * OByteSerializer.BYTE_SIZE;

  public static final String DEF_EXTENSION = ".oet";

  public OEHTreeStateStore(String iPath, String iType) throws IOException {
    super(iPath, iType);
  }

  public OEHTreeStateStore(OStorageLocal iStorage, OStorageFileConfiguration iConfig) throws IOException {
    super(iStorage, iConfig);
  }

  public OEHTreeStateStore(OStorageLocal iStorage, OStorageFileConfiguration iConfig, String iType) throws IOException {
    super(iStorage, iConfig, iType);
  }

  public void setHashTreeSize(long hashTreeSize) throws IOException {
    file.writeHeaderLong(0, hashTreeSize);
  }

  public long getHashTreeSize() throws IOException {
    return file.readHeaderLong(0);
  }

  public void storeTreeState(int index, long[] node, OEHNodeMetadata nodeMetadata) throws IOException {
    long nodePosition = index * RECORD_SIZE;
    if (getFilledUpTo() < nodePosition + RECORD_SIZE)
      file.allocateSpace((int) (nodePosition + RECORD_SIZE - file.getFilledUpTo()));

    byte[] nodeContentBuffer = new byte[RECORD_SIZE];
    int offset = 0;

    for (long position : node) {
      OLongSerializer.INSTANCE.serializeNative(position, nodeContentBuffer, offset);
      offset += OLongSerializer.LONG_SIZE;
    }

    nodeContentBuffer[offset++] = (byte) nodeMetadata.getMaxLeftChildDepth();
    nodeContentBuffer[offset++] = (byte) nodeMetadata.getMaxRightChildDepth();
    nodeContentBuffer[offset] = (byte) nodeMetadata.getNodeLocalDepth();

    file.write(nodePosition, nodeContentBuffer);
  }

  public long[] loadTreeNode(int index) throws IOException {
    long nodePosition = index * RECORD_SIZE;
    byte[] nodeContentBuffer = new byte[256 * OLongSerializer.LONG_SIZE];
    file.read(nodePosition, nodeContentBuffer, nodeContentBuffer.length);

    long[] node = new long[256];
    for (int i = 0; i < node.length; i++)
      node[i] = OLongSerializer.INSTANCE.deserializeNative(nodeContentBuffer, i * OLongSerializer.LONG_SIZE);

    return node;
  }

  public OEHNodeMetadata loadMetadata(int index) throws IOException {
    long nodePosition = index * RECORD_SIZE + 256 * OLongSerializer.LONG_SIZE;
    byte[] nodeContentBuffer = new byte[3];

    file.read(nodePosition, nodeContentBuffer, 3);

    return new OEHNodeMetadata(nodeContentBuffer[0], nodeContentBuffer[1], nodeContentBuffer[2]);
  }
}
