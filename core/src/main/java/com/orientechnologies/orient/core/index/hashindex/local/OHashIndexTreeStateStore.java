package com.orientechnologies.orient.core.index.hashindex.local;

import java.io.IOException;

import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin
 * @since 11.02.13
 */
public class OHashIndexTreeStateStore extends OSingleFileSegment {
  private static final int RECORD_SIZE = OLocalHashTable.MAX_LEVEL_SIZE * OLongSerializer.LONG_SIZE + 4 * OByteSerializer.BYTE_SIZE;

  public OHashIndexTreeStateStore(String iPath, String iType) throws IOException {
    super(iPath, iType);
  }

  public OHashIndexTreeStateStore(OStorageLocalAbstract iStorage, OStorageFileConfiguration iConfig) throws IOException {
    super(iStorage, iConfig);
  }

  public OHashIndexTreeStateStore(OStorageLocalAbstract iStorage, OStorageFileConfiguration iConfig, String iType)
      throws IOException {
    super(iStorage, iConfig, iType);
  }

  public void setHashTreeSize(long hashTreeSize) throws IOException {
    file.writeHeaderLong(0, hashTreeSize);
  }

  public long getHashTreeSize() throws IOException {
    return file.readHeaderLong(0);
  }

  public void setHashTreeTombstone(long hashTreeTombstone) throws IOException {
    file.writeHeaderLong(OLongSerializer.LONG_SIZE, hashTreeTombstone);
  }

  public long getHashTreeTombstone() throws IOException {
    return file.readHeaderLong(OLongSerializer.LONG_SIZE);
  }

  public void setBucketTombstonePointer(long bucketTombstonePointer) throws IOException {
    file.writeHeaderLong(2 * OLongSerializer.LONG_SIZE, bucketTombstonePointer);
  }

  public long getBucketTombstonePointer() throws IOException {
    return file.readHeaderLong(2 * OLongSerializer.LONG_SIZE);
  }

  public void storeTreeState(long[][] hashTree, OHashTreeNodeMetadata[] nodesMetadata) throws IOException {
    truncate();

    file.allocateSpace(hashTree.length * RECORD_SIZE);
    long filePosition = 0;
    for (int i = 0; i < hashTree.length; i++) {
      long[] node = hashTree[i];
      byte[] nodeContentBuffer = new byte[RECORD_SIZE];
      int offset = 0;

      if (node != null) {
        OBooleanSerializer.INSTANCE.serializeNative(true, nodeContentBuffer, offset);
        offset++;

        for (long position : node) {
          OLongSerializer.INSTANCE.serializeNative(position, nodeContentBuffer, offset);
          offset += OLongSerializer.LONG_SIZE;
        }

        OHashTreeNodeMetadata nodeMetadata = nodesMetadata[i];
        nodeContentBuffer[offset++] = (byte) nodeMetadata.getMaxLeftChildDepth();
        nodeContentBuffer[offset++] = (byte) nodeMetadata.getMaxRightChildDepth();
        nodeContentBuffer[offset] = (byte) nodeMetadata.getNodeLocalDepth();
      } else {
        OBooleanSerializer.INSTANCE.serializeNative(false, nodeContentBuffer, offset);
      }

      file.write(filePosition, nodeContentBuffer);
      filePosition += nodeContentBuffer.length;
    }
  }

  public TreeState loadTreeState(int hashTreeSize) throws IOException {
    OHashTreeNodeMetadata[] hashTreeNodeMetadata = new OHashTreeNodeMetadata[hashTreeSize];
    long[][] hashTree = new long[hashTreeSize][];

    long filePosition = 0;
    for (int i = 0; i < hashTreeSize; i++) {
      byte[] contentBuffer = new byte[RECORD_SIZE];
      file.read(filePosition, contentBuffer, contentBuffer.length);

      int offset = 0;
      boolean notNullNode = OBooleanSerializer.INSTANCE.deserializeNative(contentBuffer, offset);
      offset++;

      if (notNullNode) {
        long[] node = new long[OLocalHashTable.MAX_LEVEL_SIZE];
        for (int n = 0; n < node.length; n++) {
          node[n] = OLongSerializer.INSTANCE.deserializeNative(contentBuffer, offset);
          offset += OLongSerializer.LONG_SIZE;
        }

        hashTree[i] = node;
        OHashTreeNodeMetadata nodeMetadata = new OHashTreeNodeMetadata(contentBuffer[offset++], contentBuffer[offset++],
            contentBuffer[offset]);
        hashTreeNodeMetadata[i] = nodeMetadata;
      } else {
        hashTree[i] = null;
        hashTreeNodeMetadata[i] = null;
        hashTreeNodeMetadata[i] = null;
      }

      filePosition += RECORD_SIZE;
    }

    return new TreeState(hashTree, hashTreeNodeMetadata);
  }

  public static class TreeState {
    private final long[][]                hashTree;
    private final OHashTreeNodeMetadata[] hashTreeNodeMetadata;

    public TreeState(long[][] hashTree, OHashTreeNodeMetadata[] hashTreeNodeMetadata) {
      this.hashTree = hashTree;
      this.hashTreeNodeMetadata = hashTreeNodeMetadata;
    }

    public long[][] getHashTree() {
      return hashTree;
    }

    public OHashTreeNodeMetadata[] getHashTreeNodeMetadata() {
      return hashTreeNodeMetadata;
    }
  }
}
