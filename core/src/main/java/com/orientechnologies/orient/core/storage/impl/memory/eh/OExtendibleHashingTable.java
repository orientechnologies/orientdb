/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.impl.memory.eh;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

/**
 * @author Andrey Lomakin
 * @since 21.01.13
 */
public class OExtendibleHashingTable {
  private static final int               MAX_LEVEL_DEPTH   = 8;
  private static final int               MAX_LEVEL_SIZE    = 256;
  private static final int               LEVEL_MASK        = Integer.MAX_VALUE >>> (31 - MAX_LEVEL_DEPTH);

  private long[][]                       hashTree;
  private byte[]                         nodeLocalDepths;

  private List<OExtendibleHashingBucket> file;

  private int                            hashTreeSize;

  private int                            size;

  private int                            hashTreeTombstone = -1;

  public OExtendibleHashingTable() {
    final long[] rootTree = new long[MAX_LEVEL_SIZE];

    hashTree = new long[1][];
    hashTree[0] = rootTree;

    nodeLocalDepths = new byte[1];
    nodeLocalDepths[0] = MAX_LEVEL_DEPTH;

    size = 0;
    hashTreeSize = 1;

    file = new ArrayList<OExtendibleHashingBucket>();
  }

  public boolean put(OPhysicalPosition value) {
    NodeInfo nodeInfo = getBucket(value.clusterPosition);
    long[] node = hashTree[nodeInfo.nodeIndex];

    long filePosition = node[nodeInfo.itemIndex + nodeInfo.hashMapOffset];
    final OExtendibleHashingBucket bucket;
    long newFilePosition;

    if (filePosition == 0) {
      bucket = new OExtendibleHashingBucket(nodeInfo.nodeGlobalDepth);
      file.add(bucket);
      newFilePosition = file.size();

      node[nodeInfo.itemIndex + nodeInfo.hashMapOffset] = newFilePosition;
    } else {
      bucket = file.get((int) filePosition - 1);
    }

    if (bucket.getPosition(value.clusterPosition) > -1)
      return false;

    if (bucket.size() < OExtendibleHashingBucket.BUCKET_MAX_SIZE) {
      bucket.addEntry(value);
      size++;
      return true;
    }

    final OExtendibleHashingBucket newBucket = new OExtendibleHashingBucket(bucket.getDepth());
    int bucketDepth = bucket.getDepth();
    bucketDepth = splitBucket(bucket, newBucket, bucketDepth);

    file.add(newBucket);

    newFilePosition = file.size();

    if (bucketDepth <= nodeInfo.nodeGlobalDepth) {
      updateNodeAfterSplit(nodeInfo, bucketDepth, newFilePosition);
    } else {
      if (nodeLocalDepths[nodeInfo.nodeIndex] < MAX_LEVEL_DEPTH) {
        final long[] newNode = splitNode(nodeInfo, node);

        final int nodeLocalDepth = nodeLocalDepths[nodeInfo.nodeIndex];
        final int hashMapSize = 1 << nodeLocalDepth;

        boolean allHashMapsEquals = checkAllMapsContainSameBucket(newNode, hashMapSize);

        int newNodeIndex = -1;
        if (!allHashMapsEquals) {
          newNodeIndex = addNewNode(newNode, nodeLocalDepth);
        }

        updateNodesAfterSplit(nodeInfo, newNode, nodeLocalDepth, hashMapSize, allHashMapsEquals, newNodeIndex);

        final int newIndex = nodeInfo.itemIndex << 1;
        final int newOffset = nodeInfo.hashMapOffset << 1;
        final int newGlobalDepth = nodeInfo.nodeGlobalDepth + 1;

        if (newOffset < MAX_LEVEL_SIZE) {
          final NodeInfo updatedNodeInfo = new NodeInfo(nodeInfo.parent, newOffset, newIndex, nodeInfo.nodeIndex, newGlobalDepth);
          updateNodeAfterSplit(updatedNodeInfo, bucketDepth, newFilePosition);
        } else {
          final NodeInfo newNodeInfo;
          if (!allHashMapsEquals) {
            newNodeInfo = new NodeInfo(nodeInfo.parent, newOffset - MAX_LEVEL_SIZE, newIndex, newNodeIndex, newGlobalDepth);
          } else {
            newNodeInfo = nodeInfo.parent;
          }

          updateNodeAfterSplit(newNodeInfo, bucketDepth, newFilePosition);
        }
      } else {
        addNewLevelNode(nodeInfo, node, newFilePosition);
      }
    }

    return put(value);
  }

  public boolean contains(OClusterPosition clusterPosition) {
    NodeInfo nodeInfo = getBucket(clusterPosition);
    long position = hashTree[nodeInfo.nodeIndex][nodeInfo.itemIndex + nodeInfo.hashMapOffset];
    if (position == 0)
      return false;

    final OExtendibleHashingBucket bucket = file.get((int) position - 1);

    return bucket.getPosition(clusterPosition) != -1;
  }

  public OPhysicalPosition delete(OClusterPosition clusterPosition) {
    final NodeInfo nodeInfo = getBucket(clusterPosition);
    final long position = hashTree[nodeInfo.nodeIndex][nodeInfo.itemIndex + nodeInfo.hashMapOffset];
    final OExtendibleHashingBucket bucket = file.get((int) position - 1);
    final int positionIndex = bucket.getPosition(clusterPosition);
    if (positionIndex == -1)
      return null;

    final OPhysicalPosition removedPosition = bucket.deleteEntry(positionIndex);
    if (bucket.size() > 0)
      return removedPosition;

    final long[] node = hashTree[nodeInfo.nodeIndex];
    mergeNodesAfterDeletion(nodeInfo, bucket);

    if (nodeInfo.parent != null) {
      final int hashMapSize = 1 << nodeLocalDepths[nodeInfo.nodeIndex];

      final boolean allMapsContainSameBucket = checkAllMapsContainSameBucket(node, hashMapSize);
      if (allMapsContainSameBucket)
        mergeNodeToParent(node, nodeInfo);

    }

    return removedPosition;
  }

  private void mergeNodeToParent(long[] node, NodeInfo nodeInfo) {
    final long[] parentNode = hashTree[nodeInfo.parent.nodeIndex];
    int startIndex = -1;
    for (int i = 0; i < parentNode.length; i++)
      if (parentNode[i] < 0 && (parentNode[i] & Long.MAX_VALUE) >>> 8 == nodeInfo.nodeIndex) {
        startIndex = i;
        break;
      }

    final int hashMapSize = nodeLocalDepths[nodeInfo.nodeIndex];

    for (int i = 0, k = startIndex; i < node.length; i += hashMapSize, k++) {
      parentNode[k] = node[i];
    }

    deleteNode(nodeInfo.nodeIndex);
  }

  private void deleteNode(int nodeIndex) {
    if (nodeIndex == hashTreeSize - 1) {
      hashTree[nodeIndex] = null;
      hashTreeSize--;
      return;
    }

    if (hashTreeTombstone > -1) {
      final long[] tombstone = new long[] { hashTreeTombstone };
      hashTree[nodeIndex] = tombstone;
      hashTreeTombstone = nodeIndex;
    } else {
      hashTree[nodeIndex] = null;
      hashTreeTombstone = nodeIndex;
    }
  }

  private void mergeNodesAfterDeletion(NodeInfo nodeInfo, OExtendibleHashingBucket bucket) {
    final long[] node = hashTree[nodeInfo.nodeIndex];

    final int bucketDepth = bucket.getDepth();
    int offset = nodeInfo.nodeGlobalDepth - (bucketDepth - 1);
    NodeInfo currentNode = nodeInfo;
    int nodeLocalDepth = nodeLocalDepths[nodeInfo.nodeIndex];
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = nodeInfo.parent;
        nodeLocalDepth = nodeLocalDepths[currentNode.nodeIndex];
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);
    final int interval = (1 << (nodeLocalDepth - diff - 1));

    int firstStartIndex = currentNode.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);
    int firstEndIndex = firstStartIndex + interval;

    final int secondStartIndex = firstEndIndex;
    final int secondEndIndex = secondStartIndex + interval;

    if ((currentNode.itemIndex >>> (nodeLocalDepth - diff - 1) & 1) == 1) {
      long buddyPosition = node[firstStartIndex];

      while (buddyPosition < 0) {
        final int nodeIndex = (int) ((buddyPosition & Long.MAX_VALUE) >> 8);
        final int itemOffset = (int) buddyPosition & 0xFF;

        buddyPosition = hashTree[nodeIndex][itemOffset];
      }

      for (int i = secondStartIndex; i < secondEndIndex; i++) {
        updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, buddyPosition);
      }
    } else {
      long buddyPosition = node[secondStartIndex];

      while (buddyPosition < 0) {
        final int nodeIndex = (int) ((buddyPosition & Long.MAX_VALUE) >> 8);
        final int itemOffset = (int) buddyPosition & 0xFF;

        buddyPosition = hashTree[nodeIndex][itemOffset];
      }

      for (int i = firstStartIndex; i < firstEndIndex; i++) {
        updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, buddyPosition);
      }
    }
  }

  public void clear() {
    final long[] rootTree = new long[MAX_LEVEL_SIZE];

    hashTree = new long[1][];
    hashTree[0] = rootTree;

    nodeLocalDepths = new byte[1];
    nodeLocalDepths[0] = MAX_LEVEL_DEPTH;

    size = 0;
    hashTreeSize = 1;

    file.clear();
  }

  public long size() {
    return size;
  }

  public OPhysicalPosition get(OClusterPosition clusterPosition) {
    NodeInfo nodeInfo = getBucket(clusterPosition);
    long position = hashTree[nodeInfo.nodeIndex][nodeInfo.itemIndex + nodeInfo.hashMapOffset];
    final OExtendibleHashingBucket bucket = file.get((int) position - 1);

    return bucket.get(clusterPosition);
  }

  public Entry[] higherEntries(OClusterPosition currentRecord) {
    return null;
  }

  public Entry[] ceilingEntries(OClusterPosition currentRecord) {
    return null;
  }

  public Entry[] lowerEntries(OClusterPosition currentRecord) {
    return null;
  }

  public Entry[] floorEntries(OClusterPosition currentRecord) {
    return null;
  }

  public static final class Entry implements Comparable<Entry> {
    public final OClusterPosition  key;
    public final OPhysicalPosition value;

    public Entry(OClusterPosition key, OPhysicalPosition value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int compareTo(Entry otherEntry) {
      return key.compareTo(otherEntry.key);
    }
  }

  private void addNewLevelNode(NodeInfo nodeInfo, long[] node, long newFilePosition) {
    final long[] newNode = new long[MAX_LEVEL_SIZE];

    final int newNodeDepth;
    final int newNodeStartIndex;
    final int mapInterval;

    if (nodeInfo.itemIndex < node.length / 2) {
      final int maxDepth = getMaxLevelDepth(node, 0, node.length / 2);
      if (maxDepth > 0)
        newNodeDepth = maxDepth;
      else
        newNodeDepth = 1;

      mapInterval = 1 << (MAX_LEVEL_DEPTH - newNodeDepth);
      newNodeStartIndex = (nodeInfo.itemIndex / mapInterval) * mapInterval;
    } else {
      final int maxDepth = getMaxLevelDepth(node, node.length / 2, node.length);
      if (maxDepth > 0)
        newNodeDepth = maxDepth;
      else
        newNodeDepth = 1;

      mapInterval = 1 << (MAX_LEVEL_DEPTH - newNodeDepth);
      newNodeStartIndex = ((nodeInfo.itemIndex - node.length / 2) / mapInterval) * mapInterval + node.length / 2;
    }

    final int newNodeIndex = addNewNode(newNode, newNodeDepth);
    final int mapSize = 1 << newNodeDepth;
    for (int i = 0; i < mapInterval; i++) {
      final int nodeOffset = i + newNodeStartIndex;
      final long position = node[nodeOffset];
      if (nodeOffset != nodeInfo.itemIndex) {
        for (int n = i << newNodeDepth; n < (i + 1) << newNodeDepth; n++)
          newNode[n] = position;
      } else {
        for (int n = i << newNodeDepth; n < (2 * i + 1) << (newNodeDepth - 1); n++)
          newNode[n] = position;

        for (int n = (2 * i + 1) << (newNodeDepth - 1); n < (i + 1) << newNodeDepth; n++)
          newNode[n] = newFilePosition;
      }

      node[nodeOffset] = (newNodeIndex << 8) | (i * mapSize) | Long.MIN_VALUE;
    }
  }

  private int getMaxLevelDepth(long node[], int start, int end) {
    int currentIndex = -1;
    int maxDepth = 0;

    for (int i = start; i < end; i++) {
      final long nodePosition = node[i];
      if (nodePosition >= 0)
        continue;

      final int index = (int) ((nodePosition & Long.MAX_VALUE) >>> 8);
      if (index == currentIndex)
        continue;

      currentIndex = index;
      if (maxDepth < nodeLocalDepths[index])
        maxDepth = nodeLocalDepths[index];
    }

    return maxDepth;
  }

  private void updateNodesAfterSplit(NodeInfo nodeInfo, long[] newNode, int nodeLocalDepth, int hashMapSize,
      boolean allHashMapsEquals, int newNodeIndex) {
    final long[] parentNode = hashTree[nodeInfo.parent.nodeIndex];
    int startIndex = -1;
    for (int i = 0; i < parentNode.length; i++)
      if (parentNode[i] < 0 && (parentNode[i] & Long.MAX_VALUE) >>> 8 == nodeInfo.nodeIndex) {
        startIndex = i;
        break;
      }

    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);
    for (int i = 0; i < pointersSize; i++) {
      parentNode[startIndex + i] = (nodeInfo.nodeIndex << 8) | (i * hashMapSize) | Long.MIN_VALUE;
    }

    if (allHashMapsEquals) {
      for (int i = 0; i < pointersSize; i++) {
        final long position = newNode[i * hashMapSize];
        parentNode[startIndex + pointersSize + i] = position;
      }
    } else {
      for (int i = 0; i < pointersSize; i++)
        parentNode[startIndex + pointersSize + i] = (newNodeIndex << 8) | (i * hashMapSize) | Long.MIN_VALUE;
    }
  }

  private boolean checkAllMapsContainSameBucket(long[] newNode, int hashMapSize) {
    int n = 0;
    boolean allHashMapsEquals = true;
    while (n < newNode.length) {
      boolean allHashBucketEquals = true;
      for (int i = 0; i < hashMapSize - 1; i++) {
        if (newNode[i + n] != newNode[i + n + 1]) {
          allHashBucketEquals = false;
          break;
        }
      }
      n += hashMapSize;
      if (!allHashBucketEquals) {
        allHashMapsEquals = false;
        break;
      }
    }

    assert assertAllNodesAreFilePointers(allHashMapsEquals, newNode, hashMapSize);

    return allHashMapsEquals;
  }

  private boolean assertAllNodesAreFilePointers(boolean allHashMapsEquals, long[] newNode, int hashMapSize) {
    if (allHashMapsEquals) {
      int n = 0;
      while (n < newNode.length) {
        for (int i = 0; i < hashMapSize; i++) {
          if (newNode[i] < 0) {
            return false;
          }
        }
        n += hashMapSize;
      }
    }

    return true;
  }

  private long[] splitNode(NodeInfo nodeInfo, long[] node) {
    final long[] newNode = new long[MAX_LEVEL_SIZE];

    for (int i = MAX_LEVEL_SIZE / 2; i < MAX_LEVEL_SIZE; i++) {
      final long position = node[i];

      newNode[2 * (i - MAX_LEVEL_SIZE / 2)] = position;
      newNode[2 * (i - MAX_LEVEL_SIZE / 2) + 1] = position;
    }

    final long[] updatedNode = new long[node.length];
    for (int i = 0; i < MAX_LEVEL_SIZE / 2; i++) {
      final long position = node[i];

      updatedNode[2 * i] = position;
      updatedNode[2 * i + 1] = position;
    }

    nodeLocalDepths[nodeInfo.nodeIndex]++;
    hashTree[nodeInfo.nodeIndex] = updatedNode;

    return newNode;
  }

  private int addNewNode(long[] newNode, int nodeLocalDepth) {
    if (hashTreeSize >= hashTree.length) {
      long[][] newHashTree = new long[hashTree.length << 1][];
      System.arraycopy(hashTree, 0, newHashTree, 0, hashTree.length);
      hashTree = newHashTree;
      newHashTree = null;

      byte[] newNodeLocalDepths = new byte[nodeLocalDepths.length << 1];
      System.arraycopy(nodeLocalDepths, 0, newNodeLocalDepths, 0, nodeLocalDepths.length);
      nodeLocalDepths = newNodeLocalDepths;
      newNodeLocalDepths = null;
    }

    hashTree[hashTreeSize] = newNode;
    nodeLocalDepths[hashTreeSize] = (byte) nodeLocalDepth;

    hashTreeSize++;

    return hashTreeSize - 1;
  }

  private void updateNodeAfterSplit(NodeInfo info, int bucketDepth, long newFilePosition) {
    int offset = info.nodeGlobalDepth - (bucketDepth - 1);
    NodeInfo currentNode = info;
    int nodeLocalDepth = nodeLocalDepths[info.nodeIndex];
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = info.parent;
        nodeLocalDepth = nodeLocalDepths[currentNode.nodeIndex];
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);

    final int interval = (1 << (nodeLocalDepth - diff));
    final int firstStartIndex = currentNode.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);
    final int firstEndIndex = firstStartIndex + (1 << (nodeLocalDepth - diff));

    final int secondStartIndex = firstStartIndex + (interval >>> 1);
    final int secondEndIndex = firstEndIndex;

    for (int i = secondStartIndex; i < secondEndIndex; i++)
      updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, newFilePosition);
  }

  private void updateBucket(int nodeIndex, int itemIndex, int offset, long newFilePosition) {
    final long node[] = hashTree[nodeIndex];

    final long position = node[itemIndex + offset];
    if (position >= 0)
      node[itemIndex + offset] = newFilePosition;
    else {
      final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >>> 8);
      final int childOffset = (int) (position & 0xFF);
      final int childNodeDepth = nodeLocalDepths[childNodeIndex];
      final int interval = 1 << childNodeDepth;
      for (int i = 0; i < interval; i++) {
        updateBucket(childNodeIndex, i, childOffset, newFilePosition);
      }
    }
  }

  private NodeInfo getBucket(final OClusterPosition key) {
    return getBucket(key, null);
  }

  private NodeInfo getBucket(final OClusterPosition key, NodeInfo startNode) {
    final long hash = key.longValueHigh();

    int nodeDepth;
    int localNodeDepth;
    NodeInfo parentNode;
    int nodeIndex;
    int offset;

    if (startNode != null) {
      nodeDepth = startNode.nodeGlobalDepth;
      localNodeDepth = nodeLocalDepths[startNode.nodeIndex];
      parentNode = startNode;
      nodeIndex = startNode.nodeIndex;
      offset = startNode.hashMapOffset;
    } else {
      localNodeDepth = nodeLocalDepths[0];
      nodeDepth = localNodeDepth;
      parentNode = null;
      nodeIndex = 0;
      offset = 0;
    }

    int index = (int) ((hash >>> (64 - nodeDepth)) & (LEVEL_MASK >>> (MAX_LEVEL_DEPTH - localNodeDepth)));
    NodeInfo currentNode = new NodeInfo(parentNode, 0, index, 0, nodeDepth);
    do {
      final long position = hashTree[nodeIndex][index + offset];
      if (position >= 0)
        return currentNode;

      nodeIndex = (int) ((position & Long.MAX_VALUE) >>> 8);
      offset = (int) (position & 0xFF);

      localNodeDepth = nodeLocalDepths[nodeIndex];
      nodeDepth += localNodeDepth;

      index = (int) ((hash >>> (64 - nodeDepth)) & (LEVEL_MASK >>> (MAX_LEVEL_DEPTH - localNodeDepth)));

      parentNode = currentNode;
      currentNode = new NodeInfo(parentNode, offset, index, nodeIndex, nodeDepth);
    } while (nodeDepth <= 64);

    throw new IllegalStateException("Extendible hashing tree in corrupted state.");
  }

  private int splitBucket(OExtendibleHashingBucket bucket, OExtendibleHashingBucket newBucket, int bucketDepth) {
    assert checkBucketDepth(bucket);

    bucketDepth++;

    final Collection<OPhysicalPosition> content = bucket.getContent();
    bucket.emptyBucket();

    for (OPhysicalPosition position : content) {
      if (((position.clusterPosition.longValueHigh() >>> (64 - bucketDepth)) & 1) == 0)
        bucket.addEntry(position);
      else
        newBucket.addEntry(position);
    }

    bucket.setDepth(bucketDepth);
    newBucket.setDepth(bucketDepth);

    assert checkBucketDepth(bucket);
    assert checkBucketDepth(newBucket);

    return bucketDepth;
  }

  private boolean checkBucketDepth(OExtendibleHashingBucket bucket) {
    int bucketDepth = bucket.getDepth();

    if (bucket.size() == 0)
      return true;

    List<OPhysicalPosition> content = new ArrayList<OPhysicalPosition>(bucket.getContent());

    int firstValue = (int) (content.get(0).clusterPosition.longValueHigh() >>> (64 - bucketDepth));
    for (int i = 1; i < content.size(); i++) {
      final int value = (int) (content.get(i).clusterPosition.longValueHigh() >>> (64 - bucketDepth));
      if (value != firstValue)
        return false;
    }

    return true;
  }

  private static final class NodeInfo {
    private final NodeInfo parent;
    private final int      hashMapOffset;
    private final int      itemIndex;
    private final int      nodeIndex;
    private final int      nodeGlobalDepth;

    private NodeInfo(NodeInfo parent, int hashMapOffset, int itemIndex, int nodeIndex, int nodeDepth) {
      this.parent = parent;
      this.hashMapOffset = hashMapOffset;
      this.itemIndex = itemIndex;
      this.nodeIndex = nodeIndex;
      this.nodeGlobalDepth = nodeDepth;
    }
  }
}
