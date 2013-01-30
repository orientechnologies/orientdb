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
    NodePath nodePath = getBucket(value.clusterPosition);
    long[] node = hashTree[nodePath.nodeIndex];

    long filePosition = node[nodePath.itemIndex + nodePath.hashMapOffset];
    final OExtendibleHashingBucket bucket;
    long newFilePosition;

    if (filePosition == 0) {
      bucket = new OExtendibleHashingBucket(nodePath.nodeGlobalDepth);

      final long nextBucketPos = nextBucket(new NodePath(nodePath.parent, nodePath.hashMapOffset, nodePath.itemIndex + 1,
          nodePath.nodeIndex, nodePath.nodeGlobalDepth));
      bucket.setNextBucket(nextBucketPos);
      final long prevBucketPos = prevBucket(new NodePath(nodePath.parent, nodePath.hashMapOffset, nodePath.itemIndex - 1,
          nodePath.nodeIndex, nodePath.nodeGlobalDepth));
      bucket.setPrevBucket(prevBucketPos);

      file.add(bucket);
      newFilePosition = file.size();

      node[nodePath.itemIndex + nodePath.hashMapOffset] = newFilePosition;

      if (nextBucketPos > 0) {
        final OExtendibleHashingBucket nextBucket = file.get((int) nextBucketPos - 1);
        nextBucket.setPrevBucket(newFilePosition);
      }

      if (prevBucketPos > 0) {
        final OExtendibleHashingBucket prevBucket = file.get((int) prevBucketPos - 1);
        prevBucket.setNextBucket(newFilePosition);
      }

      assert checkFileOrder();
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

    if (((nodePath.itemIndex >>> (64 - bucketDepth + 1)) & 1) == 0) {
      final long oldNextBucketPosition = bucket.getNextBucket();

      bucket.setNextBucket(newFilePosition);
      newBucket.setPrevBucket(filePosition);

      newBucket.setNextBucket(oldNextBucketPosition);

      if (oldNextBucketPosition > 0) {
        final OExtendibleHashingBucket oldNextBucket = file.get((int) oldNextBucketPosition - 1);
        assert oldNextBucket.getPrevBucket() == filePosition;

        oldNextBucket.setPrevBucket(newFilePosition);
      }
    } else {
      newBucket.setNextBucket(filePosition);

      final long oldPrevBucketPosition = bucket.getPrevBucket();
      bucket.setPrevBucket(newFilePosition);

      if (oldPrevBucketPosition > 0) {
        final OExtendibleHashingBucket prevBucket = file.get((int) oldPrevBucketPosition - 1);
        assert prevBucket.getNextBucket() == filePosition;

        prevBucket.setNextBucket(newFilePosition);
      }
    }

    assert checkFileOrder();

    if (bucketDepth <= nodePath.nodeGlobalDepth) {
      updateNodeAfterSplit(nodePath, bucketDepth, newFilePosition);
    } else {
      if (nodeLocalDepths[nodePath.nodeIndex] < MAX_LEVEL_DEPTH) {
        final long[] newNode = splitNode(nodePath, node);

        final int nodeLocalDepth = nodeLocalDepths[nodePath.nodeIndex];
        final int hashMapSize = 1 << nodeLocalDepth;

        boolean allHashMapsEquals = checkAllMapsContainSameBucket(newNode, hashMapSize);

        int newNodeIndex = -1;
        if (!allHashMapsEquals) {
          newNodeIndex = addNewNode(newNode, nodeLocalDepth);
        }

        updateNodesAfterSplit(nodePath, newNode, nodeLocalDepth, hashMapSize, allHashMapsEquals, newNodeIndex);

        final int newIndex = nodePath.itemIndex << 1;
        final int newOffset = nodePath.hashMapOffset << 1;
        final int newGlobalDepth = nodePath.nodeGlobalDepth + 1;

        if (newOffset < MAX_LEVEL_SIZE) {
          final NodePath updatedNodePath = new NodePath(nodePath.parent, newOffset, newIndex, nodePath.nodeIndex, newGlobalDepth);
          updateNodeAfterSplit(updatedNodePath, bucketDepth, newFilePosition);
        } else {
          final NodePath newNodePath;
          if (!allHashMapsEquals) {
            newNodePath = new NodePath(nodePath.parent, newOffset - MAX_LEVEL_SIZE, newIndex, newNodeIndex, newGlobalDepth);
          } else {
            newNodePath = nodePath.parent;
          }

          updateNodeAfterSplit(newNodePath, bucketDepth, newFilePosition);
        }
      } else {
        addNewLevelNode(nodePath, node, newFilePosition);
      }
    }

    return put(value);
  }

  public boolean contains(OClusterPosition clusterPosition) {
    NodePath nodePath = getBucket(clusterPosition);
    long position = hashTree[nodePath.nodeIndex][nodePath.itemIndex + nodePath.hashMapOffset];
    if (position == 0)
      return false;

    final OExtendibleHashingBucket bucket = file.get((int) position - 1);

    return bucket.getPosition(clusterPosition) >= 0;
  }

  public OPhysicalPosition delete(OClusterPosition clusterPosition) {
    final NodePath nodePath = getBucket(clusterPosition);
    final long position = hashTree[nodePath.nodeIndex][nodePath.itemIndex + nodePath.hashMapOffset];
    final OExtendibleHashingBucket bucket = file.get((int) position - 1);
    final int positionIndex = bucket.getPosition(clusterPosition);
    if (positionIndex < 0)
      return null;

    final OPhysicalPosition removedPosition = bucket.deleteEntry(positionIndex);
    if (bucket.size() > 0)
      return removedPosition;

    mergeNodesAfterDeletion(nodePath, bucket, position);

    assert checkFileOrder();

    if (nodePath.parent != null) {
      final int hashMapSize = 1 << nodeLocalDepths[nodePath.nodeIndex];

      final long[] node = hashTree[nodePath.nodeIndex];
      final boolean allMapsContainSameBucket = checkAllMapsContainSameBucket(node, hashMapSize);
      if (allMapsContainSameBucket)
        mergeNodeToParent(node, nodePath);
    }

    return removedPosition;
  }

  private void mergeNodeToParent(long[] node, NodePath nodePath) {
    final long[] parentNode = hashTree[nodePath.parent.nodeIndex];
    int startIndex = -1;
    for (int i = 0; i < parentNode.length; i++)
      if (parentNode[i] < 0 && (parentNode[i] & Long.MAX_VALUE) >>> 8 == nodePath.nodeIndex) {
        startIndex = i;
        break;
      }

    final int hashMapSize = 1 << nodeLocalDepths[nodePath.nodeIndex];

    for (int i = 0, k = startIndex; i < node.length; i += hashMapSize, k++) {
      parentNode[k] = node[i];
    }

    deleteNode(nodePath.nodeIndex);
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

  private void mergeNodesAfterDeletion(NodePath nodePath, OExtendibleHashingBucket bucket, long filePosition) {
    final int bucketDepth = bucket.getDepth();
    int offset = nodePath.nodeGlobalDepth - (bucketDepth - 1);
    NodePath currentNode = nodePath;
    int nodeLocalDepth = nodeLocalDepths[nodePath.nodeIndex];
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = nodePath.parent;
        nodeLocalDepth = nodeLocalDepths[currentNode.nodeIndex];
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);
    final int interval = (1 << (nodeLocalDepth - diff - 1));

    int firstStartIndex = currentNode.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);
    int firstEndIndex = firstStartIndex + interval;

    final int secondStartIndex = firstEndIndex;
    final int secondEndIndex = secondStartIndex + interval;

    final OExtendibleHashingBucket buddyBucket;

    final long[] node = hashTree[currentNode.nodeIndex];
    if ((currentNode.itemIndex >>> (nodeLocalDepth - diff - 1) & 1) == 1) {
      long buddyPosition = node[firstStartIndex + currentNode.hashMapOffset];

      while (buddyPosition < 0) {
        final int nodeIndex = (int) ((buddyPosition & Long.MAX_VALUE) >> 8);
        final int itemOffset = (int) buddyPosition & 0xFF;

        buddyPosition = hashTree[nodeIndex][itemOffset];
      }

      if (buddyPosition == 0)
        return;

      buddyBucket = file.get((int) buddyPosition - 1);
      if (buddyBucket.getDepth() != bucketDepth)
        return;

      for (int i = secondStartIndex; i < secondEndIndex; i++) {
        updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, buddyPosition);
      }

      assert bucket.getPrevBucket() == buddyPosition;
      assert buddyBucket.getNextBucket() == filePosition;

      final long nextBucketPosition = bucket.getNextBucket();
      buddyBucket.setNextBucket(nextBucketPosition);

      if (nextBucketPosition > 0) {
        final OExtendibleHashingBucket nextBucket = file.get((int) nextBucketPosition - 1);

        assert nextBucket.getPrevBucket() == filePosition;
        nextBucket.setPrevBucket(buddyPosition);
      }
    } else {
      long buddyPosition = node[secondStartIndex + currentNode.hashMapOffset];

      while (buddyPosition < 0) {
        final int nodeIndex = (int) ((buddyPosition & Long.MAX_VALUE) >> 8);
        final int itemOffset = (int) buddyPosition & 0xFF;

        buddyPosition = hashTree[nodeIndex][itemOffset];
      }

      if (buddyPosition == 0)
        return;

      buddyBucket = file.get((int) buddyPosition - 1);
      if (buddyBucket.getDepth() != bucketDepth)
        return;

      for (int i = firstStartIndex; i < firstEndIndex; i++) {
        updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, buddyPosition);
      }

      assert bucket.getNextBucket() == buddyPosition;
      final long prevBucketPosition = bucket.getPrevBucket();
      buddyBucket.setPrevBucket(prevBucketPosition);

      if (prevBucketPosition > 0) {
        final OExtendibleHashingBucket prevBucket = file.get((int) prevBucketPosition - 1);

        assert prevBucket.getNextBucket() == filePosition;
        prevBucket.setNextBucket(buddyPosition);
      }
    }

    buddyBucket.setDepth(bucketDepth - 1);
    assert checkBucketDepth(buddyBucket);
  }

  private long nextBucket(NodePath nodePath) {
    nextBucketLoop: while (nodePath != null) {
      final long[] node = hashTree[nodePath.nodeIndex];
      final int startIndex = nodePath.itemIndex + nodePath.hashMapOffset;
      final int endIndex = MAX_LEVEL_SIZE;

      for (int i = startIndex; i < endIndex; i++) {
        final long position = node[i];
        if (position > 0)
          return position;

        if (position < 0) {
          final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >> 8);
          final int childItemOffset = (int) position & 0xFF;

          final NodePath parent = new NodePath(nodePath.parent, 0, i, nodePath.nodeIndex, -1);
          nodePath = new NodePath(parent, childItemOffset, 0, childNodeIndex, -1);
          continue nextBucketLoop;
        }
      }

      nodePath = nextLevelUp(nodePath);
    }

    return 0;
  }

  private NodePath nextLevelUp(NodePath nodePath) {
    if (nodePath.parent == null)
      return null;

    final int nodeLocalDepth = nodeLocalDepths[nodePath.nodeIndex];
    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);

    final NodePath parent = nodePath.parent;

    if (parent.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int nextParentIndex = (parent.itemIndex / pointersSize + 1) * pointersSize;
      return new NodePath(parent.parent, 0, nextParentIndex, parent.nodeIndex, parent.nodeGlobalDepth);
    }

    final int nextParentIndex = ((nodePath.parent.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize + 1) * pointersSize;
    if (nextParentIndex < MAX_LEVEL_SIZE)
      return new NodePath(parent.parent, 0, nextParentIndex, parent.nodeIndex, parent.nodeGlobalDepth);

    return nextLevelUp(new NodePath(parent.parent, 0, MAX_LEVEL_SIZE - 1, parent.nodeIndex, parent.nodeGlobalDepth));
  }

  private long prevBucket(NodePath nodePath) {
    prevBucketLoop: while (nodePath != null) {
      final long[] node = hashTree[nodePath.nodeIndex];
      final int startIndex = 0;
      final int endIndex = nodePath.itemIndex + nodePath.hashMapOffset;

      for (int i = endIndex; i >= startIndex; i--) {
        final long position = node[i];
        if (position > 0)
          return position;

        if (position < 0) {
          final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >> 8);
          final int childItemOffset = (int) position & 0xFF;
          final int localDepth = nodeLocalDepths[childNodeIndex];
          final int endChildIndex = 1 << localDepth - 1;

          final NodePath parent = new NodePath(nodePath.parent, 0, i, nodePath.nodeIndex, -1);
          nodePath = new NodePath(parent, childItemOffset, endChildIndex, childNodeIndex, -1);
          continue prevBucketLoop;
        }
      }

      nodePath = prevLevelUp(nodePath);
    }

    return 0;
  }

  private NodePath prevLevelUp(NodePath nodePath) {
    if (nodePath.parent == null)
      return null;

    final int nodeLocalDepth = nodeLocalDepths[nodePath.nodeIndex];
    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);

    final NodePath parent = nodePath.parent;

    if (parent.itemIndex > MAX_LEVEL_SIZE / 2) {
      final int prevParentIndex = ((nodePath.parent.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize) * pointersSize - 1;
      return new NodePath(parent.parent, 0, prevParentIndex, parent.nodeIndex, -1);
    }

    final int prevParentIndex = (parent.itemIndex / pointersSize) * pointersSize - 1;
    if (prevParentIndex >= 0)
      return new NodePath(parent.parent, 0, prevParentIndex, parent.nodeIndex, -1);

    return prevLevelUp(new NodePath(parent.parent, 0, 0, parent.nodeIndex, -1));
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

    hashTreeTombstone = -1;
  }

  public long size() {
    return size;
  }

  public OPhysicalPosition get(OClusterPosition clusterPosition) {
    NodePath nodePath = getBucket(clusterPosition);
    long position = hashTree[nodePath.nodeIndex][nodePath.itemIndex + nodePath.hashMapOffset];
    if (position == 0)
      return null;

    final OExtendibleHashingBucket bucket = file.get((int) position - 1);

    return bucket.find(clusterPosition);
  }

  public Entry[] higherEntries(OClusterPosition key) {
    final NodePath nodePath = getBucket(key);
    long position = hashTree[nodePath.nodeIndex][nodePath.itemIndex + nodePath.hashMapOffset];
    if (position == 0)
      position = nextBucket(nodePath);

    if (position == 0)
      return new Entry[0];

    OExtendibleHashingBucket bucket = file.get((int) position - 1);
    while (bucket != null && (bucket.size() == 0 || bucket.get(bucket.size() - 1).clusterPosition.compareTo(key) <= 0)) {
      final long nextPosition = bucket.getNextBucket();
      if (nextPosition > 0)
        bucket = file.get((int) nextPosition - 1);
      else
        bucket = null;
    }

    if (bucket != null) {
      final int index = bucket.getPosition(key);
      final int startIndex;
      if (index >= 0)
        startIndex = index + 1;
      else
        startIndex = -index - 1;

      final int endIndex = bucket.size();
      return convertBucketToEntries(bucket, startIndex, endIndex);
    }

    return new Entry[0];
  }

  public Entry[] ceilingEntries(OClusterPosition key) {
    final NodePath nodePath = getBucket(key);
    long position = hashTree[nodePath.nodeIndex][nodePath.itemIndex + nodePath.hashMapOffset];
    if (position == 0)
      position = nextBucket(nodePath);

    if (position == 0)
      return new Entry[0];

    OExtendibleHashingBucket bucket = file.get((int) position - 1);
    while (bucket != null && bucket.size() == 0) {
      final long nextPosition = bucket.getNextBucket();
      if (nextPosition > 0)
        bucket = file.get((int) nextPosition - 1);
      else
        bucket = null;
    }

    if (bucket != null) {
      final int index = bucket.getPosition(key);
      final int startIndex;
      if (index >= 0)
        startIndex = index;
      else
        startIndex = -index - 1;

      final int endIndex = bucket.size();
      return convertBucketToEntries(bucket, startIndex, endIndex);
    }

    return new Entry[0];
  }

  public Entry[] lowerEntries(OClusterPosition key) {
    final NodePath nodePath = getBucket(key);
    long position = hashTree[nodePath.nodeIndex][nodePath.itemIndex + nodePath.hashMapOffset];
    if (position == 0)
      position = prevBucket(nodePath);

    if (position == 0)
      return new Entry[0];

    OExtendibleHashingBucket bucket = file.get((int) position - 1);
    while (bucket != null && (bucket.size() == 0 || bucket.get(0).clusterPosition.compareTo(key) >= 0)) {
      final long prevPosition = bucket.getPrevBucket();
      if (prevPosition > 0)
        bucket = file.get((int) prevPosition - 1);
      else
        bucket = null;
    }

    if (bucket != null) {
      final int startIndex = 0;
      final int index = bucket.getPosition(key);

      final int endIndex;
      if (index >= 0)
        endIndex = index;
      else
        endIndex = -index - 1;

      return convertBucketToEntries(bucket, startIndex, endIndex);
    }

    return new Entry[0];
  }

  public Entry[] floorEntries(OClusterPosition key) {
    final NodePath nodePath = getBucket(key);
    long position = hashTree[nodePath.nodeIndex][nodePath.itemIndex + nodePath.hashMapOffset];
    if (position == 0)
      position = prevBucket(nodePath);

    if (position == 0)
      return new Entry[0];

    OExtendibleHashingBucket bucket = file.get((int) position - 1);
    while (bucket != null && bucket.size() == 0) {
      final long prevPosition = bucket.getPrevBucket();
      if (prevPosition > 0)
        bucket = file.get((int) prevPosition - 1);
      else
        bucket = null;
    }

    if (bucket != null) {
      final int startIndex = 0;
      final int index = bucket.getPosition(key);

      final int endIndex;
      if (index >= 0)
        endIndex = index + 1;
      else
        endIndex = -index - 1;

      return convertBucketToEntries(bucket, startIndex, endIndex);
    }

    return new Entry[0];
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

  private Entry[] convertBucketToEntries(final OExtendibleHashingBucket bucket, int startIndex, int endIndex) {
    final Entry[] entries = new Entry[endIndex - startIndex];
    final List<OPhysicalPosition> content = bucket.getContent();
    for (int i = 0, k = startIndex; k < endIndex; i++, k++) {
      final OPhysicalPosition position = content.get(k);
      entries[i] = new Entry(position.clusterPosition, position);
    }

    return entries;
  }

  private void addNewLevelNode(NodePath nodePath, long[] node, long newFilePosition) {
    final long[] newNode = new long[MAX_LEVEL_SIZE];

    final int newNodeDepth;
    final int newNodeStartIndex;
    final int mapInterval;

    if (nodePath.itemIndex < node.length / 2) {
      final int maxDepth = getMaxLevelDepth(node, 0, node.length / 2);
      if (maxDepth > 0)
        newNodeDepth = maxDepth;
      else
        newNodeDepth = 1;

      mapInterval = 1 << (MAX_LEVEL_DEPTH - newNodeDepth);
      newNodeStartIndex = (nodePath.itemIndex / mapInterval) * mapInterval;
    } else {
      final int maxDepth = getMaxLevelDepth(node, node.length / 2, node.length);
      if (maxDepth > 0)
        newNodeDepth = maxDepth;
      else
        newNodeDepth = 1;

      mapInterval = 1 << (MAX_LEVEL_DEPTH - newNodeDepth);
      newNodeStartIndex = ((nodePath.itemIndex - node.length / 2) / mapInterval) * mapInterval + node.length / 2;
    }

    final int newNodeIndex = addNewNode(newNode, newNodeDepth);
    final int mapSize = 1 << newNodeDepth;
    for (int i = 0; i < mapInterval; i++) {
      final int nodeOffset = i + newNodeStartIndex;
      final long position = node[nodeOffset];
      if (nodeOffset != nodePath.itemIndex) {
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

  private void updateNodesAfterSplit(NodePath nodePath, long[] newNode, int nodeLocalDepth, int hashMapSize,
      boolean allHashMapsEquals, int newNodeIndex) {
    final long[] parentNode = hashTree[nodePath.parent.nodeIndex];
    int startIndex = -1;
    for (int i = 0; i < parentNode.length; i++)
      if (parentNode[i] < 0 && (parentNode[i] & Long.MAX_VALUE) >>> 8 == nodePath.nodeIndex) {
        startIndex = i;
        break;
      }

    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);
    for (int i = 0; i < pointersSize; i++) {
      parentNode[startIndex + i] = (nodePath.nodeIndex << 8) | (i * hashMapSize) | Long.MIN_VALUE;
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

  private long[] splitNode(NodePath nodePath, long[] node) {
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

    nodeLocalDepths[nodePath.nodeIndex]++;
    hashTree[nodePath.nodeIndex] = updatedNode;

    return newNode;
  }

  private int addNewNode(long[] newNode, int nodeLocalDepth) {
    if (hashTreeTombstone >= 0) {
      long[] tombstone = hashTree[hashTreeTombstone];
      hashTree[hashTreeTombstone] = newNode;

      final int nodeIndex = hashTreeTombstone;
      if (tombstone != null)
        hashTreeTombstone = (int) tombstone[0];
      else
        hashTreeTombstone = -1;

      return nodeIndex;
    }

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

  private void updateNodeAfterSplit(NodePath info, int bucketDepth, long newFilePosition) {
    int offset = info.nodeGlobalDepth - (bucketDepth - 1);
    NodePath currentNode = info;
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

  private NodePath getBucket(final OClusterPosition key) {
    return getBucket(key, null);
  }

  private NodePath getBucket(final OClusterPosition key, NodePath startNode) {
    final long hash = key.longValueHigh();

    int nodeDepth;
    int localNodeDepth;
    NodePath parentNode;
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
    NodePath currentNode = new NodePath(parentNode, 0, index, 0, nodeDepth);
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
      currentNode = new NodePath(parentNode, offset, index, nodeIndex, nodeDepth);
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

  private boolean checkFileOrder() {
    if (size == 0)
      return true;

    final long firstBucket = nextBucket(new NodePath(null, 0, 0, 0, MAX_LEVEL_DEPTH));

    OExtendibleHashingBucket bucket = file.get((int) firstBucket - 1);
    OClusterPosition lastPrevKey = null;
    OClusterPosition nextFirstKey = null;

    while (bucket.getNextBucket() > 0) {
      OExtendibleHashingBucket nextBucket = file.get((int) bucket.getNextBucket() - 1);

      if (nextBucket.size() > 0) {
        nextFirstKey = nextBucket.get(0).clusterPosition;

        if (bucket.size() > 0)
          lastPrevKey = bucket.get(bucket.size() - 1).clusterPosition;
      }

      if (nextFirstKey != null && lastPrevKey != null)
        if (lastPrevKey.compareTo(nextFirstKey) >= 0)
          return false;
      bucket = nextBucket;
    }

    return true;
  }

  private static final class NodePath {
    private final NodePath parent;
    private final int      hashMapOffset;
    private final int      itemIndex;
    private final int      nodeIndex;
    private final int      nodeGlobalDepth;

    private NodePath(NodePath parent, int hashMapOffset, int itemIndex, int nodeIndex, int nodeDepth) {
      this.parent = parent;
      this.hashMapOffset = hashMapOffset;
      this.itemIndex = itemIndex;
      this.nodeIndex = nodeIndex;
      this.nodeGlobalDepth = nodeDepth;
    }
  }
}
