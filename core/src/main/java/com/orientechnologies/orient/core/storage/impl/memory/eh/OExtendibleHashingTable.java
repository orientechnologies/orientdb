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
  private static final int                 MAX_LEVEL_DEPTH   = 8;

  private long[][]                         hashTree;
  private OExtendibleHashingNodeMetadata[] nodesMetadata;

  private List<OExtendibleHashingBucket>   file;

  private int                              hashTreeSize;

  private int                              size;

  private int                              hashTreeTombstone = -1;

  private final int                        maxLevelDepth;
  private final int                        maxLevelSize;
  private final int                        levelMask;

  private final int                        maxBucketSize;

  public OExtendibleHashingTable() {
    this(MAX_LEVEL_DEPTH, OExtendibleHashingBucket.BUCKET_MAX_SIZE);
  }

  public OExtendibleHashingTable(int maxLevelDepth, int maxBucketSize) {
    this.maxLevelDepth = maxLevelDepth;
    this.maxLevelSize = 1 << maxLevelDepth;
    this.levelMask = Integer.MAX_VALUE >>> (31 - maxLevelDepth);
    this.maxBucketSize = maxBucketSize;

    init();
  }

  private void init() {
    final long[] rootTree = new long[maxLevelSize];

    hashTree = new long[1][];
    hashTree[0] = rootTree;

    nodesMetadata = new OExtendibleHashingNodeMetadata[1];
    nodesMetadata[0] = new OExtendibleHashingNodeMetadata((byte) 0, (byte) 0, (byte) maxLevelDepth);

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
      bucket = new OExtendibleHashingBucket(nodePath.nodeGlobalDepth, maxBucketSize);
      file.add(bucket);

      fixBucketOrderAfterAdd(nodePath, node, bucket);

      assert checkFileOrder();
    } else {
      bucket = file.get((int) filePosition - 1);
    }

    if (bucket.getPosition(value.clusterPosition) > -1)
      return false;

    if (bucket.size() < maxBucketSize) {
      bucket.addEntry(value);
      size++;
      return true;
    }

    final OExtendibleHashingBucket newBucket = new OExtendibleHashingBucket(bucket.getDepth(), maxBucketSize);
    int bucketDepth = bucket.getDepth();
    bucketDepth = splitBucket(bucket, newBucket, bucketDepth);

    file.add(newBucket);

    newFilePosition = file.size();

    fixBucketOrderAfterSplit(nodePath, filePosition, bucket, newFilePosition, newBucket, bucketDepth);

    assert checkFileOrder();

    if (bucketDepth <= nodePath.nodeGlobalDepth) {
      updateNodeAfterSplit(nodePath, bucketDepth, newFilePosition);
    } else {
      if (nodesMetadata[nodePath.nodeIndex].getNodeLocalDepth() < maxLevelDepth) {
        final NodeSplitResult nodeSplitResult = splitNode(nodePath, node);

        assert !(nodeSplitResult.allLeftHashMapsEqual && nodeSplitResult.allRightHashMapsEqual);

        final long[] newNode = nodeSplitResult.newNode;

        final int nodeLocalDepth = nodesMetadata[nodePath.nodeIndex].getNodeLocalDepth();
        final int hashMapSize = 1 << nodeLocalDepth;

        assert nodeSplitResult.allRightHashMapsEqual == checkAllMapsContainSameBucket(newNode, hashMapSize);

        int newNodeIndex = -1;
        if (!nodeSplitResult.allRightHashMapsEqual || nodePath.itemIndex >= maxLevelSize / 2)
          newNodeIndex = addNewNode(newNode, nodeLocalDepth);

        final int updatedItemIndex = nodePath.itemIndex << 1;
        final int updatedOffset = nodePath.hashMapOffset << 1;
        final int updatedGlobalDepth = nodePath.nodeGlobalDepth + 1;

        boolean allLeftHashMapEqual = nodeSplitResult.allLeftHashMapsEqual;
        boolean allRightHashMapEqual = nodeSplitResult.allRightHashMapsEqual;

        if (updatedOffset < maxLevelSize) {
          allLeftHashMapEqual = false;
          final NodePath updatedNodePath = new NodePath(nodePath.parent, updatedOffset, updatedItemIndex, nodePath.nodeIndex,
              updatedGlobalDepth, nodeLocalDepth);
          updateNodeAfterSplit(updatedNodePath, bucketDepth, newFilePosition);
        } else {
          allRightHashMapEqual = false;
          final NodePath newNodePath = new NodePath(nodePath.parent, updatedOffset - maxLevelSize, updatedItemIndex, newNodeIndex,
              updatedGlobalDepth, nodeLocalDepth);
          updateNodeAfterSplit(newNodePath, bucketDepth, newFilePosition);
        }

        final long[] updatedNode = hashTree[nodePath.nodeIndex];
        updateNodesAfterSplit(nodePath, updatedNode, newNode, nodeLocalDepth, hashMapSize, allLeftHashMapEqual,
            allRightHashMapEqual, newNodeIndex);
      } else {
        addNewLevelNode(nodePath, node, newFilePosition);
      }
    }

    return put(value);
  }

  private void fixBucketOrderAfterAdd(NodePath nodePath, long[] node, OExtendibleHashingBucket bucket) {
    long newFilePosition;
    final long nextBucketPos = nextBucket(new NodePath(nodePath.parent, nodePath.hashMapOffset, nodePath.itemIndex + 1,
        nodePath.nodeIndex, nodePath.nodeGlobalDepth, nodePath.nodeLocalDepth));
    bucket.setNextBucket(nextBucketPos);
    final long prevBucketPos = prevBucket(new NodePath(nodePath.parent, nodePath.hashMapOffset, nodePath.itemIndex - 1,
        nodePath.nodeIndex, nodePath.nodeGlobalDepth, nodePath.nodeLocalDepth));
    bucket.setPrevBucket(prevBucketPos);

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
  }

  private void fixBucketOrderAfterSplit(NodePath nodePath, long filePosition, OExtendibleHashingBucket bucket,
      long newFilePosition, OExtendibleHashingBucket newBucket, int bucketDepth) {
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
      final int hashMapSize = 1 << nodesMetadata[nodePath.nodeIndex].getNodeLocalDepth();

      final long[] node = hashTree[nodePath.nodeIndex];
      final boolean allMapsContainSameBucket = checkAllMapsContainSameBucket(node, hashMapSize);
      if (allMapsContainSameBucket)
        mergeNodeToParent(node, nodePath);
    }

    return removedPosition;
  }

  private void mergeNodeToParent(long[] node, NodePath nodePath) {
    final int startIndex = findParentNodeStartIndex(nodePath);
    final int localNodeDepth = nodesMetadata[nodePath.nodeIndex].getNodeLocalDepth();
    final int hashMapSize = 1 << localNodeDepth;

    final long[] parentNode = hashTree[nodePath.parent.nodeIndex];
    for (int i = 0, k = startIndex; i < node.length; i += hashMapSize, k++) {
      parentNode[k] = node[i];
    }

    deleteNode(nodePath.nodeIndex);

    final OExtendibleHashingNodeMetadata metadata = nodesMetadata[nodePath.parent.nodeIndex];
    if (nodePath.parent.itemIndex < maxLevelSize / 2) {
      final int maxChildDepth = metadata.getMaxLeftChildDepth();
      if (maxChildDepth == localNodeDepth)
        metadata.setMaxLeftChildDepth(getMaxLevelDepth(parentNode, 0, parentNode.length / 2));
    } else {
      final int maxChildDepth = metadata.getMaxRightChildDepth();
      if (maxChildDepth == localNodeDepth)
        metadata.setMaxRightChildDepth(getMaxLevelDepth(parentNode, parentNode.length / 2, parentNode.length));
    }
  }

  private void deleteNode(int nodeIndex) {
    if (nodeIndex == hashTreeSize - 1) {
      hashTree[nodeIndex] = null;
      nodesMetadata[nodeIndex] = null;
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

    nodesMetadata[nodeIndex] = null;
  }

  private void mergeNodesAfterDeletion(NodePath nodePath, OExtendibleHashingBucket bucket, long filePosition) {
    final int bucketDepth = bucket.getDepth();
    int offset = nodePath.nodeGlobalDepth - (bucketDepth - 1);
    NodePath currentNode = nodePath;
    int nodeLocalDepth = nodesMetadata[nodePath.nodeIndex].getNodeLocalDepth();
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = nodePath.parent;
        nodeLocalDepth = nodesMetadata[currentNode.nodeIndex].getNodeLocalDepth();
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);
    final int interval = (1 << (nodeLocalDepth - diff - 1));

    int firstStartIndex = currentNode.itemIndex & ((levelMask << (nodeLocalDepth - diff)) & levelMask);
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
      final int endIndex = maxLevelSize;

      for (int i = startIndex; i < endIndex; i++) {
        final long position = node[i];
        if (position > 0)
          return position;

        if (position < 0) {
          final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >> 8);
          final int childItemOffset = (int) position & 0xFF;

          final NodePath parent = new NodePath(nodePath.parent, 0, i, nodePath.nodeIndex, -1, nodePath.nodeLocalDepth);
          nodePath = new NodePath(parent, childItemOffset, 0, childNodeIndex, -1, nodesMetadata[childNodeIndex].getNodeLocalDepth());
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

    final int nodeLocalDepth = nodesMetadata[nodePath.nodeIndex].getNodeLocalDepth();
    final int pointersSize = 1 << (maxLevelDepth - nodeLocalDepth);

    final NodePath parent = nodePath.parent;

    if (parent.itemIndex < maxLevelSize / 2) {
      final int nextParentIndex = (parent.itemIndex / pointersSize + 1) * pointersSize;
      return new NodePath(parent.parent, 0, nextParentIndex, parent.nodeIndex, parent.nodeGlobalDepth, nodeLocalDepth);
    }

    final int nextParentIndex = ((nodePath.parent.itemIndex - maxLevelSize / 2) / pointersSize + 1) * pointersSize;
    if (nextParentIndex < maxLevelSize)
      return new NodePath(parent.parent, 0, nextParentIndex, parent.nodeIndex, parent.nodeGlobalDepth, nodeLocalDepth);

    return nextLevelUp(new NodePath(parent.parent, 0, maxLevelSize - 1, parent.nodeIndex, parent.nodeGlobalDepth, nodeLocalDepth));
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
          final int localDepth = nodesMetadata[childNodeIndex].getNodeLocalDepth();
          final int endChildIndex = 1 << localDepth - 1;

          final NodePath parent = new NodePath(nodePath.parent, 0, i, nodePath.nodeIndex, -1, nodePath.nodeLocalDepth);
          nodePath = new NodePath(parent, childItemOffset, endChildIndex, childNodeIndex, -1, nodePath.nodeLocalDepth);
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

    final int nodeLocalDepth = nodesMetadata[nodePath.nodeIndex].getNodeLocalDepth();
    final int pointersSize = 1 << (maxLevelDepth - nodeLocalDepth);

    final NodePath parent = nodePath.parent;

    if (parent.itemIndex > maxLevelSize / 2) {
      final int prevParentIndex = ((nodePath.parent.itemIndex - maxLevelSize / 2) / pointersSize) * pointersSize - 1;
      return new NodePath(parent.parent, 0, prevParentIndex, parent.nodeIndex, -1, nodeLocalDepth);
    }

    final int prevParentIndex = (parent.itemIndex / pointersSize) * pointersSize - 1;
    if (prevParentIndex >= 0)
      return new NodePath(parent.parent, 0, prevParentIndex, parent.nodeIndex, -1, nodeLocalDepth);

    return prevLevelUp(new NodePath(parent.parent, 0, 0, parent.nodeIndex, -1, nodeLocalDepth));
  }

  public void clear() {
    init();
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
    final long[] newNode = new long[maxLevelSize];

    final int newNodeDepth;
    final int newNodeStartIndex;
    final int mapInterval;

    if (nodePath.itemIndex < node.length / 2) {
      final int maxDepth = nodesMetadata[nodePath.nodeIndex].getMaxLeftChildDepth();
      assert getMaxLevelDepth(node, 0, node.length / 2) == maxDepth;
      if (maxDepth > 0)
        newNodeDepth = maxDepth;
      else
        newNodeDepth = 1;

      mapInterval = 1 << (maxLevelDepth - newNodeDepth);
      newNodeStartIndex = (nodePath.itemIndex / mapInterval) * mapInterval;
    } else {
      final int maxDepth = nodesMetadata[nodePath.nodeIndex].getMaxRightChildDepth();
      assert getMaxLevelDepth(node, node.length / 2, node.length) == maxDepth;
      if (maxDepth > 0)
        newNodeDepth = maxDepth;
      else
        newNodeDepth = 1;

      mapInterval = 1 << (maxLevelDepth - newNodeDepth);
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

    updateMaxChildDepth(nodePath, newNodeDepth);
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
      if (maxDepth < nodesMetadata[index].getNodeLocalDepth())
        maxDepth = nodesMetadata[index].getNodeLocalDepth();
    }

    return maxDepth;
  }

  private void updateNodesAfterSplit(NodePath nodePath, long[] node, long[] newNode, int nodeLocalDepth, int hashMapSize,
      boolean allLeftHashMapEquals, boolean allRightHashMapsEquals, int newNodeIndex) {

    final int startIndex = findParentNodeStartIndex(nodePath);

    final long[] parentNode = hashTree[nodePath.parent.nodeIndex];
    assert assertParentNodeStartIndex(nodePath, parentNode, startIndex);

    final int pointersSize = 1 << (maxLevelDepth - nodeLocalDepth);
    if (allLeftHashMapEquals) {
      for (int i = 0; i < pointersSize; i++) {
        final long position = node[i * hashMapSize];
        parentNode[startIndex + i] = position;
      }
    } else {
      for (int i = 0; i < pointersSize; i++)
        parentNode[startIndex + i] = (nodePath.nodeIndex << 8) | (i * hashMapSize) | Long.MIN_VALUE;
    }

    if (allRightHashMapsEquals) {
      for (int i = 0; i < pointersSize; i++) {
        final long position = newNode[i * hashMapSize];
        parentNode[startIndex + pointersSize + i] = position;
      }
    } else {
      for (int i = 0; i < pointersSize; i++)
        parentNode[startIndex + pointersSize + i] = (newNodeIndex << 8) | (i * hashMapSize) | Long.MIN_VALUE;
    }

    updateMaxChildDepth(nodePath.parent, nodePath.nodeLocalDepth + 1);
  }

  private void updateMaxChildDepth(NodePath parentPath, int childDepth) {
    if (parentPath == null)
      return;

    final OExtendibleHashingNodeMetadata metadata = nodesMetadata[parentPath.nodeIndex];
    if (parentPath.itemIndex < maxLevelSize / 2) {
      final int maxChildDepth = metadata.getMaxLeftChildDepth();
      if (childDepth > maxChildDepth)
        metadata.setMaxLeftChildDepth(childDepth);
    } else {
      final int maxChildDepth = metadata.getMaxRightChildDepth();
      if (childDepth + 1 > maxChildDepth)
        metadata.setMaxRightChildDepth(childDepth);
    }
  }

  private boolean assertParentNodeStartIndex(NodePath nodePath, long[] parentNode, int calculatedIndex) {
    int startIndex = -1;
    for (int i = 0; i < parentNode.length; i++)
      if (parentNode[i] < 0 && (parentNode[i] & Long.MAX_VALUE) >>> 8 == nodePath.nodeIndex) {
        startIndex = i;
        break;
      }

    return startIndex == calculatedIndex;
  }

  private int findParentNodeStartIndex(NodePath nodePath) {
    final NodePath parentNodePath = nodePath.parent;
    final int pointersSize = 1 << (maxLevelDepth - nodePath.nodeLocalDepth);

    if (parentNodePath.itemIndex < maxLevelSize / 2)
      return (parentNodePath.itemIndex / pointersSize) * pointersSize;

    return ((parentNodePath.itemIndex - maxLevelSize / 2) / pointersSize) * pointersSize + maxLevelSize / 2;
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

  private NodeSplitResult splitNode(NodePath nodePath, long[] node) {
    final long[] newNode = new long[maxLevelSize];
    final int hashMapSize = 1 << (nodePath.nodeLocalDepth + 1);

    boolean hashMapItemsAreEqual = true;
    boolean allLeftItemsAreEqual;
    boolean allRightItemsAreEqual;

    int mapCounter = 0;
    long firstPosition = -1;

    for (int i = maxLevelSize / 2; i < maxLevelSize; i++) {
      final long position = node[i];
      if (hashMapItemsAreEqual && mapCounter == 0)
        firstPosition = position;

      newNode[2 * (i - maxLevelSize / 2)] = position;
      newNode[2 * (i - maxLevelSize / 2) + 1] = position;

      if (hashMapItemsAreEqual) {
        hashMapItemsAreEqual = firstPosition == position;
        mapCounter += 2;

        if (mapCounter >= hashMapSize)
          mapCounter = 0;
      }
    }

    mapCounter = 0;
    allRightItemsAreEqual = hashMapItemsAreEqual;

    hashMapItemsAreEqual = true;
    final long[] updatedNode = new long[node.length];
    for (int i = 0; i < maxLevelSize / 2; i++) {
      final long position = node[i];
      if (hashMapItemsAreEqual && mapCounter == 0)
        firstPosition = position;

      updatedNode[2 * i] = position;
      updatedNode[2 * i + 1] = position;

      if (hashMapItemsAreEqual) {
        hashMapItemsAreEqual = firstPosition == position;

        mapCounter += 2;

        if (mapCounter >= hashMapSize)
          mapCounter = 0;
      }
    }

    allLeftItemsAreEqual = hashMapItemsAreEqual;

    nodesMetadata[nodePath.nodeIndex].incrementLocalNodeDepth();
    hashTree[nodePath.nodeIndex] = updatedNode;

    return new NodeSplitResult(newNode, allLeftItemsAreEqual, allRightItemsAreEqual);
  }

  private int addNewNode(long[] newNode, int nodeLocalDepth) {
    if (hashTreeTombstone >= 0) {
      long[] tombstone = hashTree[hashTreeTombstone];

      hashTree[hashTreeTombstone] = newNode;
      nodesMetadata[hashTreeTombstone] = new OExtendibleHashingNodeMetadata((byte) 0, (byte) 0, (byte) nodeLocalDepth);

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

      OExtendibleHashingNodeMetadata[] newNodeMetadata = new OExtendibleHashingNodeMetadata[nodesMetadata.length << 1];
      System.arraycopy(nodesMetadata, 0, newNodeMetadata, 0, nodesMetadata.length);
      nodesMetadata = newNodeMetadata;
      newNodeMetadata = null;
    }

    hashTree[hashTreeSize] = newNode;
    nodesMetadata[hashTreeSize] = new OExtendibleHashingNodeMetadata((byte) 0, (byte) 0, (byte) nodeLocalDepth);

    hashTreeSize++;

    return hashTreeSize - 1;
  }

  private void updateNodeAfterSplit(NodePath info, int bucketDepth, long newFilePosition) {
    int offset = info.nodeGlobalDepth - (bucketDepth - 1);
    NodePath currentNode = info;
    int nodeLocalDepth = nodesMetadata[info.nodeIndex].getNodeLocalDepth();
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = info.parent;
        nodeLocalDepth = nodesMetadata[currentNode.nodeIndex].getNodeLocalDepth();
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);

    final int interval = (1 << (nodeLocalDepth - diff));
    final int firstStartIndex = currentNode.itemIndex & ((levelMask << (nodeLocalDepth - diff)) & levelMask);
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
      final int childNodeDepth = nodesMetadata[childNodeIndex].getNodeLocalDepth();
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
      localNodeDepth = nodesMetadata[startNode.nodeIndex].getNodeLocalDepth();
      parentNode = startNode;
      nodeIndex = startNode.nodeIndex;
      offset = startNode.hashMapOffset;
    } else {
      localNodeDepth = nodesMetadata[0].getNodeLocalDepth();
      nodeDepth = localNodeDepth;
      parentNode = null;
      nodeIndex = 0;
      offset = 0;
    }

    int index = (int) ((hash >>> (64 - nodeDepth)) & (levelMask >>> (maxLevelDepth - localNodeDepth)));
    NodePath currentNode = new NodePath(parentNode, 0, index, 0, nodeDepth, localNodeDepth);
    do {
      final long position = hashTree[nodeIndex][index + offset];
      if (position >= 0)
        return currentNode;

      nodeIndex = (int) ((position & Long.MAX_VALUE) >>> 8);
      offset = (int) (position & 0xFF);

      localNodeDepth = nodesMetadata[nodeIndex].getNodeLocalDepth();
      nodeDepth += localNodeDepth;

      index = (int) ((hash >>> (64 - nodeDepth)) & (levelMask >>> (maxLevelDepth - localNodeDepth)));

      parentNode = currentNode;
      currentNode = new NodePath(parentNode, offset, index, nodeIndex, nodeDepth, localNodeDepth);
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

    final long firstBucket = nextBucket(new NodePath(null, 0, 0, 0, maxLevelDepth, maxLevelDepth));

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

  private static final class NodeSplitResult {
    private final long[]  newNode;
    private final boolean allLeftHashMapsEqual;
    private final boolean allRightHashMapsEqual;

    private NodeSplitResult(long[] newNode, boolean allLeftHashMapsEqual, boolean allRightHashMapsEqual) {
      this.newNode = newNode;
      this.allLeftHashMapsEqual = allLeftHashMapsEqual;
      this.allRightHashMapsEqual = allRightHashMapsEqual;
    }
  }

  private static final class NodePath {
    private final NodePath parent;
    private final int      hashMapOffset;
    private final int      itemIndex;
    private final int      nodeIndex;
    private final int      nodeGlobalDepth;
    private final int      nodeLocalDepth;

    private NodePath(NodePath parent, int hashMapOffset, int itemIndex, int nodeIndex, int nodeGlobalDepth, int nodeLocalDepth) {
      this.parent = parent;
      this.hashMapOffset = hashMapOffset;
      this.itemIndex = itemIndex;
      this.nodeIndex = nodeIndex;
      this.nodeGlobalDepth = nodeGlobalDepth;
      this.nodeLocalDepth = nodeLocalDepth;
    }
  }
}
