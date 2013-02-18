package com.orientechnologies.orient.core.index.hashindex.local;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.eh.OEHFileMetadata;
import com.orientechnologies.orient.core.storage.impl.local.eh.OEHFileMetadataStore;
import com.orientechnologies.orient.core.storage.impl.local.eh.OEHTreeStateStore;
import com.orientechnologies.orient.core.storage.impl.memory.eh.OEHNodeMetadata;

/**
 * @author Andrey Lomakin
 * @since 2/17/13
 */
public class OAbstractLocalHashIndex<T> extends OSharedResourceAdaptive implements OIndexInternal<T> {
  public static final String   TYPE_ID           = OClass.INDEX_TYPE.HASH.toString();

  private static final int     SEED              = 362498820;

  private static final double  MERGE_THRESHOLD   = 0.2;

  private long[][]             hashTree;
  private OEHNodeMetadata[]    nodesMetadata;

  private OEHFileMetadata[]    filesMetadata     = new OEHFileMetadata[64];

  private int                  hashTreeSize;
  private long                 size;
  private int                  hashTreeTombstone = -1;

  private final int            maxLevelDepth     = 8;
  private final int            maxLevelSize;

  private final int            levelMask;

  private OStorageLocal        storage;

  private String               name;

  private final int            bucketBufferSize;
  private final int            keySize           = 1024;
  private final int            entreeSize;

  private OEHFileMetadataStore metadataStore;
  private OEHTreeStateStore    treeStateStore;

  private OBinarySerializer    keySerializer;

  public OAbstractLocalHashIndex(ODatabaseRecord iDatabase) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());

    storage = (OStorageLocal) iDatabase.getStorage();

    this.maxLevelSize = 1 << maxLevelDepth;
    this.levelMask = Integer.MAX_VALUE >>> (31 - maxLevelDepth);
    entreeSize = OPhysicalPosition.binarySize();
    bucketBufferSize = OHashIndexBucket.calculateBufferSize(keySize, entreeSize).getBufferSize();
  }

  @Override
  public OIndex<T> create(String iName, OIndexDefinition iIndexDefinition, ODatabaseRecord iDatabase, String iClusterIndexName,
      int[] iClusterIdsToIndex, OProgressListener iProgressListener) {

    acquireExclusiveLock();
    try {
      for (int i = 0; i < 2; i++) {
        final OEHFileMetadata metadata = createFileMetadata(i);

        filesMetadata[i] = metadata;
      }

      metadataStore.create(-1);
      treeStateStore.create(-1);

      initHashTreeState();

      return this;
    } catch (IOException e) {
      throw new OIndexException("Error during index creation.", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private OEHFileMetadata createFileMetadata(int i) throws IOException {
    final OEHFileMetadata metadata = new OEHFileMetadata();
    final OStorageFileConfiguration fileConfiguration = new OStorageFileConfiguration(null, OStorageVariableParser.DB_PATH_VARIABLE
        + '/' + name + i + OEHFileMetadata.DEF_EXTENSION, OFileFactory.MMAP, "0", "100%");

    final OSingleFileSegment bucketFile = new OSingleFileSegment(storage, fileConfiguration);
    bucketFile.create(bucketBufferSize * maxLevelSize);

    metadata.setFile(bucketFile);
    return metadata;
  }

  private void initHashTreeState() throws IOException {
    final byte[] emptyBuffer = new byte[bucketBufferSize];
    final OHashIndexBucket emptyBucket = new OHashIndexBucket(maxLevelDepth, emptyBuffer, 0, entreeSize);

    final OSingleFileSegment zeroLevelFile = filesMetadata[0].getFile();

    zeroLevelFile.getFile().allocateSpace(bucketBufferSize * maxLevelSize);

    for (long filePosition = 0; filePosition < bucketBufferSize * maxLevelSize; filePosition += bucketBufferSize)
      saveBucket(0, filePosition, emptyBucket);

    filesMetadata[0].setBucketsCount(maxLevelSize);

    final long[] rootTree = new long[maxLevelSize];
    for (int i = 0; i < maxLevelSize; i++)
      rootTree[i] = createBucketPointer(i * bucketBufferSize, 0);

    hashTree = new long[1][];
    hashTree[0] = rootTree;

    nodesMetadata = new OEHNodeMetadata[1];
    nodesMetadata[0] = new OEHNodeMetadata((byte) 0, (byte) 0, (byte) maxLevelDepth);

    size = 0;
    hashTreeSize = 1;
  }

  @Override
  public void unload() {
  }

  @Override
  public String getDatabaseName() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public OType[] getKeyTypes() {
    return new OType[0];
  }

  @Override
  public Iterator<Map.Entry<Object, T>> iterator() {
    return null;
  }

  @Override
  public Iterator<Map.Entry<Object, T>> inverseIterator() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Iterator<OIdentifiable> valuesIterator() {
    return null;
  }

  @Override
  public Iterator<OIdentifiable> valuesInverseIterator() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public T get(Object key) {
    acquireSharedLock();
    try {
      final byte[] serializedKey = new byte[keySerializer.getObjectSize(key)];
      final long hashCode = OMurmurHash3.murmurHash3_x64_64(serializedKey, SEED);

      BucketPath bucketPath = getBucket(hashCode);
      final long bucketPointer = hashTree[bucketPath.nodeIndex][bucketPath.itemIndex + bucketPath.hashMapOffset];
      if (bucketPointer == 0)
        return null;

      final int fileLevel = getFileLevel(bucketPointer);
      final long filePosition = getFilePosition(bucketPointer);

      final OHashIndexBucket bucket = readBucket(fileLevel, filePosition);

      return (T) bucket.find(serializedKey).rid;
    } catch (IOException e) {
      throw new OIndexException("Exception during index value retrieval", e);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public long count(Object iKey) {
    return 0; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean contains(Object iKey) {
    return false; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public OIndex<T> put(Object key, OIdentifiable value) {
    acquireExclusiveLock();
    try {
      final byte[] serializedKey = new byte[keySerializer.getObjectSize(key)];
      final long hashCode = OMurmurHash3.murmurHash3_x64_64(serializedKey, SEED);

      final BucketPath bucketPath = getBucket(hashCode);
      long[] node = hashTree[bucketPath.nodeIndex];

      final long bucketPointer = node[bucketPath.itemIndex + bucketPath.hashMapOffset];
      if (bucketPointer == 0)
        throw new IllegalStateException("In this version of hash table buckets are added through split only.");

      final int fileLevel = getFileLevel(bucketPointer);
      final long filePosition = getFilePosition(bucketPointer);

      final OHashIndexBucket bucket = readBucket(fileLevel, filePosition);

      assert bucket.getDepth() - maxLevelDepth == fileLevel;

      final int index = bucket.getIndex(serializedKey);
      if (index > -1) {
        bucket.updateEntry(index, value.getIdentity());
        return this;
      }

      if (bucket.size() < OHashIndexBucket.MAX_BUCKET_SIZE) {
        bucket.addEntry(serializedKey, value.getIdentity());

        assert bucket.getEntry(bucket.getIndex(serializedKey)).equals(
            new OHashIndexBucket.Entry(serializedKey, value.getIdentity()));

        saveBucket(fileLevel, filePosition, bucket);

        size++;
        return this;
      }

      final BucketSplitResult splitResult = splitBucket(bucket, fileLevel, filePosition);

      final long updatedBucketPointer = splitResult.updatedBucketPointer;
      final long newBucketPointer = splitResult.newBucketPointer;
      final int bucketDepth = splitResult.newDepth;

      if (bucketDepth <= bucketPath.nodeGlobalDepth) {
        updateNodeAfterBucketSplit(bucketPath, bucketDepth, newBucketPointer, updatedBucketPointer);
      } else {
        if (bucketPath.nodeLocalDepth < maxLevelDepth) {
          final NodeSplitResult nodeSplitResult = splitNode(bucketPath, node);

          assert !(nodeSplitResult.allLeftHashMapsEqual && nodeSplitResult.allRightHashMapsEqual);

          final long[] newNode = nodeSplitResult.newNode;

          final int nodeLocalDepth = bucketPath.nodeLocalDepth + 1;
          final int hashMapSize = 1 << nodeLocalDepth;

          assert nodeSplitResult.allRightHashMapsEqual == checkAllMapsContainSameBucket(newNode, hashMapSize);

          int newNodeIndex = -1;
          if (!nodeSplitResult.allRightHashMapsEqual || bucketPath.itemIndex >= maxLevelSize / 2)
            newNodeIndex = addNewNode(newNode, nodeLocalDepth);

          final int updatedItemIndex = bucketPath.itemIndex << 1;
          final int updatedOffset = bucketPath.hashMapOffset << 1;
          final int updatedGlobalDepth = bucketPath.nodeGlobalDepth + 1;

          boolean allLeftHashMapsEqual = nodeSplitResult.allLeftHashMapsEqual;
          boolean allRightHashMapsEqual = nodeSplitResult.allRightHashMapsEqual;

          if (updatedOffset < maxLevelSize) {
            allLeftHashMapsEqual = false;
            final BucketPath updatedBucketPath = new BucketPath(bucketPath.parent, updatedOffset, updatedItemIndex,
                bucketPath.nodeIndex, nodeLocalDepth, updatedGlobalDepth);
            updateNodeAfterBucketSplit(updatedBucketPath, bucketDepth, newBucketPointer, updatedBucketPointer);
          } else {
            allRightHashMapsEqual = false;
            final BucketPath newBucketPath = new BucketPath(bucketPath.parent, updatedOffset - maxLevelSize, updatedItemIndex,
                newNodeIndex, nodeLocalDepth, updatedGlobalDepth);
            updateNodeAfterBucketSplit(newBucketPath, bucketDepth, newBucketPointer, updatedBucketPointer);
          }

          final long[] updatedNode = hashTree[bucketPath.nodeIndex];
          updateNodesAfterSplit(bucketPath, updatedNode, newNode, nodeLocalDepth, hashMapSize, allLeftHashMapsEqual,
              allRightHashMapsEqual, newNodeIndex);

          if (allLeftHashMapsEqual)
            deleteNode(bucketPath.nodeIndex);

        } else {
          addNewLevelNode(bucketPath, node, newBucketPointer, updatedBucketPointer);
        }
      }

      return put(key, value);
    } catch (IOException e) {
      throw new OIndexException("Error during index update", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void mergeNodeToParent(long[] node, BucketPath nodePath) {
    final int startIndex = findParentNodeStartIndex(nodePath);
    final int localNodeDepth = nodePath.nodeLocalDepth;
    final int hashMapSize = 1 << localNodeDepth;

    final long[] parentNode = hashTree[nodePath.parent.nodeIndex];
    for (int i = 0, k = startIndex; i < node.length; i += hashMapSize, k++) {
      parentNode[k] = node[i];
    }

    deleteNode(nodePath.nodeIndex);

    final OEHNodeMetadata metadata = nodesMetadata[nodePath.parent.nodeIndex];
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

  private boolean mergeBucketsAfterDeletion(BucketPath nodePath, OHashIndexBucket bucket) throws IOException {
    final int bucketDepth = bucket.getDepth();
    if (bucket.size() > OHashIndexBucket.MAX_BUCKET_SIZE * MERGE_THRESHOLD)
      return false;

    if (bucketDepth - maxLevelDepth < 1)
      return false;

    int offset = nodePath.nodeGlobalDepth - (bucketDepth - 1);
    BucketPath currentNode = nodePath;
    int nodeLocalDepth = nodePath.nodeLocalDepth;
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = nodePath.parent;
        nodeLocalDepth = currentNode.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);
    final int interval = (1 << (nodeLocalDepth - diff - 1));

    int firstStartIndex = currentNode.itemIndex & ((levelMask << (nodeLocalDepth - diff)) & levelMask);
    int firstEndIndex = firstStartIndex + interval;

    final int secondStartIndex = firstEndIndex;
    final int secondEndIndex = secondStartIndex + interval;

    final OHashIndexBucket buddyBucket;

    int buddyLevel;
    long buddyPosition;
    long buddyPointer;

    final long[] node = hashTree[currentNode.nodeIndex];
    if ((currentNode.itemIndex >>> (nodeLocalDepth - diff - 1) & 1) == 1) {
      buddyPointer = node[firstStartIndex + currentNode.hashMapOffset];

      while (buddyPointer < 0) {
        final int nodeIndex = (int) ((buddyPointer & Long.MAX_VALUE) >> 8);
        final int itemOffset = (int) buddyPointer & 0xFF;

        buddyPointer = hashTree[nodeIndex][itemOffset];
      }

      assert buddyPointer > 0;

      buddyLevel = getFileLevel(buddyPointer);
      buddyPosition = getFilePosition(buddyPointer);
    } else {
      buddyPointer = node[secondStartIndex + currentNode.hashMapOffset];

      while (buddyPointer < 0) {
        final int nodeIndex = (int) ((buddyPointer & Long.MAX_VALUE) >> 8);
        final int itemOffset = (int) buddyPointer & 0xFF;

        buddyPointer = hashTree[nodeIndex][itemOffset];
      }

      assert buddyPointer > 0;

      buddyLevel = getFileLevel(buddyPointer);
      buddyPosition = getFilePosition(buddyPointer);
    }

    buddyBucket = readBucket(buddyLevel, buddyPosition);

    if (buddyBucket.getDepth() != bucketDepth)
      return false;

    if (bucket.size() + buddyBucket.size() >= OHashIndexBucket.MAX_BUCKET_SIZE)
      return false;

    for (OHashIndexBucket.Entry entry : bucket)
      buddyBucket.addEntry(entry.key, entry.rid);

    final long bucketPosition = getFilePosition(hashTree[nodePath.nodeIndex][nodePath.itemIndex + nodePath.hashMapOffset]);

    int oldBuddyLevel = buddyLevel;
    long oldBuddyPosition = buddyPosition;

    filesMetadata[buddyLevel].setBucketsCount(filesMetadata[buddyLevel].geBucketsCount() - 2);

    buddyLevel--;
    filesMetadata[buddyLevel].setBucketsCount(filesMetadata[buddyLevel].geBucketsCount() + 1);

    buddyPosition = buddyBucket.getSplitHistory(buddyLevel);

    assert bucketPosition == oldBuddyPosition - bucketBufferSize || oldBuddyPosition == bucketPosition - bucketBufferSize;

    buddyBucket.setDepth(bucketDepth - 1);
    saveBucket(buddyLevel, buddyPosition, buddyBucket);

    buddyPointer = createBucketPointer(buddyPosition, buddyLevel);

    for (int i = firstStartIndex; i < secondEndIndex; i++)
      updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, buddyPointer);

    assert checkBucketDepth(buddyBucket);

    final OEHFileMetadata oldBuddyFileMetadata = filesMetadata[oldBuddyLevel];
    if (oldBuddyFileMetadata.geBucketsCount() > 0) {
      final OHashIndexBucket tombstone = new OHashIndexBucket(new byte[bucketBufferSize], 0, entreeSize);

      if (oldBuddyFileMetadata.getTombstonePosition() >= 0)
        tombstone.setNextRemovedBucketPair(oldBuddyFileMetadata.getTombstonePosition());
      else
        tombstone.setNextRemovedBucketPair(-1);

      oldBuddyFileMetadata.setTombstonePosition(Math.min(bucketPosition, oldBuddyPosition));

      saveBucket(oldBuddyLevel, oldBuddyPosition, tombstone);
    } else
      oldBuddyFileMetadata.setTombstonePosition(-1);

    return true;
  }

  private BucketPath nextBucketToFind(final BucketPath bucketPath, int bucketDepth) {
    int offset = bucketPath.nodeGlobalDepth - bucketDepth;

    BucketPath currentNode = bucketPath;
    int nodeLocalDepth = nodesMetadata[bucketPath.nodeIndex].getNodeLocalDepth();
    assert nodesMetadata[bucketPath.nodeIndex].getNodeLocalDepth() == bucketPath.nodeLocalDepth;

    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = bucketPath.parent;
        nodeLocalDepth = currentNode.nodeLocalDepth;
        assert nodesMetadata[currentNode.nodeIndex].getNodeLocalDepth() == currentNode.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - (currentNode.nodeGlobalDepth - nodeLocalDepth);
    final int interval = (1 << (nodeLocalDepth - diff));
    final int firstStartIndex = currentNode.itemIndex & ((levelMask << (nodeLocalDepth - diff)) & levelMask);

    final BucketPath bucketPathToFind;
    final int globalIndex = firstStartIndex + interval + currentNode.hashMapOffset;
    if (globalIndex >= maxLevelSize)
      bucketPathToFind = nextLevelUp(currentNode);
    else {
      final int hashMapSize = 1 << currentNode.nodeLocalDepth;
      final int hashMapOffset = globalIndex / hashMapSize * hashMapSize;

      final int startIndex = globalIndex - hashMapOffset;

      bucketPathToFind = new BucketPath(currentNode.parent, hashMapOffset, startIndex, currentNode.nodeIndex,
          currentNode.nodeLocalDepth, currentNode.nodeGlobalDepth);
    }

    return nextNonEmptyNode(bucketPathToFind);
  }

  private BucketPath nextNonEmptyNode(BucketPath bucketPath) {
    nextBucketLoop: while (bucketPath != null) {
      final long[] node = hashTree[bucketPath.nodeIndex];
      final int startIndex = bucketPath.itemIndex + bucketPath.hashMapOffset;
      final int endIndex = maxLevelSize;

      for (int i = startIndex; i < endIndex; i++) {
        final long position = node[i];

        if (position > 0) {
          final int hashMapSize = 1 << bucketPath.nodeLocalDepth;
          final int hashMapOffset = (i / hashMapSize) * hashMapSize;
          final int itemIndex = i - hashMapOffset;

          return new BucketPath(bucketPath.parent, hashMapOffset, itemIndex, bucketPath.nodeIndex, bucketPath.nodeLocalDepth,
              bucketPath.nodeGlobalDepth);
        }

        if (position < 0) {
          final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >> 8);
          final int childItemOffset = (int) position & 0xFF;

          final BucketPath parent = new BucketPath(bucketPath.parent, 0, i, bucketPath.nodeIndex, bucketPath.nodeLocalDepth,
              bucketPath.nodeGlobalDepth);

          final int childLocalDepth = nodesMetadata[childNodeIndex].getNodeLocalDepth();
          bucketPath = new BucketPath(parent, childItemOffset, 0, childNodeIndex, childLocalDepth, bucketPath.nodeGlobalDepth
              + childLocalDepth);

          continue nextBucketLoop;
        }
      }

      bucketPath = nextLevelUp(bucketPath);
    }

    return null;
  }

  private BucketPath nextLevelUp(BucketPath bucketPath) {
    if (bucketPath.parent == null)
      return null;

    final int nodeLocalDepth = bucketPath.nodeLocalDepth;
    assert nodesMetadata[bucketPath.nodeIndex].getNodeLocalDepth() == bucketPath.nodeLocalDepth;
    final int pointersSize = 1 << (maxLevelDepth - nodeLocalDepth);

    final BucketPath parent = bucketPath.parent;

    if (parent.itemIndex < maxLevelSize / 2) {
      final int nextParentIndex = (parent.itemIndex / pointersSize + 1) * pointersSize;
      return new BucketPath(parent.parent, 0, nextParentIndex, parent.nodeIndex, parent.nodeLocalDepth, parent.nodeGlobalDepth);
    }

    final int nextParentIndex = ((parent.itemIndex - maxLevelSize / 2) / pointersSize + 1) * pointersSize + maxLevelSize / 2;
    if (nextParentIndex < maxLevelSize)
      return new BucketPath(parent.parent, 0, nextParentIndex, parent.nodeIndex, parent.nodeLocalDepth, parent.nodeGlobalDepth);

    return nextLevelUp(new BucketPath(parent.parent, 0, maxLevelSize - 1, parent.nodeIndex, parent.nodeLocalDepth,
        parent.nodeGlobalDepth));
  }

  private void saveBucket(int fileLevel, long filePosition, OHashIndexBucket bucket) throws IOException {
    OEHFileMetadata fileMetadata = filesMetadata[fileLevel];
    if (fileMetadata == null) {
      fileMetadata = createFileMetadata(fileLevel);
      filesMetadata[fileLevel] = fileMetadata;
    }
    final OSingleFileSegment bucketFile = fileMetadata.getFile();
    bucket.toStream();

    bucketFile.getFile().write(filePosition, bucket.getDataBuffer());
  }

  private OHashIndexBucket readBucket(int fileLevel, long filePosition) throws IOException {
    final OEHFileMetadata fileMetadata = filesMetadata[fileLevel];
    final OSingleFileSegment bucketFile = fileMetadata.getFile();

    final byte[] serializedFile = new byte[bucketBufferSize];
    bucketFile.getFile().read(filePosition, serializedFile, serializedFile.length);
    return new OHashIndexBucket(serializedFile, 0, entreeSize);
  }

  private OHashIndexBucket readBucket(BucketPath bucketPath) throws IOException {
    long[] node = hashTree[bucketPath.nodeIndex];
    long bucketPointer = node[bucketPath.itemIndex + bucketPath.hashMapOffset];

    long filePosition = getFilePosition(bucketPointer);
    int fileLevel = getFileLevel(bucketPointer);

    return readBucket(fileLevel, filePosition);
  }

  private void updateNodeAfterBucketSplit(BucketPath bucketPath, int bucketDepth, long newBucketPointer, long updatedBucketPointer) {
    int offset = bucketPath.nodeGlobalDepth - (bucketDepth - 1);
    BucketPath currentNode = bucketPath;
    int nodeLocalDepth = bucketPath.nodeLocalDepth;
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = bucketPath.parent;
        nodeLocalDepth = currentNode.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);

    final int interval = (1 << (nodeLocalDepth - diff - 1));
    final int firstStartIndex = currentNode.itemIndex & ((levelMask << (nodeLocalDepth - diff)) & levelMask);
    final int firstEndIndex = firstStartIndex + interval;

    final int secondStartIndex = firstEndIndex;
    final int secondEndIndex = secondStartIndex + interval;

    for (int i = firstStartIndex; i < firstEndIndex; i++)
      updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, updatedBucketPointer);

    for (int i = secondStartIndex; i < secondEndIndex; i++)
      updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, newBucketPointer);
  }

  private int addNewNode(long[] newNode, int nodeLocalDepth) {
    if (hashTreeTombstone >= 0) {
      long[] tombstone = hashTree[hashTreeTombstone];

      hashTree[hashTreeTombstone] = newNode;
      nodesMetadata[hashTreeTombstone] = new OEHNodeMetadata((byte) 0, (byte) 0, (byte) nodeLocalDepth);

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

      OEHNodeMetadata[] newNodeMetadata = new OEHNodeMetadata[nodesMetadata.length << 1];
      System.arraycopy(nodesMetadata, 0, newNodeMetadata, 0, nodesMetadata.length);
      nodesMetadata = newNodeMetadata;
      newNodeMetadata = null;
    }

    hashTree[hashTreeSize] = newNode;
    nodesMetadata[hashTreeSize] = new OEHNodeMetadata((byte) 0, (byte) 0, (byte) nodeLocalDepth);

    hashTreeSize++;

    return hashTreeSize - 1;
  }

  private int splitBucketContent(OHashIndexBucket bucket, OHashIndexBucket updatedBucket, OHashIndexBucket newBucket,
      int bucketDepth) {
    assert checkBucketDepth(bucket);

    bucketDepth++;

    for (OHashIndexBucket.Entry entry : bucket) {
      if (((OMurmurHash3.murmurHash3_x64_64(entry.key, SEED) >>> (64 - bucketDepth)) & 1) == 0)
        updatedBucket.appendEntry(entry.key, entry.rid);
      else
        newBucket.appendEntry(entry.key, entry.rid);
    }

    updatedBucket.setDepth(bucketDepth);
    newBucket.setDepth(bucketDepth);

    assert checkBucketDepth(updatedBucket);
    assert checkBucketDepth(newBucket);

    return bucketDepth;
  }

  private BucketSplitResult splitBucket(OHashIndexBucket bucket, int fileLevel, long filePosition) throws IOException {
    final byte[] bucketBuffer = new byte[bucketBufferSize * 2];

    final OHashIndexBucket updatedBucket = new OHashIndexBucket(bucket, bucketBuffer, 0, entreeSize);
    final OHashIndexBucket newBucket = new OHashIndexBucket(bucket, bucketBuffer, bucketBufferSize, entreeSize);

    int bucketDepth = bucket.getDepth();
    bucketDepth = splitBucketContent(bucket, updatedBucket, newBucket, bucketDepth);

    final OEHFileMetadata fileMetadata = filesMetadata[fileLevel];
    fileMetadata.setBucketsCount(fileMetadata.geBucketsCount() - 1);

    assert fileMetadata.geBucketsCount() >= 0;

    int newFileLevel = bucketDepth - maxLevelDepth;
    OEHFileMetadata newFileMetadata = filesMetadata[newFileLevel];
    if (newFileMetadata == null) {
      newFileMetadata = createFileMetadata(newFileLevel);
      filesMetadata[newFileLevel] = newFileMetadata;
    }

    final long tombstonePosition = newFileMetadata.getTombstonePosition();

    updatedBucket.setSplitHistory(fileLevel, filePosition);
    newBucket.setSplitHistory(fileLevel, filePosition);

    updatedBucket.toStream();
    newBucket.toStream();

    final long updatedFilePosition;
    if (tombstonePosition >= 0) {
      final OHashIndexBucket tombstone = readBucket(newFileLevel, tombstonePosition);
      newFileMetadata.setTombstonePosition(tombstone.getNextRemovedBucketPair());

      final OFile file = newFileMetadata.getFile().getFile();
      file.write(tombstonePosition, updatedBucket.getDataBuffer());

      updatedFilePosition = tombstonePosition;
    } else {
      final OFile file = newFileMetadata.getFile().getFile();

      updatedFilePosition = file.allocateSpace(2 * bucketBufferSize);
      file.write(updatedFilePosition, updatedBucket.getDataBuffer());
    }

    final long newFilePosition = updatedFilePosition + bucketBufferSize;
    newFileMetadata.setBucketsCount(newFileMetadata.geBucketsCount() + 2);

    final long updatedBucketPointer = createBucketPointer(updatedFilePosition, newFileLevel);
    final long newBucketPointer = createBucketPointer(newFilePosition, newFileLevel);

    return new BucketSplitResult(updatedBucketPointer, newBucketPointer, bucketDepth);
  }

  private boolean checkBucketDepth(OHashIndexBucket bucket) {
    int bucketDepth = bucket.getDepth();

    if (bucket.size() == 0)
      return true;

    final Iterator<OHashIndexBucket.Entry> positionIterator = bucket.iterator();

    long firstValue = OMurmurHash3.murmurHash3_x64_64(positionIterator.next().key, SEED) >>> (64 - bucketDepth);
    while (positionIterator.hasNext()) {
      final long value = OMurmurHash3.murmurHash3_x64_64(positionIterator.next().key, SEED) >>> (64 - bucketDepth);
      if (value != firstValue)
        return false;
    }

    return true;
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

  private BucketPath getBucket(final long hashCode) {
    int localNodeDepth = nodesMetadata[0].getNodeLocalDepth();
    int nodeDepth = localNodeDepth;
    BucketPath parentNode = null;
    int nodeIndex = 0;
    int offset = 0;

    int index = (int) ((hashCode >>> (64 - nodeDepth)) & (levelMask >>> (maxLevelDepth - localNodeDepth)));
    BucketPath currentNode = new BucketPath(parentNode, 0, index, 0, localNodeDepth, nodeDepth);
    do {
      final long position = hashTree[nodeIndex][index + offset];
      if (position >= 0)
        return currentNode;

      nodeIndex = (int) ((position & Long.MAX_VALUE) >>> 8);
      offset = (int) (position & 0xFF);

      localNodeDepth = nodesMetadata[nodeIndex].getNodeLocalDepth();
      nodeDepth += localNodeDepth;

      index = (int) ((hashCode >>> (64 - nodeDepth)) & (levelMask >>> (maxLevelDepth - localNodeDepth)));

      parentNode = currentNode;
      currentNode = new BucketPath(parentNode, offset, index, nodeIndex, localNodeDepth, nodeDepth);
    } while (nodeDepth <= 64);

    throw new IllegalStateException("Extendible hashing tree in corrupted state.");

  }

  private void addNewLevelNode(BucketPath bucketPath, long[] node, long newBucketPointer, long updatedBucketPointer) {
    final long[] newNode = new long[maxLevelSize];

    final int newNodeDepth;
    final int newNodeStartIndex;
    final int mapInterval;

    if (bucketPath.itemIndex < node.length / 2) {
      final int maxDepth = nodesMetadata[bucketPath.nodeIndex].getMaxLeftChildDepth();
      assert getMaxLevelDepth(node, 0, node.length / 2) == maxDepth;
      if (maxDepth > 0)
        newNodeDepth = maxDepth;
      else
        newNodeDepth = 1;

      mapInterval = 1 << (maxLevelDepth - newNodeDepth);
      newNodeStartIndex = (bucketPath.itemIndex / mapInterval) * mapInterval;
    } else {
      final int maxDepth = nodesMetadata[bucketPath.nodeIndex].getMaxRightChildDepth();
      assert getMaxLevelDepth(node, node.length / 2, node.length) == maxDepth;
      if (maxDepth > 0)
        newNodeDepth = maxDepth;
      else
        newNodeDepth = 1;

      mapInterval = 1 << (maxLevelDepth - newNodeDepth);
      newNodeStartIndex = ((bucketPath.itemIndex - node.length / 2) / mapInterval) * mapInterval + node.length / 2;
    }

    final int newNodeIndex = addNewNode(newNode, newNodeDepth);
    final int mapSize = 1 << newNodeDepth;
    for (int i = 0; i < mapInterval; i++) {
      final int nodeOffset = i + newNodeStartIndex;
      final long bucketPointer = node[nodeOffset];

      if (nodeOffset != bucketPath.itemIndex) {
        for (int n = i << newNodeDepth; n < (i + 1) << newNodeDepth; n++)
          newNode[n] = bucketPointer;
      } else {
        for (int n = i << newNodeDepth; n < (2 * i + 1) << (newNodeDepth - 1); n++)
          newNode[n] = updatedBucketPointer;

        for (int n = (2 * i + 1) << (newNodeDepth - 1); n < (i + 1) << newNodeDepth; n++)
          newNode[n] = newBucketPointer;
      }

      node[nodeOffset] = (newNodeIndex << 8) | (i * mapSize) | Long.MIN_VALUE;
    }

    updateMaxChildDepth(bucketPath, newNodeDepth);
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

  private void updateNodesAfterSplit(BucketPath bucketPath, long[] node, long[] newNode, int nodeLocalDepth, int hashMapSize,
      boolean allLeftHashMapEquals, boolean allRightHashMapsEquals, int newNodeIndex) {

    final int startIndex = findParentNodeStartIndex(bucketPath);

    final long[] parentNode = hashTree[bucketPath.parent.nodeIndex];
    assert assertParentNodeStartIndex(bucketPath, parentNode, startIndex);

    final int pointersSize = 1 << (maxLevelDepth - nodeLocalDepth);
    if (allLeftHashMapEquals) {
      for (int i = 0; i < pointersSize; i++) {
        final long position = node[i * hashMapSize];
        parentNode[startIndex + i] = position;
      }
    } else {
      for (int i = 0; i < pointersSize; i++)
        parentNode[startIndex + i] = (bucketPath.nodeIndex << 8) | (i * hashMapSize) | Long.MIN_VALUE;
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

    updateMaxChildDepth(bucketPath.parent, bucketPath.nodeLocalDepth + 1);
  }

  private void updateMaxChildDepth(BucketPath parentPath, int childDepth) {
    if (parentPath == null)
      return;

    final OEHNodeMetadata metadata = nodesMetadata[parentPath.nodeIndex];
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

  private boolean assertParentNodeStartIndex(BucketPath bucketPath, long[] parentNode, int calculatedIndex) {
    int startIndex = -1;
    for (int i = 0; i < parentNode.length; i++)
      if (parentNode[i] < 0 && (parentNode[i] & Long.MAX_VALUE) >>> 8 == bucketPath.nodeIndex) {
        startIndex = i;
        break;
      }

    return startIndex == calculatedIndex;
  }

  private int findParentNodeStartIndex(BucketPath bucketPath) {
    final BucketPath parentBucketPath = bucketPath.parent;
    final int pointersSize = 1 << (maxLevelDepth - bucketPath.nodeLocalDepth);

    if (parentBucketPath.itemIndex < maxLevelSize / 2)
      return (parentBucketPath.itemIndex / pointersSize) * pointersSize;

    return ((parentBucketPath.itemIndex - maxLevelSize / 2) / pointersSize) * pointersSize + maxLevelSize / 2;
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

  private NodeSplitResult splitNode(BucketPath bucketPath, long[] node) {
    final long[] newNode = new long[maxLevelSize];
    final int hashMapSize = 1 << (bucketPath.nodeLocalDepth + 1);

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

    nodesMetadata[bucketPath.nodeIndex].incrementLocalNodeDepth();
    hashTree[bucketPath.nodeIndex] = updatedNode;

    return new NodeSplitResult(newNode, allLeftItemsAreEqual, allRightItemsAreEqual);
  }

  private long createBucketPointer(long filePosition, int fileLevel) {
    return ((filePosition + 1) << 8) | fileLevel;
  }

  private long getFilePosition(long bucketPointer) {
    return (bucketPointer >>> 8) - 1;
  }

  private int getFileLevel(long bucketPointer) {
    return (int) (bucketPointer & 0xFF);
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

  @Override
  public boolean remove(Object key) {
    acquireExclusiveLock();
    try {
      final byte[] serializedKey = new byte[keySerializer.getObjectSize(key)];
      final long hashCode = OMurmurHash3.murmurHash3_x64_64(serializedKey, SEED);

      final BucketPath nodePath = getBucket(hashCode);
      final long bucketPointer = hashTree[nodePath.nodeIndex][nodePath.itemIndex + nodePath.hashMapOffset];

      final int fileLevel = getFileLevel(bucketPointer);
      final long filePosition = getFilePosition(bucketPointer);

      final OHashIndexBucket bucket = readBucket(fileLevel, filePosition);

      final int positionIndex = bucket.getIndex(serializedKey);
      if (positionIndex < 0)
        return false;

      bucket.deleteEntry(positionIndex);
      size--;

      if (!mergeBucketsAfterDeletion(nodePath, bucket))
        saveBucket(fileLevel, filePosition, bucket);

      if (nodePath.parent != null) {
        final int hashMapSize = 1 << nodePath.nodeLocalDepth;

        final long[] node = hashTree[nodePath.nodeIndex];
        final boolean allMapsContainSameBucket = checkAllMapsContainSameBucket(node, hashMapSize);
        if (allMapsContainSameBucket)
          mergeNodeToParent(node, nodePath);
      }

      return true;
    } catch (IOException e) {
      throw new OIndexException("Error during index removal", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean remove(Object iKey, OIdentifiable iRID) {
    return remove(iKey);
  }

  @Override
  public int remove(OIdentifiable iRID) {
    throw new UnsupportedOperationException("onAfterTxCommit");
  }

  @Override
  public OIndex<T> clear() {
    acquireExclusiveLock();
    try {
      for (OEHFileMetadata metadata : filesMetadata) {
        if (metadata == null)
          continue;

        metadata.getFile().truncate();
        metadata.setBucketsCount(0);
        metadata.setTombstonePosition(-1);
      }

      metadataStore.truncate();
      treeStateStore.truncate();

      initHashTreeState();

      return this;
    } catch (IOException e) {
      throw new OIndexException("Error during index clear", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Iterable<Object> keys() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, Object iRangeTo) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo,
      boolean iToInclusive, int maxValuesToFetch) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive, int maxValuesToFetch) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive, int maxValuesToFetch) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive, int maxEntriesToFetch) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, int maxEntriesToFetch) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, int maxEntriesToFetch) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public long getSize() {
    return 0; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public long getKeySize() {
    return 0; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void checkEntry(OIdentifiable iRecord, Object iKey) {
    // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public OIndex<T> lazySave() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public OIndex<T> delete() {
    acquireExclusiveLock();
    try {
      for (OEHFileMetadata metadata : filesMetadata)
        if (metadata != null)
          metadata.getFile().delete();

      metadataStore.delete();
      treeStateStore.delete();

      return this;
    } catch (IOException e) {
      throw new OIndexException("Exception during index deletion", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public String getName() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public String getType() {
    return OClass.INDEX_TYPE.HASH.toString();
  }

  @Override
  public boolean isAutomatic() {
    return false; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public long rebuild() {
    return 0; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public long rebuild(OProgressListener iProgressListener) {
    return 0; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ODocument getConfiguration() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ORID getIdentity() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void commit(ODocument iDocument) {
    // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public OIndexInternal<T> getInternal() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<OIdentifiable> getValues(Collection<?> iKeys) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<OIdentifiable> getValues(Collection<?> iKeys, int maxValuesToFetch) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<ODocument> getEntries(Collection<?> iKeys) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<ODocument> getEntries(Collection<?> iKeys, int maxEntriesToFetch) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public OIndexDefinition getDefinition() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Set<String> getClusters() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public void flush() {
    acquireExclusiveLock();
    try {
      if (metadataStore.getFile().isOpen()) {
        storeMetadata();
        metadataStore.close();
      }

      if (treeStateStore.getFile().isOpen()) {
        storeHashTree();
        treeStateStore.close();
      }

      for (OEHFileMetadata metadata : filesMetadata)
        if (metadata != null && metadata.getFile().getFile().isOpen())
          metadata.getFile().close();
    } catch (IOException e) {
      throw new OIndexException("Error during index save", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void storeMetadata() throws IOException {
    metadataStore.setRecordsCount(size);
    metadataStore.storeMetadata(filesMetadata);
  }

  private void storeHashTree() throws IOException {
    treeStateStore.setHashTreeSize(hashTreeSize);
    for (int i = 0; i < hashTreeSize; i++)
      treeStateStore.storeTreeState(i, hashTree[i], nodesMetadata[i]);
  }

  @Override
  public int count(OIdentifiable iRecord) {
    return 0; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean loadFromConfiguration(ODocument iConfig) {
    return false; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ODocument updateConfiguration() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public OIndex<T> addCluster(String iClusterName) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public OIndex<T> removeCluster(String iClusterName) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  @Override
  public void freeze(boolean throwException) {
    throw new UnsupportedOperationException("freeze");
  }

  @Override
  public void release() {
    throw new UnsupportedOperationException("release");
  }

  @Override
  public void acquireModificationLock() {
    throw new UnsupportedOperationException("acquireModificationLock");
  }

  @Override
  public void releaseModificationLock() {
    throw new UnsupportedOperationException("releaseModificationLock");
  }

  @Override
  public void onCreate(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onCreate");
  }

  @Override
  public void onDelete(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onDelete");
  }

  @Override
  public void onOpen(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onOpen");
  }

  @Override
  public void onBeforeTxBegin(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onBeforeTxBegin");
  }

  @Override
  public void onBeforeTxRollback(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onBeforeTxRollback");
  }

  @Override
  public void onAfterTxRollback(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onAfterTxRollback");
  }

  @Override
  public void onBeforeTxCommit(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onBeforeTxCommit");
  }

  @Override
  public void onAfterTxCommit(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onAfterTxCommit");
  }

  @Override
  public void onClose(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onClose");
  }

  @Override
  public boolean onCorruptionRepairDatabase(ODatabase iDatabase, String iReason, String iWhatWillbeFixed) {
    throw new UnsupportedOperationException("onCorruptionRepairDatabase");
  }

  private static final class BucketPath {
    private final BucketPath parent;
    private final int        hashMapOffset;
    private final int        itemIndex;
    private final int        nodeIndex;
    private final int        nodeGlobalDepth;
    private final int        nodeLocalDepth;

    private BucketPath(BucketPath parent, int hashMapOffset, int itemIndex, int nodeIndex, int nodeLocalDepth, int nodeGlobalDepth) {
      this.parent = parent;
      this.hashMapOffset = hashMapOffset;
      this.itemIndex = itemIndex;
      this.nodeIndex = nodeIndex;
      this.nodeGlobalDepth = nodeGlobalDepth;
      this.nodeLocalDepth = nodeLocalDepth;
    }
  }

  private static final class BucketSplitResult {
    private final long updatedBucketPointer;
    private final long newBucketPointer;
    private final int  newDepth;

    private BucketSplitResult(long updatedBucketPointer, long newBucketPointer, int newDepth) {
      this.updatedBucketPointer = updatedBucketPointer;
      this.newBucketPointer = newBucketPointer;
      this.newDepth = newDepth;
    }
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
}
