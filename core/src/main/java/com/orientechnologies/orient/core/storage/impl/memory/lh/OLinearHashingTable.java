package com.orientechnologies.orient.core.storage.impl.memory.lh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.impl.utils.linearhashing.OGroupOverflowTable;
import com.orientechnologies.orient.core.storage.impl.utils.linearhashing.OLinearHashingHashCalculator;
import com.orientechnologies.orient.core.storage.impl.utils.linearhashing.OLinearHashingIndex;
import com.orientechnologies.orient.core.storage.impl.utils.linearhashing.OPageIndicator;

/**
 * @author Andrey Lomakin
 * @since 23.07.12
 */
public class OLinearHashingTable<K extends OClusterPosition, V extends OPhysicalPosition> {

  private static final int                 FILE_SIZE      = 4096;

  private OLinearHashingIndex              primaryIndex;
  private OLinearHashingIndex              secondaryIndex;
  private int                              level;
  private int                              next;
  private double                           maxCapacity;
  private double                           minCapacity;
  private OGroupOverflowTable              groupOverflowTable;
  private List<OLinearHashingBucket<K, V>> file;
  private OPageIndicator                   pageIndicator;
  private List<V>                          recordPool     = new ArrayList<V>(100);
  private int                              size;
  private static final int                 MAX_GROUP_SIZE = 128;

  public OLinearHashingTable() {
    size = 0;
    level = 0;
    next = 0;
    maxCapacity = 0.8;
    minCapacity = 0.4;
    primaryIndex = new OLinearHashingIndex();
    secondaryIndex = new OLinearHashingIndex();
    groupOverflowTable = new OGroupOverflowTable();
    pageIndicator = new OPageIndicator();

    file = new ArrayList<OLinearHashingBucket<K, V>>(FILE_SIZE);

    file.add(new OLinearHashingBucket<K, V>());

    primaryIndex.addNewPosition();

  }

  public boolean put(V value) {
    int[] hash = calculateBucketNumberAndLevel((K) value.clusterPosition);

    final boolean result = tryInsertIntoChain(hash, value);
    splitBucketsIfNeeded();

    return result;
  }

  private int[] calculateBucketNumberAndLevel(K key) {
    long internalHash = OLinearHashingHashCalculator.INSTANCE.calculateNaturalOrderedHash(key, level);

    final int bucketNumber;
    final int currentLevel;
    if (internalHash < next) {
      long keyHash = OLinearHashingHashCalculator.INSTANCE.calculateNaturalOrderedHash(key, level + 1);
      bucketNumber = OLinearHashingHashCalculator.INSTANCE.calculateBucketNumber(keyHash, level + 1);
      currentLevel = level + 1;
    } else {
      bucketNumber = OLinearHashingHashCalculator.INSTANCE.calculateBucketNumber(internalHash, level);
      currentLevel = level;
    }

    return new int[] { bucketNumber, currentLevel };
  }

  private int calculateNextHash(K key) {
    int keyHash = (int) OLinearHashingHashCalculator.INSTANCE.calculateNaturalOrderedHash(key, level + 1);
    return (int) OLinearHashingHashCalculator.INSTANCE.calculateBucketNumber(keyHash, level + 1);
  }

  private boolean tryInsertIntoChain(final int[] hash, V value) {
    int chainDisplacement = primaryIndex.getChainDisplacement(hash[0]);

    // try to store record in main bucket
    int pageToStore;
    final byte keySignature;

    if (chainDisplacement > 253) {
      final boolean result = storeRecordInBucket(hash[0], value, true);
      if (result)
        size++;

      return result;
    } else {
      byte chainSignature = primaryIndex.getChainSignature(hash[0]);

      keySignature = OLinearHashingHashCalculator.INSTANCE.calculateSignature(value.clusterPosition);
      if (keySignature < chainSignature) {
        moveLargestRecordToRecordPool(hash[0], chainSignature);
        final boolean result = storeRecordInBucket(hash[0], value, true);
        storeRecordFromRecordPool();

        if (result)
          size++;
        return result;
      } else if (keySignature == chainSignature) {
        recordPool.add(value);
        moveLargestRecordToRecordPool(hash[0], chainSignature);
        OLinearHashingBucket bucket = file.get(hash[0]);

        primaryIndex.updateSignature(hash[0], bucket.keys, bucket.size);

        storeRecordFromRecordPool();
        return true;
      } else {
        if (chainDisplacement == 253) {
          // allocate new page
          final boolean result = allocateNewPageAndStore(hash[0], hash[0], value, hash[1], true);
          if (result)
            size++;

          return result;
        } else {
          pageToStore = findNextPageInChain(hash[0], hash[1], chainDisplacement);
        }
      }
    }

    // try to store in overflow bucket chain
    while (true) {
      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToStore);
      chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);

      if (chainDisplacement > 253) {
        final boolean result = storeRecordInBucket(pageToStore, value, false);
        if (result)
          size++;

        return result;
      } else {
        int chainSignature = secondaryIndex.getChainSignature(realPosInSecondaryIndex);
        if (keySignature < chainSignature) {
          moveLargestRecordToRecordPool(pageToStore, (byte) chainSignature);
          final boolean result = storeRecordInBucket(pageToStore, value, false);
          storeRecordFromRecordPool();
          if (result)
            size++;

          return result;
        } else if (keySignature == chainSignature) {
          recordPool.add(value);
          moveLargestRecordToRecordPool(pageToStore, (byte) chainSignature);
          OLinearHashingBucket bucket = file.get(pageToStore);

          secondaryIndex.updateSignature(realPosInSecondaryIndex, bucket.keys, bucket.size);

          storeRecordFromRecordPool();
          return true;
        } else {
          if (chainDisplacement == 253) {
            // allocate new page
            final boolean result = allocateNewPageAndStore(hash[0], pageToStore, value, hash[1], false);
            if (result)
              size++;

            return result;
          } else {
            pageToStore = findNextPageInChain(hash[0], hash[1], chainDisplacement);
          }
        }
      }
    }
  }

  private void splitBucketsIfNeeded() {
    // calculate load factor
    double capacity = ((double) size) / (primaryIndex.bucketCount() * OLinearHashingBucket.BUCKET_MAX_SIZE);
    if (capacity > maxCapacity) {
      int bucketNumberToSplit = OLinearHashingHashCalculator.INSTANCE.calculateBucketNumber(next, level);
      // TODO make this durable by inventing cool record pool
      loadChainInPool(bucketNumberToSplit, level);

      groupOverflowTable.removeUnusedGroups(pageIndicator);

      int pageToStore = next + (1 << level);
      byte groupWithStartingPageLessThan = groupOverflowTable.getGroupWithStartingPageLessThenOrEqual(pageToStore);
      if (groupWithStartingPageLessThan >= 0) {
        int groupSize = groupOverflowTable.getSizeForGroup(groupWithStartingPageLessThan);
        final boolean needMove = pageIndicator.isUsedPageExistInRange(pageToStore, pageToStore + groupSize);
        if (needMove) {
          moveOverflowGroupToNewPosition(pageToStore);
        }
      } else if (groupWithStartingPageLessThan == -1) {
        groupOverflowTable.moveDummyGroup(calculateGroupSize(level + 1));
      } else if (groupWithStartingPageLessThan != -2)
        throw new IllegalStateException("Invalid group  number : " + groupWithStartingPageLessThan);

      primaryIndex.addNewPosition(pageToStore);
      while (file.size() < pageToStore + 1) {
        file.add(null);
      }
      file.set(pageToStore, new OLinearHashingBucket<K, V>());
      groupOverflowTable.moveDummyGroupIfNeeded(pageToStore, calculateGroupSize(level + 1));

      next++;

      if (next == 1 << level) {
        next = 0;
        level++;
      }
      storeRecordFromRecordPool();
    }
  }

  private void mergeBucketIfNeeded() {
    // calculate load factor
    double capacity = ((double) size) / (primaryIndex.bucketCount() * OLinearHashingBucket.BUCKET_MAX_SIZE);
    if (capacity < minCapacity && level > 0) {
      // TODO make this durable by inventing cool record pool
      final int naturalOrderKey1;
      final int bucketNumberToMerge1;
      final int bucketNumberToMerge2;
      final int currentLevel;
      if (next == 0) {
        currentLevel = level;
        naturalOrderKey1 = (1 << level) - 2;
        bucketNumberToMerge1 = OLinearHashingHashCalculator.INSTANCE.calculateBucketNumber(naturalOrderKey1, level);
        bucketNumberToMerge2 = (1 << level) - 1;
      } else {
        currentLevel = level + 1;
        naturalOrderKey1 = 2 * (next - 1);
        bucketNumberToMerge1 = OLinearHashingHashCalculator.INSTANCE.calculateBucketNumber(naturalOrderKey1, level + 1);
        bucketNumberToMerge2 = next - 1 + (1 << level);
      }
      loadChainInPool(bucketNumberToMerge1, currentLevel);
      loadChainInPool(bucketNumberToMerge2, currentLevel);

      primaryIndex.remove(bucketNumberToMerge2);
      file.set(bucketNumberToMerge2, null);

      next--;

      if (next < 0) {
        level--;
        next = (1 << level) - 1;
      }

      storeRecordFromRecordPool();
    }
  }

  private void moveOverflowGroupToNewPosition(int page) {
    List<OGroupOverflowTable.GroupOverflowInfo> groupsToMove = groupOverflowTable.getOverflowGroupsInfoToMove(page);

    for (OGroupOverflowTable.GroupOverflowInfo groupOverflowInfo : groupsToMove) {
      int oldPage = groupOverflowInfo.getStartingPage();
      int groupSize = groupOverflowTable.getSizeForGroup(groupOverflowInfo.getGroup());
      int newPage = groupOverflowTable.move(groupOverflowInfo.getGroup(), groupSize);

      moveGroupToNewPosition(oldPage, newPage, groupSize);

    }
  }

  private void moveGroupToNewPosition(int oldPage, int newPage, int groupSize) {
    while (file.size() < newPage + groupSize + 1) {
      file.add(null);
    }
    for (int i = oldPage; i < oldPage + groupSize; ++i) {
      if (pageIndicator.get(i)) {
        final OLinearHashingBucket<K, V> bucket = file.get(i);
        file.set(i - oldPage + newPage, bucket);
        file.set(i, null);
        // move resords in secondary index
        int oldPositionInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(i);
        pageIndicator.set(i - oldPage + newPage);
        pageIndicator.unset(i);
        int newPositionInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(i - oldPage + newPage);

        secondaryIndex.moveRecord(oldPositionInSecondaryIndex, newPositionInSecondaryIndex);
      }
    }
  }

  private void loadChainInPool(final int bucketNumber, final int currentLevel) {
    Collection<V> content = file.get(bucketNumber).getContent();
    size -= content.size();
    recordPool.addAll(content);
    file.get(bucketNumber).emptyBucket();

    int displacement = primaryIndex.getChainDisplacement(bucketNumber);
    int pageToUse;

    while (displacement < 253) {
      pageToUse = findNextPageInChain(bucketNumber, currentLevel, displacement);

      content = file.get(pageToUse).getContent();
      size -= content.size();
      recordPool.addAll(content);
      file.get(pageToUse).emptyBucket();

      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToUse);
      displacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);

      // free index
      pageIndicator.unset(pageToUse);
      secondaryIndex.remove(realPosInSecondaryIndex);
    }

    primaryIndex.clearChainInfo(bucketNumber);
  }

  private void storeRecordFromRecordPool() {
    while (!recordPool.isEmpty()) {
      V key = recordPool.remove(0);

      if (!put(key)) {
        throw new ODatabaseException("Error while saving record " + key + " from record pool");
      }
    }
  }

  private void moveLargestRecordToRecordPool(int chainNumber, byte signature) {
    final OLinearHashingBucket<K, V> bucket = file.get(chainNumber);
    List<V> largestRecords = bucket.getLargestRecords(signature);
    recordPool.addAll(largestRecords);
    size -= largestRecords.size();
  }

  private int findNextPageInChain(int bucketNumber, int currentLevel, int chainDisplacement) {
    byte groupNumber = calculateGroupNumber(bucketNumber, currentLevel);
    int startingPage = groupOverflowTable.getPageForGroup(groupNumber);
    if (startingPage == -1) {
      return -1;
    }
    return startingPage + chainDisplacement;
  }

  private boolean allocateNewPageAndStore(int bucketNumber, int pageToStore, V value, int currentLevel, boolean mainChain) {
    int groupSize = calculateGroupSize(currentLevel);
    byte groupNumber = calculateGroupNumber(bucketNumber, currentLevel);

    int[] pos = groupOverflowTable.searchForGroupOrCreate(groupNumber, groupSize);

    int pageToUse = pageIndicator.getFirstEmptyPage(pos[0], pos[1]);

    int actualStartingPage = pos[0];

    if (pageToUse == -1) {
      if (pos[1] == MAX_GROUP_SIZE) {
        throw new OGroupOverflowException("There is no empty page for group size " + groupSize + " because pages "
            + pageIndicator.toString() + " are already allocated." + "Starting page is " + pos[0]);
      } else {
        groupSize = pos[1] * 2;
        int newStartingPage = groupOverflowTable.enlargeGroupSize(groupNumber, groupSize);
        moveGroupToNewPosition(pos[0], newStartingPage, pos[1]);
        pageToUse = pageIndicator.getFirstEmptyPage(newStartingPage, groupSize);
        actualStartingPage = newStartingPage;
      }
    }

    // update displacement of existing index element
    if (mainChain) {
      primaryIndex.updateDisplacement(pageToStore, (byte) (pageToUse - actualStartingPage));
    } else {
      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(actualStartingPage + pageToStore - pos[0]);
      secondaryIndex.updateDisplacement(realPosInSecondaryIndex, (byte) (pageToUse - actualStartingPage));
    }

    pageIndicator.set(pageToUse);
    int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToUse);
    secondaryIndex.addNewPosition(realPosInSecondaryIndex);
    final OLinearHashingBucket<K, V> bucket = new OLinearHashingBucket<K, V>();
    while (file.size() < pageToUse + 1) {
      file.add(null);
    }
    file.set(pageToUse, bucket);

    return storeRecordInBucket(pageToUse, value, false);
  }

  private boolean storeRecordInBucket(final int bucketNumber, V value, boolean mainBucket) {

    final OLinearHashingBucket bucket = file.get(bucketNumber);

    for (int i = 0; i < bucket.size; i++) {
      if (value.clusterPosition.equals(bucket.keys[i])) {
        return false;
      }
    }

    bucket.keys[bucket.size] = value.clusterPosition;
    bucket.values[bucket.size] = value;
    bucket.size++;

    final int positionInIndex;
    final OLinearHashingIndex indexToUse;
    if (mainBucket) {
      positionInIndex = bucketNumber;
      indexToUse = primaryIndex;
    } else {
      positionInIndex = pageIndicator.getRealPosInSecondaryIndex(bucketNumber);
      indexToUse = secondaryIndex;
    }
    int displacement = indexToUse.changeDisplacementAfterInsertion(positionInIndex, bucket.size);

    if (bucket.size == OLinearHashingBucket.BUCKET_MAX_SIZE && displacement > 253) {
      throw new IllegalStateException("if bucket size is max displacement can't be greater than 253");
    }

    if (displacement <= 253) {
      indexToUse.updateSignature(positionInIndex, bucket.keys, bucket.size);
    }
    return true;
  }

  private byte calculateGroupNumber(int bucketNumber, int currentLevel) {
    final int groupSize;

    groupSize = calculateGroupSize(currentLevel);

    int x = (1 << currentLevel) + bucketNumber - 1;
    int y = x / groupSize;
    return (byte) (y % 31);
  }

  private int calculateGroupSize(final int currentLevel) {
    int divisor = 0;
    byte no = -1;
    double nogps;
    do {
      if (divisor < 128) {
        no++;
      }
      divisor = 1 << (2 + no);
      nogps = (1 << currentLevel) / divisor;

    } while (!((nogps <= 31) || (divisor == 128)));

    return divisor;
  }

  public boolean contains(K clusterPosition) {
    final int[] hash = calculateBucketNumberAndLevel(clusterPosition);

    byte keySignature = OLinearHashingHashCalculator.INSTANCE.calculateSignature(clusterPosition);
    byte signature = primaryIndex.getChainSignature(hash[0]);
    int pageNumberToUse = hash[0];

    int chainDisplacement = primaryIndex.getChainDisplacement(hash[0]);
    byte groupNumber = calculateGroupNumber(hash[0], hash[1]);
    int pageNumber = groupOverflowTable.getPageForGroup(groupNumber);

    while (true) {
      if (keySignature > signature) {
        if (chainDisplacement >= 253) {
          return false;
        } else {
          pageNumberToUse = pageNumber + chainDisplacement;
          int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
          signature = secondaryIndex.getChainSignature(realPosInSecondaryIndex);
          chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
        }
      } else {
        final OLinearHashingBucket<K, V> bucket = file.get(pageNumberToUse);
        return bucket != null && bucket.get(clusterPosition) != null;
      }

    }
  }

  public V delete(K key) {
    final int[] hash = calculateBucketNumberAndLevel(key);

    byte keySignature = OLinearHashingHashCalculator.INSTANCE.calculateSignature(key);
    byte signature = primaryIndex.getChainSignature(hash[0]);
    int pageNumberToUse = hash[0];

    int chainDisplacement = primaryIndex.getChainDisplacement(hash[0]);
    byte groupNumber = calculateGroupNumber(hash[0], hash[1]);
    int pageNumber = groupOverflowTable.getPageForGroup(groupNumber);

    int prevPage = hash[0];
    while (true) {
      if (keySignature > signature) {
        if (chainDisplacement >= 253) {
          return null;
        } else {
          prevPage = pageNumberToUse;
          pageNumberToUse = pageNumber + chainDisplacement;
          int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
          signature = secondaryIndex.getChainSignature(realPosInSecondaryIndex);
          chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
        }
      } else {
        OLinearHashingBucket<K, V> bucket = file.get(pageNumberToUse);
        int position = bucket.getPosition(key);
        final V deletedValue;
        if (position >= 0) {
          deletedValue = bucket.values[position];
          bucket.deleteEntry(position);

          // move record from successor to current bucket
          while (chainDisplacement < 253) {
            prevPage = pageNumberToUse;
            pageNumberToUse = pageNumber + chainDisplacement;
            int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
            chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
            OLinearHashingBucket<K, V> secondBucket = file.get(pageNumberToUse);
            List<V> smallestRecords = secondBucket.getSmallestRecords(OLinearHashingBucket.BUCKET_MAX_SIZE - bucket.size);
            if (smallestRecords.isEmpty()) {
              // do nothing!
            } else {
              // move this records to predecessor
              bucket.add(smallestRecords);

              for (V smallestRecord : smallestRecords) {
                if (secondBucket.deleteKey(smallestRecord.clusterPosition) < 0) {
                  throw new IllegalStateException("error while deleting record to move it to predecessor bucket");
                }
              }
              secondaryIndex.updateSignature(realPosInSecondaryIndex, secondBucket.keys, secondBucket.size);
            }

            // update signatures after removing some records from buckets
            if (prevPage == hash[0]) {
              if (primaryIndex.getChainDisplacement(hash[0]) > 253) {
                primaryIndex.updateSignature(hash[0], Byte.MAX_VALUE);
              } else {
                primaryIndex.updateSignature(hash[0], bucket.keys, bucket.size);
              }
            } else {
              int indexPosition = pageIndicator.getRealPosInSecondaryIndex(prevPage);
              if (primaryIndex.getChainDisplacement(hash[0]) > 253) {
                secondaryIndex.updateSignature(indexPosition, Byte.MAX_VALUE);
              } else {
                secondaryIndex.updateSignature(indexPosition, bucket.keys, bucket.size);
              }
            }

            bucket = secondBucket;
          }

          // update displacement and signature in last bucket
          if (pageNumberToUse == hash[0]) {
            // main bucket does not have overflow chain
            int displacement = primaryIndex.decrementDisplacement(hash[0], bucket.size);
            if (displacement <= 253) {
              primaryIndex.updateSignature(hash[0], bucket.keys, bucket.size);
            } else {
              primaryIndex.updateSignature(hash[0], Byte.MAX_VALUE);
            }
          } else {
            int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
            if (bucket.size == 0) {
              secondaryIndex.remove(realPosInSecondaryIndex);
              pageIndicator.unset(pageNumberToUse);
              // set prev bucket in chain correct displacement
              if (prevPage == hash[0]) {
                int displacement = primaryIndex.changeDisplacementAfterDeletion(hash[0], file.get(hash[0]).size, true);
                if (displacement > 253) {
                  primaryIndex.updateSignature(hash[0], Byte.MAX_VALUE);
                }
              } else {
                int prevIndexPosition = pageIndicator.getRealPosInSecondaryIndex(prevPage);
                int displacement = secondaryIndex.changeDisplacementAfterDeletion(prevIndexPosition, file.get(prevPage).size, true);
                if (displacement > 253) {
                  secondaryIndex.updateSignature(prevIndexPosition, Byte.MAX_VALUE);
                }
              }
            } else {
              int displacement = secondaryIndex.decrementDisplacement(realPosInSecondaryIndex, bucket.size);
              if (displacement <= 253) {
                secondaryIndex.updateSignature(realPosInSecondaryIndex, bucket.keys, bucket.size);
              } else {
                secondaryIndex.updateSignature(realPosInSecondaryIndex, Byte.MAX_VALUE);
              }
            }
          }
        } else {
          return null;
        }
        size--;
        mergeBucketIfNeeded();
        return deletedValue;
      }
    }
  }

  public void clear() {
    file.clear();
    primaryIndex.clear();
    secondaryIndex.clear();
    pageIndicator.clear();
    groupOverflowTable.clear();

    file.add(new OLinearHashingBucket<K, V>());

    primaryIndex.addNewPosition();
  }

  public long size() {
    return size;
  }

  public V get(K clusterPosition) {

    final int[] hash = calculateBucketNumberAndLevel(clusterPosition);

    byte keySignature = OLinearHashingHashCalculator.INSTANCE.calculateSignature(clusterPosition);
    byte signature = primaryIndex.getChainSignature(hash[0]);
    int pageNumberToUse = hash[0];

    int chainDisplacement = primaryIndex.getChainDisplacement(hash[0]);
    byte groupNumber = calculateGroupNumber(hash[0], hash[1]);

    int pageNumber = groupOverflowTable.getPageForGroup(groupNumber);

    while (true) {
      if (keySignature > signature) {
        if (chainDisplacement >= 253) {
          return null;
        } else {
          pageNumberToUse = pageNumber + chainDisplacement;
          int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
          signature = secondaryIndex.getChainSignature(realPosInSecondaryIndex);
          chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
        }
      } else {
        OLinearHashingBucket<K, V> bucket = file.get(pageNumberToUse);
        if (bucket != null) {
          return bucket.get(clusterPosition);
        }
        return null;
      }

    }
  }

  public Entry<K, V>[] higherEntries(K currentRecord) {
    int[] bucketLevelAndOrder = calculateBucketNumberAndLevel(currentRecord);

    Entry<K, V>[] result = fetchHigherEntries(currentRecord, bucketLevelAndOrder[0], bucketLevelAndOrder[1]);
    if (result.length > 0)
      return result;

    int step = 1;
    bucketLevelAndOrder = calculateNextBucketNumberAndLevel(currentRecord, step);

    while (bucketLevelAndOrder != null) {
      result = fetchHigherEntries(currentRecord, bucketLevelAndOrder[0], bucketLevelAndOrder[1]);
      if (result.length > 0)
        return result;

      step++;
      bucketLevelAndOrder = calculateNextBucketNumberAndLevel(currentRecord, step);
    }

    return new Entry[0];
  }

  public Entry<K, V>[] ceilingEntries(K currentRecord) {
    int[] bucketLevelAndOrder = calculateBucketNumberAndLevel(currentRecord);

    Entry<K, V>[] result = fetchCeilingEntries(currentRecord, bucketLevelAndOrder[0], bucketLevelAndOrder[1]);
    if (result.length > 0)
      return result;

    int step = 1;
    bucketLevelAndOrder = calculateNextBucketNumberAndLevel(currentRecord, step);

    while (bucketLevelAndOrder != null) {
      result = fetchCeilingEntries(currentRecord, bucketLevelAndOrder[0], bucketLevelAndOrder[1]);
      if (result.length > 0)
        return result;

      step++;
      bucketLevelAndOrder = calculateNextBucketNumberAndLevel(currentRecord, step);
    }

    return new Entry[0];
  }

  public Entry<K, V>[] lowerEntries(K currentRecord) {
    int[] bucketLevelAndOrder = calculateBucketNumberAndLevel(currentRecord);

    Entry<K, V>[] result = fetchLessThanEntries(currentRecord, bucketLevelAndOrder[0], bucketLevelAndOrder[1]);
    if (result.length > 0)
      return result;

    int step = 1;
    bucketLevelAndOrder = calculatePrevBucketNumberAndLevel(currentRecord, step);

    while (bucketLevelAndOrder != null) {
      result = fetchLessThanEntries(currentRecord, bucketLevelAndOrder[0], bucketLevelAndOrder[1]);
      if (result.length > 0)
        return result;

      step++;
      bucketLevelAndOrder = calculatePrevBucketNumberAndLevel(currentRecord, step);
    }

    return new Entry[0];
  }

  public Entry<K, V>[] floorEntries(K currentRecord) {
    int[] bucketLevelAndOrder = calculateBucketNumberAndLevel(currentRecord);

    Entry<K, V>[] result = fetchFloorEntries(currentRecord, bucketLevelAndOrder[0], bucketLevelAndOrder[1]);
    if (result.length > 0)
      return result;

    int step = 1;
    bucketLevelAndOrder = calculatePrevBucketNumberAndLevel(currentRecord, step);

    while (bucketLevelAndOrder != null) {
      result = fetchFloorEntries(currentRecord, bucketLevelAndOrder[0], bucketLevelAndOrder[1]);
      if (result.length > 0)
        return result;

      step++;
      bucketLevelAndOrder = calculatePrevBucketNumberAndLevel(currentRecord, step);
    }

    return new Entry[0];
  }

  private Entry<K, V>[] fetchHigherEntries(K keyToCompare, int bucketNumber, int level) {
    int displacement = primaryIndex.getChainDisplacement(bucketNumber);

    OLinearHashingBucket<K, V> bucket = file.get(bucketNumber);
    final ArrayList<Entry<K, V>> result = new ArrayList<Entry<K, V>>();
    addHigherEntriesFromBucket(keyToCompare, bucket, result);
    // load buckets from overflow positions
    while (displacement < 253) {
      int pageToUse = findNextPageInChain(bucketNumber, level, displacement);
      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToUse);
      displacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
      bucket = file.get(pageToUse);

      addHigherEntriesFromBucket(keyToCompare, bucket, result);
    }

    Entry<K, V>[] entries = new Entry[result.size()];
    entries = result.toArray(entries);
    Arrays.sort(entries);

    return entries;
  }

  private Entry<K, V>[] fetchCeilingEntries(K keyToCompare, int bucketNumber, int level) {
    int displacement = primaryIndex.getChainDisplacement(bucketNumber);

    OLinearHashingBucket<K, V> bucket = file.get(bucketNumber);
    final ArrayList<Entry<K, V>> result = new ArrayList<Entry<K, V>>();
    addCeilingEntriesFromBucket(keyToCompare, bucket, result);
    // load buckets from overflow positions
    while (displacement < 253) {
      int pageToUse = findNextPageInChain(bucketNumber, level, displacement);
      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToUse);
      displacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
      bucket = file.get(pageToUse);

      addCeilingEntriesFromBucket(keyToCompare, bucket, result);
    }

    Entry<K, V>[] entries = new Entry[result.size()];
    entries = result.toArray(entries);
    Arrays.sort(entries);

    return entries;
  }

  private void addCeilingEntriesFromBucket(K keyToCompare, OLinearHashingBucket<K, V> bucket, ArrayList<Entry<K, V>> result) {
    for (int i = 0; i < bucket.size; i++) {
      if (bucket.keys[i].compareTo(keyToCompare) >= 0)
        result.add(new Entry<K, V>(bucket.keys[i], bucket.values[i]));
    }
  }

  private void addHigherEntriesFromBucket(K keyToCompare, OLinearHashingBucket<K, V> bucket, ArrayList<Entry<K, V>> result) {
    for (int i = 0; i < bucket.size; i++) {
      if (bucket.keys[i].compareTo(keyToCompare) > 0)
        result.add(new Entry<K, V>(bucket.keys[i], bucket.values[i]));
    }
  }

  private Entry<K, V>[] fetchLessThanEntries(K keyToCompare, int bucketNumber, int level) {
    int displacement = primaryIndex.getChainDisplacement(bucketNumber);

    OLinearHashingBucket<K, V> bucket = file.get(bucketNumber);
    final ArrayList<Entry<K, V>> result = new ArrayList<Entry<K, V>>();
    addLowerEntriesFromBucket(keyToCompare, bucket, result);
    // load buckets from overflow positions
    while (displacement < 253) {
      int pageToUse = findNextPageInChain(bucketNumber, level, displacement);
      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToUse);
      displacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
      bucket = file.get(pageToUse);

      addLowerEntriesFromBucket(keyToCompare, bucket, result);
    }

    Entry<K, V>[] entries = new Entry[result.size()];
    entries = result.toArray(entries);
    Arrays.sort(entries);

    return entries;
  }

  private Entry<K, V>[] fetchFloorEntries(K keyToCompare, int bucketNumber, int level) {
    int displacement = primaryIndex.getChainDisplacement(bucketNumber);

    OLinearHashingBucket<K, V> bucket = file.get(bucketNumber);
    final ArrayList<Entry<K, V>> result = new ArrayList<Entry<K, V>>();
    addFloorEntriesFromBucket(keyToCompare, bucket, result);
    // load buckets from overflow positions
    while (displacement < 253) {
      int pageToUse = findNextPageInChain(bucketNumber, level, displacement);
      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToUse);
      displacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
      bucket = file.get(pageToUse);

      addFloorEntriesFromBucket(keyToCompare, bucket, result);
    }

    Entry<K, V>[] entries = new Entry[result.size()];
    entries = result.toArray(entries);
    Arrays.sort(entries);

    return entries;
  }

  private void addFloorEntriesFromBucket(K keyToCompare, OLinearHashingBucket<K, V> bucket, ArrayList<Entry<K, V>> result) {
    for (int i = 0; i < bucket.size; i++) {
      if (bucket.keys[i].compareTo(keyToCompare) <= 0)
        result.add(new Entry<K, V>(bucket.keys[i], bucket.values[i]));
    }
  }

  private void addLowerEntriesFromBucket(K keyToCompare, OLinearHashingBucket<K, V> bucket, ArrayList<Entry<K, V>> result) {
    for (int i = 0; i < bucket.size; i++) {
      if (bucket.keys[i].compareTo(keyToCompare) < 0)
        result.add(new Entry<K, V>(bucket.keys[i], bucket.values[i]));
    }
  }

  private int[] calculateNextBucketNumberAndLevel(K currentRecord, int step) {
    int currentLevel = level;

    long naturalOrderedKey = OLinearHashingHashCalculator.INSTANCE.calculateNaturalOrderedHash(currentRecord, currentLevel);
    if (naturalOrderedKey < next)
      currentLevel++;

    naturalOrderedKey = OLinearHashingHashCalculator.INSTANCE.calculateNaturalOrderedHash(currentRecord, currentLevel);

    naturalOrderedKey += step;
    if (currentLevel > level && naturalOrderedKey >= 2 * next) {
      currentLevel--;
      naturalOrderedKey = naturalOrderedKey / 2;
    }

    if (naturalOrderedKey >= 1 << currentLevel)
      return null;

    final int bucketNumber = OLinearHashingHashCalculator.INSTANCE.calculateBucketNumber(naturalOrderedKey, currentLevel);

    return new int[] { bucketNumber, currentLevel };
  }

  private int[] calculatePrevBucketNumberAndLevel(K currentRecord, int step) {
    int currentLevel;
    currentLevel = level;
    long naturalOrderedKey = OLinearHashingHashCalculator.INSTANCE.calculateNaturalOrderedHash(currentRecord, currentLevel);
    if (naturalOrderedKey < next)
      currentLevel++;

    naturalOrderedKey = OLinearHashingHashCalculator.INSTANCE.calculateNaturalOrderedHash(currentRecord, currentLevel);
    naturalOrderedKey -= step;

    if (naturalOrderedKey < 0)
      return null;

    if (currentLevel == level && naturalOrderedKey < next) {
      naturalOrderedKey = naturalOrderedKey * 2 + 1;
      currentLevel++;
    }

    final int bucketNumber = OLinearHashingHashCalculator.INSTANCE.calculateBucketNumber(naturalOrderedKey, currentLevel);

    return new int[] { bucketNumber, currentLevel };
  }

  public static final class Entry<K extends OClusterPosition, V extends OPhysicalPosition> implements Comparable<Entry<K, V>> {
    public final K key;
    public final V value;

    public Entry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int compareTo(Entry<K, V> otherEntry) {
      return key.compareTo(otherEntry.key);
    }
  }
}
