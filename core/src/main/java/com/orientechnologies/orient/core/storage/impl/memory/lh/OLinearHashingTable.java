package com.orientechnologies.orient.core.storage.impl.memory.lh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

/**
 * @author Andrey Lomakin
 * @since 23.07.12
 */
public class OLinearHashingTable<K extends OClusterPosition, V extends OPhysicalPosition> {

  private static final int           FILE_SIZE      = 4096;

  private OLinearHashingIndex        primaryIndex;
  private OLinearHashingIndex        secondaryIndex;
  private final int                  CHAIN_NUMBER;
  private int                        level;
  private int                        next;
  private double                     maxCapacity;
  private double                     minCapacity;
  private OGroupOverflowTable        groupOverflowTable;
  private List<OLinearHashingBucket> file;
  private OPageIndicator             pageIndicator;
  private List<V>                    recordPool     = new ArrayList<V>(100);
  private int                        size;
  private static final int           MAX_GROUP_SIZE = 128;

  public OLinearHashingTable() {
    CHAIN_NUMBER = 1;
    size = 0;
    level = 0;
    next = 0;
    maxCapacity = 0.8;
    minCapacity = 0.7;
    primaryIndex = new OLinearHashingIndex();
    secondaryIndex = new OLinearHashingIndex();
    groupOverflowTable = new OGroupOverflowTable();
    pageIndicator = new OPageIndicator();

    file = new ArrayList<OLinearHashingBucket>(FILE_SIZE);

    file.add(new OLinearHashingBucket());

    primaryIndex.addNewPosition();

  }

  public boolean put(V value) {
    int[] hash = calculateHash((K) value.clusterPosition);

    final boolean result = tryInsertIntoChain(hash, value);
    if (result) {
      size++;
    }
    splitBucketsIfNeeded();

    return result;
  }

  private int[] calculateHash(K key) {
    int internalHash = OLinearHashingHashCalculatorFactory.INSTANCE.calculateNaturalOrderedHash(key, level);

    final int bucketNumber;
    final int currentLevel;
    if (internalHash < next) {
      bucketNumber = calculateNextHash(key);
      currentLevel = level + 1;
    } else {
      bucketNumber = OLinearHashingHashCalculatorFactory.INSTANCE.calculateBucketNumber(internalHash, level);
      currentLevel = level;
    }

    return new int[] { bucketNumber, currentLevel };
  }

  private int calculateNextHash(K key) {
    int keyHash = OLinearHashingHashCalculatorFactory.INSTANCE.calculateNaturalOrderedHash(key, level + 1);
    return OLinearHashingHashCalculatorFactory.INSTANCE.calculateBucketNumber(keyHash, level + 1);
  }

  private boolean tryInsertIntoChain(final int[] hash, V value) {
    int chainDisplacement = primaryIndex.getChainDisplacement(hash[0]);

    // try to store record in main bucket
    int pageToStore;
    final byte keySignature;

    if (chainDisplacement > 253) {
      return storeRecordInBucket(hash[0], value, true);
    } else {
      byte chainSignature = primaryIndex.getChainSignature(hash[0]);

      keySignature = OLinearHashingHashCalculatorFactory.INSTANCE.calculateSignature(value.clusterPosition);
      if (keySignature < chainSignature) {
        moveLargestRecordToRecordPool(hash[0], chainSignature);
        final boolean result = storeRecordInBucket(hash[0], value, true);
        storeRecordFromRecordPool();
        return result;
      } else if (keySignature == chainSignature) {
        recordPool.add(value);
        size--;
        moveLargestRecordToRecordPool(hash[0], chainSignature);
        OLinearHashingBucket bucket = file.get(hash[0]);

        primaryIndex.updateSignature(hash[0], bucket.keys, bucket.size);

        storeRecordFromRecordPool();
        return true;
      } else {
        if (chainDisplacement == 253) {
          // allocate new page

          return allocateNewPageAndStore(hash[0], hash[0], value, hash[1], true);

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
        return storeRecordInBucket(pageToStore, value, false);
      } else {
        int chainSignature = secondaryIndex.getChainSignature(realPosInSecondaryIndex);
        if (keySignature < chainSignature) {
          moveLargestRecordToRecordPool(pageToStore, (byte) chainSignature);
          final boolean result = storeRecordInBucket(pageToStore, value, false);
          storeRecordFromRecordPool();
          return result;
        } else if (keySignature == chainSignature) {
          recordPool.add(value);
          size--;
          moveLargestRecordToRecordPool(pageToStore, (byte) chainSignature);
          OLinearHashingBucket bucket = file.get(pageToStore);

          secondaryIndex.updateSignature(realPosInSecondaryIndex, bucket.keys, bucket.size);

          storeRecordFromRecordPool();
          return true;
        } else {
          if (chainDisplacement == 253) {
            // allocate new page
            return allocateNewPageAndStore(hash[0], pageToStore, value, hash[1], false);
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
      int bucketNumberToSplit = OLinearHashingHashCalculatorFactory.INSTANCE.calculateBucketNumber(next, level);
      // TODO make this durable by inventing cool record pool
      loadChainInPool(bucketNumberToSplit, level);
      int pageToStore = next + (int) Math.pow(2, level);
      byte groupWithStartingPageLessThan = groupOverflowTable.getGroupWithStartingPageLessThenOrEqual(pageToStore);
      if (groupWithStartingPageLessThan != -2) {
        int groupSize = groupOverflowTable.getSizeForGroup(groupWithStartingPageLessThan);
        boolean needMove = false;
        // TODO use group size from group overflow to prevent collisions
        for (int i = pageToStore; i < pageToStore + groupSize; ++i) {
          if (pageIndicator.get(i)) {
            needMove = true;
            break;
          }
        }

        if (needMove) {
          moveOverflowGroupToNewPosition(pageToStore);
        }
      }

      primaryIndex.addNewPosition(pageToStore);
      while (file.size() < pageToStore + 1) {
        file.add(null);
      }
      file.set(pageToStore, new OLinearHashingBucket());
      groupOverflowTable.moveDummyGroupIfNeeded(pageToStore, calculateGroupSize(level + 1));

      next++;

      if (next == (CHAIN_NUMBER * Math.pow(2, level))) {
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
        naturalOrderKey1 = (int) (CHAIN_NUMBER * Math.pow(2, level)) - 2;
        bucketNumberToMerge1 = OLinearHashingHashCalculatorFactory.INSTANCE.calculateBucketNumber(naturalOrderKey1, level);
        bucketNumberToMerge2 = (int) Math.pow(2, level) - 1;
      } else {
        currentLevel = level + 1;
        naturalOrderKey1 = 2 * (next - 1);
        bucketNumberToMerge1 = OLinearHashingHashCalculatorFactory.INSTANCE.calculateBucketNumber(naturalOrderKey1, level + 1);
        bucketNumberToMerge2 = next - 1 + (int) Math.pow(2, level);
      }
      loadChainInPool(bucketNumberToMerge1, currentLevel);
      loadChainInPool(bucketNumberToMerge2, currentLevel);

      primaryIndex.remove(bucketNumberToMerge2);
      file.set(bucketNumberToMerge2, null);

      next--;

      if (next < 0) {
        level--;
        next = (int) (CHAIN_NUMBER * Math.pow(2, level) - 1);
      }

      storeRecordFromRecordPool();
    }
  }

  private void moveOverflowGroupToNewPosition(int page) {
    List<OGroupOverflowTable.GroupOverflowInfo> groupsToMove = groupOverflowTable.getOverflowGroupsInfoToMove(page);

    for (OGroupOverflowTable.GroupOverflowInfo groupOverflowInfo : groupsToMove) {
      int oldPage = groupOverflowInfo.startingPage;
      int groupSize = groupOverflowTable.getSizeForGroup(groupOverflowInfo.group);
      int newPage = groupOverflowTable.move(groupOverflowInfo.group, groupSize);

      moveGroupToNewPosition(oldPage, newPage, groupSize);

    }
  }

  private void moveGroupToNewPosition(int oldPage, int newPage, int groupSize) {
    while (file.size() < newPage + groupSize + 1) {
      file.add(null);
    }
    for (int i = oldPage; i < oldPage + groupSize; ++i) {
      if (pageIndicator.get(i)) {
        OLinearHashingBucket bucket = file.get(i);
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

    groupOverflowTable.removeUnusedGroups(pageIndicator);
    primaryIndex.clearChainInfo(bucketNumber);
  }

  private void storeRecordFromRecordPool() {
    while (!recordPool.isEmpty()) {
      V key = recordPool.remove(0);

      // TODO be sure that key successfully stored
      if (!put(key)) {
        throw new RuntimeException("error while saving records from pool");
      }
    }
  }

  private void moveLargestRecordToRecordPool(int chainNumber, byte signature) {
    OLinearHashingBucket bucket = file.get(chainNumber);
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
    int groupSize = calculateGroupSize(level);
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
    OLinearHashingBucket bucket = new OLinearHashingBucket();
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
    int displacement = indexToUse.incrementChainDisplacement(positionInIndex, bucket.size);

    if (bucket.size == OLinearHashingBucket.BUCKET_MAX_SIZE && displacement > 253) {
      throw new RuntimeException("this can't be true");
    }

      byte [] signatures = new byte[bucket.size];
      for (int i = 0, keysLength = bucket.size; i < keysLength; i++){
        signatures[i] = OLinearHashingHashCalculatorFactory.INSTANCE.calculateSignature(bucket.keys[i]);
      }


    if (displacement <= 253) {
      indexToUse.updateSignature(positionInIndex, bucket.keys, bucket.size);
    }

    return true;
  }

  private byte calculateGroupNumber(int bucketNumber, int currentLevel) {
    final int groupSize;

    groupSize = calculateGroupSize(currentLevel);

    int x = (int) (CHAIN_NUMBER * Math.pow(2, currentLevel) + bucketNumber - 1);
    int y = x / groupSize;
    return (byte) (y % 31);
  }

  private int calculateGroupSize(final int iLevel) {
    int divisor = 0;
    byte no = -1;
    double nogps;
    do {
      if (divisor < 128) {
        no++;
      }
      divisor = (int) Math.pow(2, 2 + no);
      nogps = (CHAIN_NUMBER * Math.pow(2, iLevel)) / divisor;

    } while (!((nogps <= 31) || (divisor == 128)));

    return divisor;
  }

  public boolean contains(K clusterPosition) {
    final int[] hash = calculateHash(clusterPosition);

    byte keySignature = OLinearHashingHashCalculatorFactory.INSTANCE.calculateSignature(clusterPosition);
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
        OLinearHashingBucket bucket = file.get(pageNumberToUse);
        return bucket != null && bucket.get(clusterPosition) != null;
      }

    }
  }

  public boolean delete(K key) {
    final int[] hash = calculateHash(key);

    byte keySignature = OLinearHashingHashCalculatorFactory.INSTANCE.calculateSignature(key);
    byte signature = primaryIndex.getChainSignature(hash[0]);
    int pageNumberToUse = hash[0];

    int chainDisplacement = primaryIndex.getChainDisplacement(hash[0]);
    byte groupNumber = calculateGroupNumber(hash[0], hash[1]);
    int pageNumber = groupOverflowTable.getPageForGroup(groupNumber);

    int prevPage = hash[0];
    while (true) {
      if (keySignature > signature) {
        if (chainDisplacement >= 253) {
          return false;
        } else {
          prevPage = pageNumberToUse;
          pageNumberToUse = pageNumber + chainDisplacement;
          int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
          signature = secondaryIndex.getChainSignature(realPosInSecondaryIndex);
          chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
        }
      } else {
        OLinearHashingBucket bucket = file.get(pageNumberToUse);
        int position = bucket.deleteKey(key);
        if (position >= 0) {
          // move record from successor to current bucket
          while (chainDisplacement < 253) {
            prevPage = pageNumberToUse;
            pageNumberToUse = pageNumber + chainDisplacement;
            int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
            chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
            OLinearHashingBucket secondBucket = file.get(pageNumberToUse);
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
                int displacement = primaryIndex.decrementDisplacement(hash[0], file.get(hash[0]).size, true);
                if (displacement > 253) {
                  primaryIndex.updateSignature(hash[0], Byte.MAX_VALUE);
                }
              } else {
                int prevIndexPosition = pageIndicator.getRealPosInSecondaryIndex(prevPage);
                int displacement = secondaryIndex.decrementDisplacement(prevIndexPosition, file.get(prevPage).size, true);
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
          return false;
        }
        size--;
        mergeBucketIfNeeded();
        return true;
      }
    }
  }

  public void clear() {
    file.clear();
    primaryIndex.clear();
    secondaryIndex.clear();
    pageIndicator.clear();
    groupOverflowTable.clear();

    file.add(new OLinearHashingBucket());

    primaryIndex.addNewPosition();
  }

  public long size() {
    return size;
  }

  public V get(K clusterPosition) {

    final int[] hash = calculateHash(clusterPosition);

    byte keySignature = OLinearHashingHashCalculatorFactory.INSTANCE.calculateSignature(clusterPosition);
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
        OLinearHashingBucket bucket = file.get(pageNumberToUse);
        if (bucket != null) {
          return (V) bucket.get(clusterPosition);
        }
        return null;
      }

    }
  }

  public K nextRecord(K currentRecord) {
    return nextRecord(currentRecord, false, +1);
  }

  private K nextRecord(K currentRecord, boolean nextNaturalOrderedKeyShouldBeUsed, int step) {
    final K[] result = getKeySet(currentRecord, nextNaturalOrderedKeyShouldBeUsed, step);

    if (result.length == 0) {
      return null;
    }

    Arrays.sort(result);
    final int recordPosition;

    recordPosition = Arrays.binarySearch(result, currentRecord);

    if (recordPosition >= 0 || recordPosition < -result.length) {
      if (recordPosition == result.length - 1 || recordPosition < -result.length) {
        return nextRecord(currentRecord, true, +1);
      } else {
        return result[recordPosition + 1];
      }
    } else {
      return result[-(recordPosition + 1)];
    }
  }

  public K prevRecord(K currentRecord) {
    return prevRecord(currentRecord, false, -1);
  }

  private K prevRecord(K currentRecord, boolean nextNaturalOrderedKeyShouldBeUsed, int step) {
    final K[] result = getKeySet(currentRecord, nextNaturalOrderedKeyShouldBeUsed, step);

    if (result.length == 0) {
      return null;
    }

    Arrays.sort(result);
    final int recordPosition = Arrays.binarySearch(result, currentRecord);

    if (recordPosition >= 0 || recordPosition == -1) {
      if (recordPosition == 0 || recordPosition == -1) {
        return prevRecord(currentRecord, true, -1);
      } else {
        return result[recordPosition - 1];
      }
    } else {
      return result[-(recordPosition + 2)];
    }
  }

  private K[] getKeySet(K currentRecord, boolean nextNaturalOrderedKeyShouldBeUsed, int step) {
    List<OLinearHashingBucket> chain = new ArrayList<OLinearHashingBucket>();
    boolean nextLevel = false;
    int naturalOrderedKey = OLinearHashingHashCalculatorFactory.INSTANCE.calculateNaturalOrderedHash(currentRecord, level);
    if (naturalOrderedKey < next) {
      naturalOrderedKey = OLinearHashingHashCalculatorFactory.INSTANCE.calculateNaturalOrderedHash(currentRecord, level + 1);
      nextLevel = true;
    }

    if (nextNaturalOrderedKeyShouldBeUsed) {
      naturalOrderedKey += step;
      if (nextLevel && naturalOrderedKey >= 2 * next && step > 0) {
        naturalOrderedKey = naturalOrderedKey / 2;
        nextLevel = false;
      }

      if (!nextLevel && naturalOrderedKey < next && step < 0) {
        naturalOrderedKey = naturalOrderedKey * 2 + 1;
        nextLevel = true;
      }
    }

    int bucketNumber;
    if (nextLevel) {
      bucketNumber = OLinearHashingHashCalculatorFactory.INSTANCE.calculateBucketNumber(naturalOrderedKey, level + 1);
    } else {
      bucketNumber = OLinearHashingHashCalculatorFactory.INSTANCE.calculateBucketNumber(naturalOrderedKey, level);
    }

    int displacement = primaryIndex.getChainDisplacement(bucketNumber);

    // load buckets from overflow positions
    while (displacement < 253) {
      int pageToUse = findNextPageInChain(bucketNumber, nextLevel ? level + 1 : level, displacement);
      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToUse);
      displacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
      OLinearHashingBucket bucket = file.get(pageToUse);
      chain.add(bucket);
    }

    OLinearHashingBucket bucket = file.get(bucketNumber);
    final K[] result;
    if (chain.size() == 0) {
      result = (K[]) new OClusterPosition[bucket.size];
      System.arraycopy(bucket.keys, 0, result, 0, bucket.size);
    } else {
      chain.add(bucket);

      int amountOfRecords = 0;
      for (OLinearHashingBucket chainElement : chain) {
        amountOfRecords += chainElement.size;
      }

      result = (K[]) new OClusterPosition[amountOfRecords];
      int freePositionInArrayPointer = 0;
      for (OLinearHashingBucket chainElement : chain) {
        System.arraycopy(chainElement.keys, 0, result, freePositionInArrayPointer, chainElement.size);
        freePositionInArrayPointer += chainElement.size;
      }
    }
    return result;
  }
}
