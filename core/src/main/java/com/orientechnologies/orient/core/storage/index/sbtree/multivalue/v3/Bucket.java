/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTree;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
final class Bucket<K> extends ODurablePage {
  private static final int EMBEDDED_ITEMS_THRESHOLD        = 64;
  private static final int RID_SIZE                        = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
  private static final int SINGLE_ELEMENT_LINKED_ITEM_SIZE = OIntegerSerializer.INT_SIZE + RID_SIZE + OByteSerializer.BYTE_SIZE;

  private static final int FREE_POINTER_OFFSET  = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET          = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET       = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET  = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int POSITIONS_ARRAY_OFFSET = RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private final boolean isLeaf;

  private final OBinarySerializer<K> keySerializer;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  private final OSBTree<OMultiValueEntry, Byte> multiContainer;

  Bucket(final OCacheEntry cacheEntry, final boolean isLeaf, final OBinarySerializer<K> keySerializer,
      final OSBTree<OMultiValueEntry, Byte> multiContainer) {
    super(cacheEntry);

    this.isLeaf = isLeaf;
    this.keySerializer = keySerializer;
    this.multiContainer = multiContainer;

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);
  }

  Bucket(final OCacheEntry cacheEntry, final OBinarySerializer<K> keySerializer,
      final OSBTree<OMultiValueEntry, Byte> multiContainer) {
    super(cacheEntry);

    this.isLeaf = getByteValue(IS_LEAF_OFFSET) > 0;
    this.keySerializer = keySerializer;
    this.multiContainer = multiContainer;
  }

  boolean isEmpty() {
    return size() == 0;
  }

  int find(final K key) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final K midVal = getKey(mid);
      final int cmp = comparator.compare(midVal, key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1); // key not found.
  }

  int remove(final int entryIndex) throws IOException {
    assert isLeaf;

    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    int position = entryPosition;
    int nextItem = getIntValue(position);
    position += OIntegerSerializer.INT_SIZE;

    final int embeddedEntriesCount = getByteValue(position);
    position += OByteSerializer.BYTE_SIZE;

    final int entriesCount = getIntValue(position);
    position += OIntegerSerializer.INT_SIZE;

    final long mId = getLongValue(position);
    position += OLongSerializer.LONG_SIZE;

    if (entriesCount > embeddedEntriesCount) {
      final List<OMultiValueEntry> entriesToRemove = new ArrayList<>(entriesCount - embeddedEntriesCount);
      final OSBTree.OSBTreeCursor<OMultiValueEntry, Byte> cursor = multiContainer
          .iterateEntriesBetween(new OMultiValueEntry(mId, 0, 0), true, new OMultiValueEntry(mId + 1, 0, 0), false, true);

      Map.Entry<OMultiValueEntry, Byte> mapEntry = cursor.next(-1);
      while (mapEntry != null) {
        final OMultiValueEntry mEntry = mapEntry.getKey();
        entriesToRemove.add(mEntry);

        mapEntry = cursor.next(-1);
      }

      for (final OMultiValueEntry entry : entriesToRemove) {
        multiContainer.remove(entry);
      }
    }

    position += RID_SIZE; //skip the rid

    final int keySize;
    keySize = getObjectSizeInDirectMemory(keySerializer, position);

    if (nextItem == -1) {
      removeMainEntry(entryIndex, entryPosition, keySize);

      return 1;
    }

    final List<Integer> itemsToRemove = new ArrayList<>(8);
    final List<Integer> itemsToRemoveSize = new ArrayList<>(8);

    final int entrySize =
        2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE + keySize + RID_SIZE;
    int totalSpace = entrySize;

    while (nextItem > 0) {
      final int arraySize = 0xFF & getByteValue(nextItem + OIntegerSerializer.INT_SIZE);
      final int itemSize = arraySize * RID_SIZE + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
      totalSpace += itemSize;

      itemsToRemove.add(nextItem);
      itemsToRemoveSize.add(itemSize);

      nextItem = getIntValue(nextItem);
    }

    int size = getIntValue(SIZE_OFFSET);

    final TreeMap<Integer, Integer> entries = new TreeMap<>();
    @SuppressWarnings("SpellCheckingInspection")
    final ConcurrentSkipListMap<Integer, Integer> nexts = new ConcurrentSkipListMap<>();

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      if (i != entryIndex) {
        final int currentEntryPosition = getIntValue(currentPositionOffset);
        final int currentNextPosition = getIntValue(currentEntryPosition);

        entries.put(currentEntryPosition, currentPositionOffset);
        if (currentNextPosition > 0) {
          nexts.put(currentNextPosition, currentEntryPosition);
        }
      }

      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    int freeSpacePointer = getIntValue(FREE_POINTER_OFFSET);

    int clearedSpace = 0;
    int counter = 0;

    for (final int itemToRemove : itemsToRemove) {
      final int itemSize = itemsToRemoveSize.get(counter);

      if (itemToRemove > freeSpacePointer) {
        moveData(freeSpacePointer, freeSpacePointer + itemSize, itemToRemove - freeSpacePointer);
        final int diff = totalSpace - clearedSpace;

        final SortedMap<Integer, Integer> entriesRefToCorrect = entries.headMap(itemToRemove);
        for (final Map.Entry<Integer, Integer> entry : entriesRefToCorrect.entrySet()) {
          final int currentEntryOffset = entry.getValue();
          final int currentEntryPosition = entry.getKey();

          setIntValue(currentEntryOffset, currentEntryPosition + diff);
        }

        entriesRefToCorrect.clear();

        final SortedMap<Integer, Integer> linkRefToCorrect = nexts.headMap(itemToRemove);
        for (final Map.Entry<Integer, Integer> entry : linkRefToCorrect.entrySet()) {
          final int first = entry.getKey();
          final int currentEntryPosition = entry.getValue();

          if (first < itemToRemove) {
            if (currentEntryPosition > 0) {
              final int updatedEntryPosition;

              if (currentEntryPosition < itemToRemove) {
                final int itemsBefore = -Collections.binarySearch(itemsToRemove, currentEntryPosition) - 1;

                if (counter >= itemsBefore) {
                  updatedEntryPosition = currentEntryPosition + itemsToRemoveSize.subList(itemsBefore, counter + 1).stream()
                      .mapToInt(Integer::intValue).sum();
                } else {
                  updatedEntryPosition = currentEntryPosition;
                }
              } else {
                updatedEntryPosition = currentEntryPosition;
              }

              setIntValue(updatedEntryPosition, first + diff);
            } else {
              final int compositeEntryPosition = -currentEntryPosition;
              final int prevCounter = compositeEntryPosition >>> 16;
              final int prevEntryPosition = compositeEntryPosition & 0xFFFF;

              final int updatedEntryPosition =
                  prevEntryPosition + itemsToRemoveSize.subList(prevCounter + 1, counter + 1).stream().mapToInt(Integer::intValue)
                      .sum();//offset of removal of prevCounter item already taken into account

              setIntValue(updatedEntryPosition, first + diff);
            }
          }

          final int[] lastEntry = incrementalUpdateAllLinkedListReferences(first, itemSize, itemToRemove, diff);

          if (lastEntry[1] > 0) {
            nexts.put(lastEntry[1], -((counter << 16) | lastEntry[0]));
          }
        }

        linkRefToCorrect.clear();
      }

      clearedSpace += itemsToRemoveSize.get(counter);
      counter++;

      freeSpacePointer += itemSize;
    }

    if (entryPosition > freeSpacePointer) {
      moveData(freeSpacePointer, freeSpacePointer + entrySize, entryPosition - freeSpacePointer);

      @SuppressWarnings("UnnecessaryLocalVariable")
      final int diff = entrySize;

      final SortedMap<Integer, Integer> entriesRefToCorrect = entries.headMap(entryPosition);
      for (final Map.Entry<Integer, Integer> entry : entriesRefToCorrect.entrySet()) {
        final int currentEntryOffset = entry.getValue();
        final int currentEntryPosition = entry.getKey();

        setIntValue(currentEntryOffset, currentEntryPosition + diff);
      }

      final SortedMap<Integer, Integer> linkRefToCorrect = nexts.headMap(entryPosition);
      for (final Map.Entry<Integer, Integer> entry : linkRefToCorrect.entrySet()) {
        final int first = entry.getKey();
        final int currentEntryPosition = entry.getValue();

        if (currentEntryPosition > 0) {
          int updatedEntryPosition;

          final int itemsBefore = -Collections.binarySearch(itemsToRemove, currentEntryPosition) - 1;
          if (itemsToRemove.size() > itemsBefore) {
            updatedEntryPosition = currentEntryPosition + itemsToRemoveSize.subList(itemsBefore, itemsToRemove.size()).stream()
                .mapToInt(Integer::intValue).sum();
          } else {
            updatedEntryPosition = currentEntryPosition;
          }

          if (currentEntryPosition < entryPosition) {
            updatedEntryPosition += entrySize;
          }

          setIntValue(updatedEntryPosition, first + diff);
        } else {
          final int compositeEntryPosition = -currentEntryPosition;
          final int prevCounter = compositeEntryPosition >>> 16;
          final int prevEntryPosition = compositeEntryPosition & 0xFFFF;

          final int updatedEntryPosition =
              entrySize + itemsToRemoveSize.subList(prevCounter + 1, itemsToRemove.size()).stream().mapToInt(Integer::intValue)
                  .sum() + prevEntryPosition;

          setIntValue(updatedEntryPosition, first + diff);
        }

        updateAllLinkedListReferences(first, entryPosition, diff);
      }
    }

    freeSpacePointer += entrySize;

    setIntValue(FREE_POINTER_OFFSET, freeSpacePointer);

    if (entryIndex < size) {
      moveData(POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    return entriesCount;
  }

  private void removeMainEntry(final int entryIndex, final int entryPosition, final int keySize) {
    int nextItem;
    int size = getIntValue(SIZE_OFFSET);

    if (entryIndex < size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    final int entrySize =
        OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + RID_SIZE + keySize;

    boolean moved = false;
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
      moved = true;
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);

    if (moved) {
      int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

      for (int i = 0; i < size; i++) {
        final int currentEntryPosition = getIntValue(currentPositionOffset);
        final int updatedEntryPosition;

        if (currentEntryPosition < entryPosition) {
          updatedEntryPosition = currentEntryPosition + entrySize;
          setIntValue(currentPositionOffset, updatedEntryPosition);
        } else {
          updatedEntryPosition = currentEntryPosition;
        }

        nextItem = getIntValue(updatedEntryPosition);
        if (nextItem > 0 && nextItem < entryPosition) {
          //update reference to the first item of linked list
          setIntValue(updatedEntryPosition, nextItem + entrySize);

          updateAllLinkedListReferences(nextItem, entryPosition, entrySize);
        }

        currentPositionOffset += OIntegerSerializer.INT_SIZE;
      }

    }
  }

  boolean remove(final int entryIndex, final ORID value) throws IOException {
    assert isLeaf;

    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    int position = entryPosition;
    int nextItem = getIntValue(position);
    position += OIntegerSerializer.INT_SIZE;

    final int embeddedEntriesCountPosition = position;
    final int embeddedEntriesCount = getByteValue(position);
    position += OByteSerializer.BYTE_SIZE;

    final int entriesCountPosition = position;
    final int entriesCount = getIntValue(entriesCountPosition);
    position += OIntegerSerializer.INT_SIZE;

    final long mId = getLongValue(position);
    position += OLongSerializer.LONG_SIZE;

    //only single element in list
    if (nextItem == -1) {
      final int clusterIdPosition = position;
      final int clusterId = getShortValue(clusterIdPosition);
      position += OShortSerializer.SHORT_SIZE;

      final long clusterPosition = getLongValue(position);
      position += OLongSerializer.LONG_SIZE;

      final int keySize = getObjectSizeInDirectMemory(keySerializer, position);

      if (clusterId != value.getClusterId()) {
        if (entriesCount > embeddedEntriesCount) {
          final Byte removed = multiContainer.remove(new OMultiValueEntry(mId, value.getClusterId(), value.getClusterPosition()));

          if (removed != null) {
            if (entriesCount == 1) {
              removeMainEntry(entryIndex, entryPosition, keySize);
            } else {
              setIntValue(entriesCountPosition, entriesCount - 1);
            }

            return true;
          }
        }

        return false;
      }

      if (clusterPosition == value.getClusterPosition()) {
        if (entriesCount > 1) {
          setShortValue(clusterIdPosition, (short) -1);

          assert embeddedEntriesCount == 1;

          setByteValue(embeddedEntriesCountPosition, (byte) (embeddedEntriesCount - 1));
          setIntValue(entriesCountPosition, entriesCount - 1);
        } else {
          removeMainEntry(entryIndex, entryPosition, keySize);
        }

        return true;
      }
    } else {
      int clusterId = getShortValue(position);
      position += OShortSerializer.SHORT_SIZE;

      long clusterPosition = getLongValue(position);
      if (clusterId == value.getClusterId() && clusterPosition == value.getClusterPosition()) {
        final int nextNextItem = getIntValue(nextItem);
        final int nextItemSize = 0xFF & getByteValue(nextItem + OIntegerSerializer.INT_SIZE);

        final byte[] nextValue = getBinaryValue(nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE, RID_SIZE);

        assert nextItemSize > 0;
        final int freePointer = getIntValue(FREE_POINTER_OFFSET);
        if (nextItemSize == 1) {
          setIntValue(entryPosition, nextNextItem);
          setIntValue(FREE_POINTER_OFFSET, freePointer + OIntegerSerializer.INT_SIZE + RID_SIZE + OByteSerializer.BYTE_SIZE);
        } else {
          setByteValue(nextItem + OIntegerSerializer.INT_SIZE, (byte) (nextItemSize - 1));
          setIntValue(FREE_POINTER_OFFSET, freePointer + RID_SIZE);
        }

        setBinaryValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE,
            nextValue);

        if (nextItem > freePointer || nextItemSize > 1) {
          if (nextItemSize == 1) {
            moveData(freePointer, freePointer + SINGLE_ELEMENT_LINKED_ITEM_SIZE, nextItem - freePointer);
          } else {
            moveData(freePointer, freePointer + RID_SIZE,
                nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE - freePointer);
          }

          final int diff = nextItemSize > 1 ? RID_SIZE : OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + RID_SIZE;

          final int size = getIntValue(SIZE_OFFSET);
          int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

          for (int i = 0; i < size; i++) {
            final int currentEntryPosition = getIntValue(currentPositionOffset);
            final int updatedEntryPosition;

            if (currentEntryPosition < nextItem) {
              updatedEntryPosition = currentEntryPosition + diff;
              setIntValue(currentPositionOffset, updatedEntryPosition);
            } else {
              updatedEntryPosition = currentEntryPosition;
            }

            final int currentNextItem = getIntValue(updatedEntryPosition);
            if (currentNextItem > 0 && currentNextItem < nextItem + diff) {
              //update reference to the first item of linked list
              setIntValue(updatedEntryPosition, currentNextItem + diff);

              updateAllLinkedListReferences(currentNextItem, nextItem + diff, diff);
            }

            currentPositionOffset += OIntegerSerializer.INT_SIZE;
          }
        }

        setByteValue(embeddedEntriesCountPosition, (byte) (embeddedEntriesCount - 1));
        setIntValue(entriesCountPosition, entriesCount - 1);

        return true;
      } else {
        int prevItem = entryPosition;

        while (nextItem > 0) {
          final int nextNextItem = getIntValue(nextItem);
          final int nextItemSize = 0xFF & getByteValue(nextItem + OIntegerSerializer.INT_SIZE);

          if (nextItemSize == 1) {
            clusterId = getShortValue(nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE);
            clusterPosition = getLongValue(
                nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + OShortSerializer.SHORT_SIZE);

            if (clusterId == value.getClusterId() && clusterPosition == value.getClusterPosition()) {
              setIntValue(prevItem, nextNextItem);

              final int freePointer = getIntValue(FREE_POINTER_OFFSET);
              setIntValue(FREE_POINTER_OFFSET, freePointer + SINGLE_ELEMENT_LINKED_ITEM_SIZE);

              if (nextItem > freePointer) {
                moveData(freePointer, freePointer + SINGLE_ELEMENT_LINKED_ITEM_SIZE, nextItem - freePointer);

                final int size = getIntValue(SIZE_OFFSET);
                int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

                for (int i = 0; i < size; i++) {
                  final int currentEntryPosition = getIntValue(currentPositionOffset);
                  final int updatedEntryPosition;

                  if (currentEntryPosition < nextItem) {
                    updatedEntryPosition = currentEntryPosition + SINGLE_ELEMENT_LINKED_ITEM_SIZE;
                    setIntValue(currentPositionOffset, updatedEntryPosition);
                  } else {
                    updatedEntryPosition = currentEntryPosition;
                  }

                  final int currentNextItem = getIntValue(updatedEntryPosition);
                  if (currentNextItem > 0 && currentNextItem < nextItem) {
                    //update reference to the first item of linked list
                    setIntValue(updatedEntryPosition, currentNextItem + SINGLE_ELEMENT_LINKED_ITEM_SIZE);

                    updateAllLinkedListReferences(currentNextItem, nextItem, SINGLE_ELEMENT_LINKED_ITEM_SIZE);
                  }

                  currentPositionOffset += OIntegerSerializer.INT_SIZE;
                }
              }

              setByteValue(embeddedEntriesCountPosition, (byte) (embeddedEntriesCount - 1));
              setIntValue(entriesCountPosition, entriesCount - 1);
              return true;
            }
          } else {
            for (int i = 0; i < nextItemSize; i++) {
              clusterId = getShortValue(nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + i * RID_SIZE);
              clusterPosition = getLongValue(
                  nextItem + OIntegerSerializer.INT_SIZE + OShortSerializer.SHORT_SIZE + OByteSerializer.BYTE_SIZE + i * RID_SIZE);

              if (clusterId == value.getClusterId() && clusterPosition == value.getClusterPosition()) {
                final int freePointer = getIntValue(FREE_POINTER_OFFSET);
                setIntValue(FREE_POINTER_OFFSET, freePointer + RID_SIZE);

                setByteValue(nextItem + OIntegerSerializer.INT_SIZE, (byte) (nextItemSize - 1));

                moveData(freePointer, freePointer + RID_SIZE,
                    nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + i * RID_SIZE - freePointer);

                final int size = getIntValue(SIZE_OFFSET);
                int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

                for (int n = 0; n < size; n++) {
                  final int currentEntryPosition = getIntValue(currentPositionOffset);
                  final int updatedEntryPosition;

                  if (currentEntryPosition < nextItem) {
                    updatedEntryPosition = currentEntryPosition + RID_SIZE;
                    setIntValue(currentPositionOffset, updatedEntryPosition);
                  } else {
                    updatedEntryPosition = currentEntryPosition;
                  }

                  final int currentNextItem = getIntValue(updatedEntryPosition);
                  if (currentNextItem > 0 && currentNextItem < nextItem + RID_SIZE) {
                    //update reference to the first item of linked list
                    setIntValue(updatedEntryPosition, currentNextItem + RID_SIZE);

                    updateAllLinkedListReferences(currentNextItem, nextItem + RID_SIZE, RID_SIZE);
                  }

                  currentPositionOffset += OIntegerSerializer.INT_SIZE;
                }

                setByteValue(embeddedEntriesCountPosition, (byte) (embeddedEntriesCount - 1));
                setIntValue(entriesCountPosition, entriesCount - 1);
                return true;
              }
            }
          }

          prevItem = nextItem;
          nextItem = nextNextItem;
        }
      }
    }

    if (mId > 0) {
      final Byte removed = multiContainer.remove(new OMultiValueEntry(mId, value.getClusterId(), value.getClusterPosition()));

      if (removed != null) {
        setIntValue(entriesCountPosition, entriesCount - 1);
        return true;
      }
    }

    return false;
  }

  private void updateAllLinkedListReferences(final int firstItem, final int boundary, final int diffSize) {
    int currentItem = firstItem + diffSize;

    while (true) {
      final int nextItem = getIntValue(currentItem);

      if (nextItem > 0 && nextItem < boundary) {
        setIntValue(currentItem, nextItem + diffSize);
        currentItem = nextItem + diffSize;
      } else {
        return;
      }
    }
  }

  private int[] incrementalUpdateAllLinkedListReferences(final int firstItem, final int itemSize, final int boundary,
      final int diffSize) {
    int currentItem = firstItem + itemSize;

    while (true) {
      final int nextItem = getIntValue(currentItem);

      if (nextItem > 0 && nextItem < boundary) {
        setIntValue(currentItem, nextItem + diffSize);
        currentItem = nextItem + itemSize;
      } else {
        return new int[] { currentItem, nextItem };
      }
    }
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  LeafEntry getLeafEntry(final int entryIndex) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    int nextItem = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final int embeddedEntriesCount = getByteValue(entryPosition);
    entryPosition += OByteSerializer.BYTE_SIZE;

    final int entriesCount = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final long mId = getLongValue(entryPosition);
    entryPosition += OLongSerializer.LONG_SIZE;

    final List<ORID> values = new ArrayList<>(entriesCount);

    int clusterId = getShortValue(entryPosition);
    entryPosition += OShortSerializer.SHORT_SIZE;

    if (clusterId >= 0) {
      final long clusterPosition = getLongValue(entryPosition);
      entryPosition += OLongSerializer.LONG_SIZE;

      values.add(new ORecordId(clusterId, clusterPosition));
    } else {
      entryPosition += OLongSerializer.LONG_SIZE;
    }

    final byte[] key;
    final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
    key = getBinaryValue(entryPosition, keySize);

    while (nextItem > 0) {
      final int nextNextItem = getIntValue(nextItem);
      final int nextItemSize = 0xFF & getByteValue(nextItem + OIntegerSerializer.INT_SIZE);

      for (int i = 0; i < nextItemSize; i++) {
        clusterId = getShortValue(nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + i * RID_SIZE);
        final long clusterPosition = getLongValue(
            nextItem + OShortSerializer.SHORT_SIZE + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + i * RID_SIZE);

        values.add(new ORecordId(clusterId, clusterPosition));
      }

      nextItem = nextNextItem;
    }

    assert values.size() == embeddedEntriesCount;

    if (values.size() < entriesCount) {
      final OSBTree.OSBTreeCursor<OMultiValueEntry, Byte> cursor = multiContainer
          .iterateEntriesBetween(new OMultiValueEntry(mId, 0, 0), true, new OMultiValueEntry(mId + 1, 0, 0), false, true);

      Map.Entry<OMultiValueEntry, Byte> mapEntry = cursor.next(-1);
      while (mapEntry != null) {
        final OMultiValueEntry mEntry = mapEntry.getKey();
        values.add(new ORecordId(mEntry.clusterId, mEntry.clusterPosition));

        mapEntry = cursor.next(-1);
      }
    }

    assert values.size() == entriesCount;

    return new LeafEntry(key, mId, values);
  }

  NonLeafEntry getNonLeafEntry(final int entryIndex) {
    assert !isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    final int leftChild = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final int rightChild = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final byte[] key;

    final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
    key = getBinaryValue(entryPosition, keySize);

    return new NonLeafEntry(key, leftChild, rightChild);
  }

  int getLeft(final int entryIndex) {
    assert !isLeaf;

    final int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition);
  }

  int getRight(final int entryIndex) {
    assert !isLeaf;

    final int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);
  }

  /**
   * Obtains the value stored under the given entry index in this bucket.
   *
   * @param entryIndex the value entry index.
   *
   * @return the obtained value.
   */
  List<ORID> getValues(final int entryIndex) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    int nextItem = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final int embeddedEntriesCount = getByteValue(entryPosition);
    entryPosition += OByteSerializer.BYTE_SIZE;

    final int entriesCount = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final long mId = getLongValue(entryPosition);
    entryPosition += OLongSerializer.LONG_SIZE;

    final List<ORID> results = new ArrayList<>(entriesCount);

    int clusterId = getShortValue(entryPosition);
    entryPosition += OShortSerializer.SHORT_SIZE;

    if (clusterId >= 0) {
      final long clusterPosition = getLongValue(entryPosition);
      results.add(new ORecordId(clusterId, clusterPosition));
    }

    while (nextItem > 0) {
      final int nextNextItem = getIntValue(nextItem);
      final int nextItemSize = 0xFF & getByteValue(nextItem + OIntegerSerializer.INT_SIZE);

      for (int i = 0; i < nextItemSize; i++) {
        clusterId = getShortValue(nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + i * RID_SIZE);
        final long clusterPosition = getLongValue(
            nextItem + OIntegerSerializer.INT_SIZE + OShortSerializer.SHORT_SIZE + OByteSerializer.BYTE_SIZE + i * RID_SIZE);

        results.add(new ORecordId(clusterId, clusterPosition));
      }

      nextItem = nextNextItem;
    }

    assert results.size() == embeddedEntriesCount;

    if (results.size() < entriesCount) {
      final OSBTree.OSBTreeCursor<OMultiValueEntry, Byte> cursor = multiContainer
          .iterateEntriesBetween(new OMultiValueEntry(mId, 0, 0), true, new OMultiValueEntry(mId + 1, 0, 0), false, true);
      Map.Entry<OMultiValueEntry, Byte> mapEntry = cursor.next(-1);
      while (mapEntry != null) {
        final OMultiValueEntry mEntry = mapEntry.getKey();
        results.add(new ORecordId(mEntry.clusterId, mEntry.clusterPosition));

        mapEntry = cursor.next(-1);
      }
    }

    assert results.size() == entriesCount;

    return results;
  }

  public K getKey(final int index) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    } else {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE + RID_SIZE;
    }

    return deserializeFromDirectMemory(keySerializer, entryPosition);
  }

  byte[] getRawKey(final int index) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    } else {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE + RID_SIZE;
    }

    final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
    return getBinaryValue(entryPosition, keySize);
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void addAll(final List<Entry> entries) throws IOException {
    if (!isLeaf) {
      for (int i = 0; i < entries.size(); i++) {
        final NonLeafEntry entry = (NonLeafEntry) entries.get(i);
        addNonLeafEntry(i, entry.key, entry.leftChild, entry.rightChild, false);
      }
    } else {
      for (int i = 0; i < entries.size(); i++) {
        final LeafEntry entry = (LeafEntry) entries.get(i);
        final byte[] key = entry.key;
        final List<ORID> values = entry.values;

        addNewLeafEntry(i, key, values.get(0), entry.mId);
        if (values.size() > 1) {
          appendNewLeafEntries(i, values.subList(1, values.size()));
        }
      }
    }

    setIntValue(SIZE_OFFSET, entries.size());
  }

  public void shrink(final int newSize) throws IOException {
    if (isLeaf) {
      final List<LeafEntry> entries = new ArrayList<>(newSize);

      for (int i = 0; i < newSize; i++) {
        entries.add(getLeafEntry(i));
      }

      setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);

      int index = 0;
      for (final LeafEntry entry : entries) {
        final byte[] key = entry.key;
        final List<ORID> values = entry.values;

        addNewLeafEntry(index, key, values.get(0), entry.mId);
        if (values.size() > 1) {
          appendNewLeafEntries(index, values.subList(1, values.size()));
        }

        index++;
      }

      setIntValue(SIZE_OFFSET, newSize);
    } else {
      final List<NonLeafEntry> entries = new ArrayList<>(newSize);

      for (int i = 0; i < newSize; i++) {
        entries.add(getNonLeafEntry(i));
      }

      setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);

      int index = 0;
      for (final NonLeafEntry entry : entries) {
        addNonLeafEntry(index, entry.key, entry.leftChild, entry.rightChild, false);
        index++;
      }

      setIntValue(SIZE_OFFSET, newSize);
    }
  }

  boolean addNewLeafEntry(final int index, final byte[] serializedKey, final ORID value, final long mId) {
    assert isLeaf;

    final int entrySize =
        OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE + RID_SIZE
            + serializedKey.length; //next item pointer + embedded entries count- entries count + mid + rid + key

    final int size = getIntValue(SIZE_OFFSET);

    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setIntValue(freePointer, -1); //next item pointer
    freePointer += setByteValue(freePointer, (byte) 1); //embedded entries count
    freePointer += setIntValue(freePointer, 1); //entries count
    freePointer += setLongValue(freePointer, mId);//mId, minus means that there are no external entries
    freePointer += setShortValue(freePointer, (short) value.getClusterId());//rid
    freePointer += setLongValue(freePointer, value.getClusterPosition());
    setBinaryValue(freePointer, serializedKey);//key

    return true;
  }

  boolean appendNewLeafEntry(final int index, final ORID value) throws IOException {
    assert isLeaf;

    final int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int nextItem = getIntValue(entryPosition);
    final int embeddedEntriesCount = getByteValue(entryPosition + OIntegerSerializer.INT_SIZE);
    final int entriesCount = getIntValue(entryPosition + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE);

    if (embeddedEntriesCount < EMBEDDED_ITEMS_THRESHOLD) {
      final int itemSize = OIntegerSerializer.INT_SIZE + RID_SIZE + OByteSerializer.BYTE_SIZE;//next item pointer + RID + size
      int freePointer = getIntValue(FREE_POINTER_OFFSET);

      final int size = getIntValue(SIZE_OFFSET);

      if (freePointer - itemSize < size * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
        return false;
      }

      freePointer -= itemSize;
      setIntValue(entryPosition, freePointer);//update list header

      freePointer += setIntValue(freePointer, nextItem);//next item pointer
      freePointer += setByteValue(freePointer, (byte) 1);//size
      freePointer += setShortValue(freePointer, (short) value.getClusterId());//rid
      setLongValue(freePointer, value.getClusterPosition());

      freePointer -= itemSize;
      setIntValue(FREE_POINTER_OFFSET, freePointer);

      setByteValue(entryPosition + OIntegerSerializer.INT_SIZE, (byte) (embeddedEntriesCount + 1));
    } else {
      final long mid = getLongValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE);

      multiContainer.put(new OMultiValueEntry(mid, value.getClusterId(), value.getClusterPosition()), (byte) 1);
    }

    setIntValue(entryPosition + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE, entriesCount + 1);

    return true;
  }

  private void appendNewLeafEntries(final int index, final List<ORID> values) throws IOException {
    assert isLeaf;

    final int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int embeddedEntriesCount = getByteValue(entryPosition + OIntegerSerializer.INT_SIZE);
    final int entriesCount = getIntValue(entryPosition + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE);

    int listSize = 0;
    if (embeddedEntriesCount < EMBEDDED_ITEMS_THRESHOLD) {
      listSize = Math.min(values.size(), EMBEDDED_ITEMS_THRESHOLD - entriesCount);

      final int itemSize =
          OIntegerSerializer.INT_SIZE + RID_SIZE * listSize + OByteSerializer.BYTE_SIZE;//next item pointer + RIDs + size

      int freePointer = getIntValue(FREE_POINTER_OFFSET);

      freePointer -= itemSize;
      setIntValue(FREE_POINTER_OFFSET, freePointer);

      final int nextItem = getIntValue(entryPosition);

      setIntValue(entryPosition, freePointer);//update list header

      freePointer += setIntValue(freePointer, nextItem);//next item pointer
      freePointer += setByteValue(freePointer, (byte) listSize);

      for (int i = 0; i < listSize; i++) {
        final ORID rid = values.get(i);

        freePointer += setShortValue(freePointer, (short) rid.getClusterId());
        freePointer += setLongValue(freePointer, rid.getClusterPosition());
      }

      setByteValue(entryPosition + OIntegerSerializer.INT_SIZE, (byte) (embeddedEntriesCount + listSize));
    }

    if (values.size() > listSize) {
      final long mId = getLongValue(entryPosition + OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE);

      for (int i = listSize; i < values.size(); i++) {
        final ORID rid = values.get(i);

        multiContainer.put(new OMultiValueEntry(mId, rid.getClusterId(), rid.getClusterPosition()), (byte) 1);
      }
    }

    setIntValue(entryPosition + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE, entriesCount + values.size());
  }

  boolean addNonLeafEntry(final int index, final byte[] serializedKey, final int leftChild, final int rightChild,
      final boolean updateNeighbors) {
    assert !isLeaf;

    final int entrySize = serializedKey.length + 2 * OIntegerSerializer.INT_SIZE;

    int size = size();
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setIntValue(freePointer, leftChild);
    freePointer += setIntValue(freePointer, rightChild);

    setBinaryValue(freePointer, serializedKey);

    size++;

    if (updateNeighbors && size > 1) {
      if (index < size - 1) {
        final int nextEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(nextEntryPosition, rightChild);
      }

      if (index > 0) {
        final int prevEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, leftChild);
      }
    }

    return true;
  }

  void setLeftSibling(final long pageIndex) {
    setLongValue(LEFT_SIBLING_OFFSET, pageIndex);
  }

  public long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  void setRightSibling(final long pageIndex) {
    setLongValue(RIGHT_SIBLING_OFFSET, pageIndex);
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  static class Entry {
    final byte[] key;

    Entry(final byte[] key) {
      this.key = key;
    }
  }

  static final class LeafEntry extends Entry {
    final long       mId;
    final List<ORID> values;

    LeafEntry(final byte[] key, final long mId, final List<ORID> values) {
      super(key);
      this.mId = mId;
      this.values = values;
    }
  }

  static final class NonLeafEntry extends Entry {
    final int leftChild;
    final int rightChild;

    NonLeafEntry(final byte[] key, final int leftChild, final int rightChild) {
      super(key);

      this.leftChild = leftChild;
      this.rightChild = rightChild;
    }
  }
}
