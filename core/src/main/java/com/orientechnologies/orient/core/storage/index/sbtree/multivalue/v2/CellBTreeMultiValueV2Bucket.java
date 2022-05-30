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

package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public final class CellBTreeMultiValueV2Bucket<K> extends ODurablePage {
  private static final int NEXT_ITEM_POINTER_OFFSET = 0;
  private static final int EMBEDDED_ENTRIES_COUNT_OFFSET =
      NEXT_ITEM_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int ENTRIES_COUNT_OFFSET =
      EMBEDDED_ENTRIES_COUNT_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int M_ID_OFFSET = ENTRIES_COUNT_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int CLUSTER_ID_OFFSET = M_ID_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int CLUSTER_POSITION_OFFSET =
      CLUSTER_ID_OFFSET + OShortSerializer.SHORT_SIZE;
  private static final int KEY_OFFSET = CLUSTER_POSITION_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int EMBEDDED_ITEMS_THRESHOLD = 64;
  private static final int RID_SIZE = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
  private static final int SINGLE_ELEMENT_LINKED_ITEM_SIZE =
      OIntegerSerializer.INT_SIZE + RID_SIZE + OByteSerializer.BYTE_SIZE;

  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int POSITIONS_ARRAY_OFFSET =
      RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;
  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  public CellBTreeMultiValueV2Bucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init(boolean isLeaf) {
    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);
  }

  public void switchBucketType() {
    if (!isEmpty()) {
      throw new IllegalStateException(
          "Type of bucket can be changed only bucket if bucket is empty");
    }

    final boolean isLeaf = isLeaf();
    if (isLeaf) {
      setByteValue(IS_LEAF_OFFSET, (byte) 0);
    } else {
      setByteValue(IS_LEAF_OFFSET, (byte) 1);
    }
  }

  boolean isEmpty() {
    return size() == 0;
  }

  int find(final K key, final OBinarySerializer<K> keySerializer, final OEncryption encryption) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final K midVal = getKey(mid, keySerializer, encryption);
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

  private void removeMainLeafEntry(
      final int entryIndex, final int entryPosition, final int keySize) {
    int nextItem;
    int size = getIntValue(SIZE_OFFSET);

    if (entryIndex < size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE,
          (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    final int entrySize =
        OLongSerializer.LONG_SIZE
            + 2 * OIntegerSerializer.INT_SIZE
            + OByteSerializer.BYTE_SIZE
            + RID_SIZE
            + keySize;

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
          // update reference to the first item of linked list
          setIntValue(updatedEntryPosition, nextItem + entrySize);

          updateAllLinkedListReferences(nextItem, entryPosition, entrySize);
        }

        currentPositionOffset += OIntegerSerializer.INT_SIZE;
      }
    }
  }

  public int removeLeafEntry(final int entryIndex, final ORID value) {
    assert isLeaf();

    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    int position = entryPosition;
    int nextItem = getIntValue(position);
    position += OIntegerSerializer.INT_SIZE;

    final int embeddedEntriesCountPosition = position;
    final int embeddedEntriesCount = getByteValue(position);
    position += OByteSerializer.BYTE_SIZE;

    final int entriesCountPosition = position;
    final int entriesCount = getIntValue(entriesCountPosition);
    position += OIntegerSerializer.INT_SIZE;
    position += OLongSerializer.LONG_SIZE; // mId

    // only single element in list
    if (nextItem == -1) {
      final int clusterIdPosition = position;
      final int clusterId = getShortValue(clusterIdPosition);
      position += OShortSerializer.SHORT_SIZE;

      final long clusterPosition = getLongValue(position);

      if (clusterId != value.getClusterId()) {
        return -1;
      }

      if (clusterPosition == value.getClusterPosition()) {
        setShortValue(clusterIdPosition, (short) -1);

        assert embeddedEntriesCount == 1;

        setByteValue(embeddedEntriesCountPosition, (byte) (embeddedEntriesCount - 1));
        setIntValue(entriesCountPosition, entriesCount - 1);

        return entriesCount - 1;
      }
    } else {
      int clusterId = getShortValue(position);
      position += OShortSerializer.SHORT_SIZE;

      long clusterPosition = getLongValue(position);
      if (clusterId == value.getClusterId() && clusterPosition == value.getClusterPosition()) {
        final int nextNextItem = getIntValue(nextItem);
        final int nextItemSize = 0xFF & getByteValue(nextItem + OIntegerSerializer.INT_SIZE);

        final byte[] nextValue =
            getBinaryValue(
                nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE, RID_SIZE);

        assert nextItemSize > 0;
        final int freePointer = getIntValue(FREE_POINTER_OFFSET);
        if (nextItemSize == 1) {
          setIntValue(entryPosition, nextNextItem);
          setIntValue(
              FREE_POINTER_OFFSET,
              freePointer + OIntegerSerializer.INT_SIZE + RID_SIZE + OByteSerializer.BYTE_SIZE);
        } else {
          setByteValue(nextItem + OIntegerSerializer.INT_SIZE, (byte) (nextItemSize - 1));
          setIntValue(FREE_POINTER_OFFSET, freePointer + RID_SIZE);
        }

        setBinaryValue(
            entryPosition
                + 2 * OIntegerSerializer.INT_SIZE
                + OByteSerializer.BYTE_SIZE
                + OLongSerializer.LONG_SIZE,
            nextValue);

        if (nextItem > freePointer || nextItemSize > 1) {
          if (nextItemSize == 1) {
            moveData(
                freePointer, freePointer + SINGLE_ELEMENT_LINKED_ITEM_SIZE, nextItem - freePointer);
          } else {
            moveData(
                freePointer,
                freePointer + RID_SIZE,
                nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE - freePointer);
          }

          final int diff =
              nextItemSize > 1
                  ? RID_SIZE
                  : OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + RID_SIZE;

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
              // update reference to the first item of linked list
              setIntValue(updatedEntryPosition, currentNextItem + diff);

              updateAllLinkedListReferences(currentNextItem, nextItem + diff, diff);
            }

            currentPositionOffset += OIntegerSerializer.INT_SIZE;
          }
        }

        setByteValue(embeddedEntriesCountPosition, (byte) (embeddedEntriesCount - 1));
        setIntValue(entriesCountPosition, entriesCount - 1);

        return entriesCount - 1;
      } else {
        int prevItem = entryPosition;

        while (nextItem > 0) {
          final int nextNextItem = getIntValue(nextItem);
          final int nextItemSize = 0xFF & getByteValue(nextItem + OIntegerSerializer.INT_SIZE);

          if (nextItemSize == 1) {
            clusterId =
                getShortValue(nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE);
            clusterPosition =
                getLongValue(
                    nextItem
                        + OIntegerSerializer.INT_SIZE
                        + OByteSerializer.BYTE_SIZE
                        + OShortSerializer.SHORT_SIZE);

            if (clusterId == value.getClusterId()
                && clusterPosition == value.getClusterPosition()) {
              setIntValue(prevItem, nextNextItem);

              final int freePointer = getIntValue(FREE_POINTER_OFFSET);
              setIntValue(FREE_POINTER_OFFSET, freePointer + SINGLE_ELEMENT_LINKED_ITEM_SIZE);

              if (nextItem > freePointer) {
                moveData(
                    freePointer,
                    freePointer + SINGLE_ELEMENT_LINKED_ITEM_SIZE,
                    nextItem - freePointer);

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
                    // update reference to the first item of linked list
                    setIntValue(
                        updatedEntryPosition, currentNextItem + SINGLE_ELEMENT_LINKED_ITEM_SIZE);

                    updateAllLinkedListReferences(
                        currentNextItem, nextItem, SINGLE_ELEMENT_LINKED_ITEM_SIZE);
                  }

                  currentPositionOffset += OIntegerSerializer.INT_SIZE;
                }
              }

              setByteValue(embeddedEntriesCountPosition, (byte) (embeddedEntriesCount - 1));
              setIntValue(entriesCountPosition, entriesCount - 1);

              return entriesCount - 1;
            }
          } else {
            for (int i = 0; i < nextItemSize; i++) {
              clusterId =
                  getShortValue(
                      nextItem
                          + OIntegerSerializer.INT_SIZE
                          + OByteSerializer.BYTE_SIZE
                          + i * RID_SIZE);
              clusterPosition =
                  getLongValue(
                      nextItem
                          + OIntegerSerializer.INT_SIZE
                          + OShortSerializer.SHORT_SIZE
                          + OByteSerializer.BYTE_SIZE
                          + i * RID_SIZE);

              if (clusterId == value.getClusterId()
                  && clusterPosition == value.getClusterPosition()) {
                final int freePointer = getIntValue(FREE_POINTER_OFFSET);
                setIntValue(FREE_POINTER_OFFSET, freePointer + RID_SIZE);

                setByteValue(nextItem + OIntegerSerializer.INT_SIZE, (byte) (nextItemSize - 1));

                moveData(
                    freePointer,
                    freePointer + RID_SIZE,
                    nextItem
                        + OIntegerSerializer.INT_SIZE
                        + OByteSerializer.BYTE_SIZE
                        + i * RID_SIZE
                        - freePointer);

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
                    // update reference to the first item of linked list
                    setIntValue(updatedEntryPosition, currentNextItem + RID_SIZE);

                    updateAllLinkedListReferences(currentNextItem, nextItem + RID_SIZE, RID_SIZE);
                  }

                  currentPositionOffset += OIntegerSerializer.INT_SIZE;
                }

                setByteValue(embeddedEntriesCountPosition, (byte) (embeddedEntriesCount - 1));
                setIntValue(entriesCountPosition, entriesCount - 1);

                return entriesCount - 1;
              }
            }
          }

          prevItem = nextItem;
          nextItem = nextNextItem;
        }
      }
    }

    return -1;
  }

  boolean hasExternalEntries(final int entryIndex) {
    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int embeddedEntriesCount = getByteValue(entryPosition + OIntegerSerializer.INT_SIZE);
    final int entriesCount =
        getIntValue(entryPosition + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE);

    assert entriesCount >= embeddedEntriesCount;
    return entriesCount > embeddedEntriesCount;
  }

  long getMid(final int entryIndex) {
    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    return getLongValue(
        entryPosition + 2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE);
  }

  public boolean decrementEntriesCount(final int entryIndex) {
    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int entriesCount =
        getIntValue(entryPosition + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE);

    setIntValue(
        entryPosition + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE, entriesCount - 1);

    return entriesCount == 1;
  }

  public void removeMainLeafEntry(final int entryIndex, final int keySize) {
    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    removeMainLeafEntry(entryIndex, entryPosition, keySize);
  }

  public void incrementEntriesCount(final int entryIndex) {
    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int entriesCount =
        getIntValue(entryPosition + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE);
    setIntValue(
        entryPosition + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE, entriesCount + 1);
  }

  private void updateAllLinkedListReferences(
      final int firstItem, final int boundary, final int diffSize) {
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

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public LeafEntry getLeafEntry(
      final int entryIndex, final OBinarySerializer<K> keySerializer, final boolean isEncrypted) {
    assert isLeaf();

    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

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
    if (!isEncrypted) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      key = getBinaryValue(entryPosition, keySize);
    } else {
      final int encryptionSize = getIntValue(entryPosition);
      key = getBinaryValue(entryPosition, encryptionSize + OIntegerSerializer.INT_SIZE);
    }

    while (nextItem > 0) {
      final int nextNextItem = getIntValue(nextItem);
      final int nextItemSize = 0xFF & getByteValue(nextItem + OIntegerSerializer.INT_SIZE);

      for (int i = 0; i < nextItemSize; i++) {
        clusterId =
            getShortValue(
                nextItem + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + i * RID_SIZE);
        final long clusterPosition =
            getLongValue(
                nextItem
                    + OShortSerializer.SHORT_SIZE
                    + OIntegerSerializer.INT_SIZE
                    + OByteSerializer.BYTE_SIZE
                    + i * RID_SIZE);

        values.add(new ORecordId(clusterId, clusterPosition));
      }

      nextItem = nextNextItem;
    }

    assert values.size() == embeddedEntriesCount;

    return new LeafEntry(key, mId, values, entriesCount);
  }

  public NonLeafEntry getNonLeafEntry(
      final int entryIndex, final OBinarySerializer<K> keySerializer, final boolean isEncrypted) {
    assert !isLeaf();

    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    final int leftChild = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final int rightChild = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    final byte[] key;

    if (!isEncrypted) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      key = getBinaryValue(entryPosition, keySize);
    } else {
      final int encryptionSize = getIntValue(entryPosition);
      key = getBinaryValue(entryPosition, encryptionSize + OIntegerSerializer.INT_SIZE);
    }

    return new NonLeafEntry(key, leftChild, rightChild);
  }

  int getLeft(final int entryIndex) {
    assert !isLeaf();

    final int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition);
  }

  int getRight(final int entryIndex) {
    assert !isLeaf();

    final int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);
  }

  public K getKey(
      final int index, final OBinarySerializer<K> keySerializer, final OEncryption encryption) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    } else {
      entryPosition +=
          2 * OIntegerSerializer.INT_SIZE
              + OByteSerializer.BYTE_SIZE
              + OLongSerializer.LONG_SIZE
              + RID_SIZE;
    }

    if (encryption == null) {
      return deserializeFromDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final byte[] encryptedKey = getBinaryValue(entryPosition, encryptedSize);
      final byte[] serializedKey = encryption.decrypt(encryptedKey);
      return keySerializer.deserializeNativeObject(serializedKey, 0);
    }
  }

  byte[] getRawKey(
      final int index, final OBinarySerializer<K> keySerializer, final OEncryption encryption) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    } else {
      entryPosition +=
          2 * OIntegerSerializer.INT_SIZE
              + OByteSerializer.BYTE_SIZE
              + OLongSerializer.LONG_SIZE
              + RID_SIZE;
    }

    if (encryption == null) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      return getBinaryValue(entryPosition, keySize);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      return getBinaryValue(entryPosition, encryptedSize + OIntegerSerializer.INT_SIZE);
    }
  }

  public boolean isLeaf() {
    return getByteValue(IS_LEAF_OFFSET) > 0;
  }

  public void addAll(
      final List<? extends Entry> entries,
      final OBinarySerializer<K> keySerializer,
      final boolean isEncrypted) {
    final int currentSize = size();

    final boolean isLeaf = isLeaf();
    if (!isLeaf) {
      for (int i = 0; i < entries.size(); i++) {
        final NonLeafEntry entry = (NonLeafEntry) entries.get(i);
        doAddNonLeafEntry(i + currentSize, entry.key, entry.leftChild, entry.rightChild, false);
      }
    } else {
      for (int i = 0; i < entries.size(); i++) {
        final LeafEntry entry = (LeafEntry) entries.get(i);
        final byte[] key = entry.key;
        final List<ORID> values = entry.values;

        if (!values.isEmpty()) {
          doCreateMainLeafEntry(i + currentSize, key, values.get(0), entry.mId);
        } else {
          doCreateMainLeafEntry(i + currentSize, key, null, entry.mId);
        }

        if (values.size() > 1) {
          appendNewLeafEntries(
              i + currentSize, values.subList(1, values.size()), entry.entriesCount);
        }
      }
    }

    setIntValue(SIZE_OFFSET, currentSize + entries.size());
  }

  public void shrink(
      final int newSize, final OBinarySerializer<K> keySerializer, final boolean isEncrypted) {
    final boolean isLeaf = isLeaf();
    final int currentSize = size();
    if (isLeaf) {
      final List<LeafEntry> entriesToAdd = new ArrayList<>(newSize);

      for (int i = 0; i < newSize; i++) {
        entriesToAdd.add(getLeafEntry(i, keySerializer, isEncrypted));
      }

      final List<LeafEntry> entriesToRemove = new ArrayList<>(currentSize - newSize);
      for (int i = newSize; i < currentSize; i++) {
        entriesToRemove.add(getLeafEntry(i, keySerializer, isEncrypted));
      }

      setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
      setIntValue(SIZE_OFFSET, 0);

      int index = 0;
      for (final LeafEntry entry : entriesToAdd) {
        final byte[] key = entry.key;
        final List<ORID> values = entry.values;

        if (!values.isEmpty()) {
          doCreateMainLeafEntry(index, key, values.get(0), entry.mId);
        } else {
          doCreateMainLeafEntry(index, key, null, entry.mId);
        }

        if (values.size() > 1) {
          appendNewLeafEntries(index, values.subList(1, values.size()), entry.entriesCount);
        }

        index++;
      }

    } else {
      final List<NonLeafEntry> entries = new ArrayList<>(newSize);

      for (int i = 0; i < newSize; i++) {
        entries.add(getNonLeafEntry(i, keySerializer, isEncrypted));
      }

      final List<NonLeafEntry> entriesToRemove = new ArrayList<>(currentSize - newSize);
      for (int i = newSize; i < currentSize; i++) {
        entriesToRemove.add(getNonLeafEntry(i, keySerializer, isEncrypted));
      }

      setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
      setIntValue(SIZE_OFFSET, 0);

      int index = 0;
      for (final NonLeafEntry entry : entries) {
        doAddNonLeafEntry(index, entry.key, entry.leftChild, entry.rightChild, false);
        index++;
      }
    }
  }

  public boolean createMainLeafEntry(
      final int index, final byte[] serializedKey, final ORID value, final long mId) {
    if (doCreateMainLeafEntry(index, serializedKey, value, mId)) {
      return false;
    }
    return true;
  }

  private boolean doCreateMainLeafEntry(int index, byte[] serializedKey, ORID value, long mId) {
    assert isLeaf();

    final int entrySize =
        OIntegerSerializer.INT_SIZE
            + OByteSerializer.BYTE_SIZE
            + OIntegerSerializer.INT_SIZE
            + OLongSerializer.LONG_SIZE
            + RID_SIZE
            + serializedKey
                .length; // next item pointer + embedded entries count- entries count + mid + rid +
    // key

    final int size = getIntValue(SIZE_OFFSET);

    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize
        < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return true;
    }

    if (index <= size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE,
          (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setIntValue(freePointer, -1); // next item pointer

    if (value != null) {
      freePointer += setByteValue(freePointer, (byte) 1); // embedded entries count
      freePointer += setIntValue(freePointer, 1); // entries count
    } else {
      freePointer += setByteValue(freePointer, (byte) 0); // embedded entries count
      freePointer += setIntValue(freePointer, 0); // entries count
    }

    freePointer += setLongValue(freePointer, mId); // mId

    if (value != null) {
      freePointer += setShortValue(freePointer, (short) value.getClusterId()); // rid
      freePointer += setLongValue(freePointer, value.getClusterPosition());
    } else {
      freePointer += setShortValue(freePointer, (short) -1); // rid
      freePointer += setLongValue(freePointer, -1);
    }

    setBinaryValue(freePointer, serializedKey); // key
    return false;
  }

  public long appendNewLeafEntry(final int index, final ORID value) {
    assert isLeaf();

    final int entryPosition =
        getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int nextItem = getIntValue(entryPosition);
    final int embeddedEntriesCount = getByteValue(entryPosition + OIntegerSerializer.INT_SIZE);
    final int entriesCount =
        getIntValue(entryPosition + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE);
    final long mId =
        getLongValue(entryPosition + OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE);

    if (embeddedEntriesCount < EMBEDDED_ITEMS_THRESHOLD) {
      if (embeddedEntriesCount > 0) {
        final int itemSize =
            OIntegerSerializer.INT_SIZE
                + RID_SIZE
                + OByteSerializer.BYTE_SIZE; // next item pointer + RID + size
        int freePointer = getIntValue(FREE_POINTER_OFFSET);

        final int size = getIntValue(SIZE_OFFSET);

        if (freePointer - itemSize < size * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
          return -2;
        }

        freePointer -= itemSize;
        setIntValue(entryPosition, freePointer); // update list header

        freePointer += setIntValue(freePointer, nextItem); // next item pointer
        freePointer += setByteValue(freePointer, (byte) 1); // size
        freePointer += setShortValue(freePointer, (short) value.getClusterId()); // rid
        freePointer += setLongValue(freePointer, value.getClusterPosition());

        freePointer -= itemSize;
        setIntValue(FREE_POINTER_OFFSET, freePointer);
      } else {
        setShortValue(entryPosition + CLUSTER_ID_OFFSET, (short) value.getClusterId());
        setLongValue(entryPosition + CLUSTER_POSITION_OFFSET, value.getClusterPosition());
      }

      setByteValue(entryPosition + OIntegerSerializer.INT_SIZE, (byte) (embeddedEntriesCount + 1));
    } else {
      return mId;
    }

    setIntValue(
        entryPosition + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE, entriesCount + 1);

    return -1;
  }

  private void appendNewLeafEntries(
      final int index, final List<ORID> values, final int entriesCount) {
    assert isLeaf();

    final int entryPosition =
        getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int embeddedEntriesCount = getByteValue(entryPosition + OIntegerSerializer.INT_SIZE);

    if (values.size() > EMBEDDED_ITEMS_THRESHOLD - embeddedEntriesCount) {
      throw new IllegalStateException(
          "Can not insert "
              + values.size()
              + " embedded entries, limit is "
              + (EMBEDDED_ITEMS_THRESHOLD - embeddedEntriesCount));
    }

    int startIndex = 0;
    if (embeddedEntriesCount == 0) {
      final ORID rid = values.get(0);

      setShortValue(
          entryPosition + 2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE,
          (short) rid.getClusterId()); // rid
      setLongValue(
          entryPosition
              + 2 * OIntegerSerializer.INT_SIZE
              + OByteSerializer.BYTE_SIZE
              + OShortSerializer.SHORT_SIZE,
          rid.getClusterPosition());
      startIndex = 1;
    }

    if (values.size() > startIndex) {
      final int itemSize =
          OIntegerSerializer.INT_SIZE
              + RID_SIZE * values.size()
              + OByteSerializer.BYTE_SIZE; // next item pointer + RIDs + size

      int freePointer = getIntValue(FREE_POINTER_OFFSET);

      freePointer -= itemSize;
      setIntValue(FREE_POINTER_OFFSET, freePointer);

      final int nextItem = getIntValue(entryPosition);

      setIntValue(entryPosition, freePointer); // update list header

      freePointer += setIntValue(freePointer, nextItem); // next item pointer
      freePointer += setByteValue(freePointer, (byte) values.size());

      for (int i = startIndex; i < values.size(); i++) {
        final ORID rid = values.get(i);

        freePointer += setShortValue(freePointer, (short) rid.getClusterId());
        freePointer += setLongValue(freePointer, rid.getClusterPosition());
      }
    }

    setByteValue(
        entryPosition + OIntegerSerializer.INT_SIZE, (byte) (embeddedEntriesCount + values.size()));
    setIntValue(
        entryPosition + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE, entriesCount);
  }

  public boolean addNonLeafEntry(
      final int index,
      final byte[] serializedKey,
      final int leftChild,
      final int rightChild,
      final boolean updateNeighbors) {
    final int prevChild =
        doAddNonLeafEntry(index, serializedKey, leftChild, rightChild, updateNeighbors);
    if (prevChild >= -1) {
      return true;
    }
    return false;
  }

  private int doAddNonLeafEntry(
      int index, byte[] serializedKey, int leftChild, int rightChild, boolean updateNeighbors) {
    assert !isLeaf();

    final int entrySize = serializedKey.length + 2 * OIntegerSerializer.INT_SIZE;

    int size = size();
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize
        < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return -2;
    }

    if (index <= size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE,
          (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setIntValue(freePointer, leftChild);
    freePointer += setIntValue(freePointer, rightChild);

    setBinaryValue(freePointer, serializedKey);

    size++;

    int prevChild = -1;
    if (updateNeighbors && size > 1) {
      if (index < size - 1) {
        final int nextEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
        prevChild = getIntValue(nextEntryPosition);
        setIntValue(nextEntryPosition, rightChild);
      }

      if (index > 0) {
        final int prevEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
        prevChild = getIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, leftChild);
      }
    }

    return prevChild;
  }

  public void removeNonLeafEntry(final int entryIndex, final byte[] key, final int prevChild) {
    if (isLeaf()) {
      throw new IllegalStateException("Remove is applied to non-leaf buckets only");
    }

    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int entrySize = key.length + 2 * OIntegerSerializer.INT_SIZE;
    int size = getIntValue(SIZE_OFFSET);

    final int leftChild = getIntValue(entryPosition);
    final int rightChild = getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);

    if (entryIndex < size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE,
          (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    if (prevChild >= 0) {
      if (entryIndex > 0) {
        final int prevEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (entryIndex - 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, prevChild);
      }

      if (entryIndex < size) {
        final int nextEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
        setIntValue(nextEntryPosition, prevChild);
      }
    }
  }

  public void setLeftSibling(final long pageIndex) {
    setLongValue(LEFT_SIBLING_OFFSET, pageIndex);
  }

  public long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  public void setRightSibling(final long pageIndex) {
    setLongValue(RIGHT_SIBLING_OFFSET, pageIndex);
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  protected static class Entry {
    public final byte[] key;

    Entry(final byte[] key) {
      this.key = key;
    }
  }

  public static final class LeafEntry extends Entry {
    public final long mId;
    public final List<ORID> values;
    public final int entriesCount;

    public LeafEntry(final byte[] key, final long mId, final List<ORID> values, int entriesCount) {
      super(key);
      this.mId = mId;
      this.values = values;
      this.entriesCount = entriesCount;
    }
  }

  public static final class NonLeafEntry extends Entry {
    public final int leftChild;
    public final int rightChild;

    public NonLeafEntry(final byte[] key, final int leftChild, final int rightChild) {
      super(key);

      this.leftChild = leftChild;
      this.rightChild = rightChild;
    }
  }
}
