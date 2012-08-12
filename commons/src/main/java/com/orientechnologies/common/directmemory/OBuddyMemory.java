/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.common.directmemory;

import java.util.Arrays;

import com.orientechnologies.common.types.OBinaryConverter;
import com.orientechnologies.common.types.OBinaryConverterFactory;
import com.orientechnologies.common.types.OBinarySerializer;

/**
 * Buddy memory allocation algorithm.
 * 
 * @author Artem Orobets, Andrey Lomakin
 * @since 12.08.12
 */
public class OBuddyMemory implements ODirectMemory {
  private static final OBinaryConverter CONVERTER        = OBinaryConverterFactory.getConverter();

  public static final int               SYSTEM_INFO_SIZE = 2;

  private static final int              TAG_OFFSET       = 0;
  public static final int               SIZE_OFFSET      = 1;
  public static final int               NEXT_OFFSET      = 2;
  public static final int               PREVIOUS_OFFSET  = 6;

  public static final byte              TAG_FREE         = 0;
  public static final byte              TAG_ALLOCATED    = 1;

  private final byte[]                  buffer;

  private final int                     minChunkSize;
  private final int[]                   freeListHeader;
  private final int[]                   freeListTail;
  private final int                     maxLevel;

  /**
   * @param capacity
   * @param minChunkSize
   *          - size of chunks on level 0. Should be power of 2.
   */
  public OBuddyMemory(int capacity, int minChunkSize) {
    synchronized (this) {
      minChunkSize = (int) Math.pow(2, (int) Math.ceil(Math.log(minChunkSize) / Math.log(2)));
      this.minChunkSize = minChunkSize;

      capacity = (int) Math.floor(capacity / minChunkSize) * minChunkSize + 1;
      maxLevel = (int) (Math.log((double) capacity / minChunkSize) / Math.log(2));

      freeListHeader = new int[maxLevel + 1];
      freeListTail = new int[maxLevel + 1];
      buffer = new byte[capacity];

      initMemory();
    }
  }

  public synchronized int allocate(byte[] bytes) {
    int pointer = allocate(bytes.length);

    if (pointer != ODirectMemory.NULL_POINTER)
      set(pointer, 0, bytes.length, bytes);

    return pointer;
  }

  public synchronized int allocate(final int size) {
    int level = Integer.SIZE - Integer.numberOfLeadingZeros((size + SYSTEM_INFO_SIZE - 1) / minChunkSize);

    if (level > maxLevel) {
      // We have no free space
      return NULL_POINTER;
    }

    int pointer = freeListHeader[level];
    if (pointer != NULL_POINTER) {
      removeNodeFromHead(level);
      buffer[pointer] = TAG_ALLOCATED;
    } else {
      int currentLevel = level + 1;
      while (freeListHeader[currentLevel] == NULL_POINTER) {
        currentLevel++;

        if (currentLevel > maxLevel) {
          // We have no free space
          return NULL_POINTER;
        }
      }

      pointer = removeNodeFromHead(currentLevel);

      do {
        pointer = split(pointer);
        currentLevel--;
        buffer[pointer + TAG_OFFSET] = (currentLevel == level) ? TAG_ALLOCATED : TAG_FREE;
        buffer[pointer + SIZE_OFFSET] = (byte) (currentLevel & 0xFF);
      } while (currentLevel > level);
    }

    return pointer;
  }

  public synchronized void free(int pointer) {
    int level = buffer[pointer + SIZE_OFFSET];
    int buddy = buddy(pointer, level);
    while (level < maxLevel && buffer[buddy + TAG_OFFSET] == TAG_FREE && buffer[buddy + SIZE_OFFSET] == level) {
      removeFromFreeList(level, buddy);

      if (buddy < pointer) {
        pointer = buddy;
      }
      level++;
      buddy = buddy(pointer, level);
    }

    buffer[pointer + TAG_OFFSET] = TAG_FREE;
    buffer[pointer + SIZE_OFFSET] = (byte) (level & 0xFF);
    addNodeToTail(level, pointer);
  }

  public int getActualSpace(int pointer) {
    return (1 << buffer[pointer + SIZE_OFFSET]) * minChunkSize;
  }

  public byte[] get(int pointer, int offset, final int length) {
    final int newLength;
    if (length > 0) {
      newLength = Math.min(length, size(pointer) - offset);
    } else {
      newLength = size(pointer) - offset;
    }

    byte[] dest = new byte[newLength];
    System.arraycopy(buffer, pointer + SYSTEM_INFO_SIZE + offset, dest, 0, newLength);
    return dest;
  }

  public void set(int pointer, int offset, int length, byte[] content) {
    System.arraycopy(content, 0, buffer, pointer + SYSTEM_INFO_SIZE + offset, length);
  }

  public <T> T get(int pointer, int offset, OBinarySerializer<T> serializer) {
    return serializer.deserializeNative(buffer, pointer + SYSTEM_INFO_SIZE + offset);
  }

  public <T> void set(int pointer, int offset, T data, OBinarySerializer<T> serializer) {
    serializer.serializeNative(data, buffer, pointer + SYSTEM_INFO_SIZE + offset);
  }

  public int capacity() {
    return buffer.length - 1;
  }

  public synchronized int freeSpace() {
    int freeSpace = 0;
    for (int level = 0; level <= maxLevel; level++) {
      int count = 0;
      int pointer = freeListHeader[level];
      while (pointer != NULL_POINTER) {
        count++;
        pointer = next(pointer);
      }

      freeSpace += (1 << level) * count;
    }
    freeSpace *= minChunkSize;

    return freeSpace;
  }

  public synchronized void clear() {
    initMemory();
  }

  public void setInt(int pointer, int offset, int value) {
    writeInt(pointer, offset + SYSTEM_INFO_SIZE, value);
  }

  public int getInt(int pointer, int offset) {
    return readInt(pointer, offset + SYSTEM_INFO_SIZE);
  }

  public long getLong(int pointer, int offset) {
    return CONVERTER.getLong(buffer, pointer + offset + SYSTEM_INFO_SIZE);
  }

  public void setLong(int pointer, int offset, long value) {
    CONVERTER.putLong(buffer, pointer + offset + SYSTEM_INFO_SIZE, value);
  }

  public byte getByte(int pointer, int offset) {
    int index = pointer + offset + SYSTEM_INFO_SIZE;
    return buffer[index];
  }

  public void setByte(int pointer, int offset, byte value) {
    int index = pointer + offset + SYSTEM_INFO_SIZE;
    buffer[index] = value;
  }

  public void copyData(int srcPointer, int fromOffset, int destPointer, int toOffset, int len) {
    int fromIndex = srcPointer + fromOffset + SYSTEM_INFO_SIZE;
    int toIndex = destPointer + toOffset + SYSTEM_INFO_SIZE;

    System.arraycopy(buffer, fromIndex, buffer, toIndex, len);
  }

  private void initMemory() {
    Arrays.fill(freeListHeader, 0, maxLevel + 1, NULL_POINTER);
    Arrays.fill(freeListTail, 0, maxLevel + 1, NULL_POINTER);

    int pointer = 0;
    byte level = (byte) maxLevel;
    int availSpace = buffer.length;

    while (level >= 0) {
      int chunkSize = (1 << level) * minChunkSize;
      if (availSpace > chunkSize) {
        buffer[pointer + TAG_OFFSET] = TAG_FREE;
        buffer[pointer + SIZE_OFFSET] = level;
        addNodeToTail(level, pointer);
        availSpace -= chunkSize;

        pointer = buddy(pointer, level);
      }
      level--;
    }
    assert availSpace == 1;

    buffer[pointer + TAG_OFFSET] = TAG_ALLOCATED;
  }

  private int split(int pointer) {
    int level = --buffer[pointer + SIZE_OFFSET];
    addNodeToTail(level, pointer);

    return buddy(pointer, level);
  }

  private int size(int pointer) {
    return (1 << buffer[pointer + SIZE_OFFSET]) * minChunkSize - SYSTEM_INFO_SIZE;
  }

  private int buddy(int pointer, int level) {
    return pointer ^ ((1 << level) * minChunkSize);
  }

  private int removeNodeFromHead(int level) {
    int pointer = freeListHeader[level];

    freeListHeader[level] = next(pointer);
    if (freeListHeader[level] != NULL_POINTER) {
      previous(freeListHeader[level], NULL_POINTER);
    } else {
      freeListTail[level] = NULL_POINTER;
    }
    return pointer;
  }

  private void addNodeToTail(int level, int pointer) {
    next(pointer, NULL_POINTER);
    previous(pointer, freeListTail[level]);
    if (freeListTail[level] == NULL_POINTER) {
      freeListHeader[level] = pointer;
    } else {
      next(freeListTail[level], pointer);
    }
    freeListTail[level] = pointer;
  }

  private void removeFromFreeList(int level, int pointer) {
    final int next = next(pointer);
    final int previous = previous(pointer);

    if (freeListHeader[level] == pointer) {
      freeListHeader[level] = next;
    } else {
      next(previous, next);
    }

    if (freeListTail[level] == pointer) {
      freeListTail[level] = previous;
    } else {
      previous(next, previous);
    }
  }

  private int next(int pointer) {
    return readInt(pointer, NEXT_OFFSET);
  }

  private void next(int pointer, int next) {
    writeInt(pointer, NEXT_OFFSET, next);
  }

  private int previous(int pointer) {
    return readInt(pointer, PREVIOUS_OFFSET);
  }

  private void previous(int pointer, int previous) {
    writeInt(pointer, PREVIOUS_OFFSET, previous);
  }

  private void writeInt(int pointer, int offset, int value) {
    CONVERTER.putInt(buffer, pointer + offset, value);
  }

  private int readInt(int pointer, int offset) {
    return CONVERTER.getInt(buffer, pointer + offset);
  }
}
