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

import java.util.Arrays;
import java.util.Collection;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

/**
 * @author Andrey Lomakin
 * @since 21.01.13
 */
public class OExtendibleHashingBucket {
  public static final int     BUCKET_MAX_SIZE = 4;

  private OClusterPosition[]  keys            = new OClusterPosition[BUCKET_MAX_SIZE];
  private OPhysicalPosition[] values          = new OPhysicalPosition[BUCKET_MAX_SIZE];

  private int                 depth;

  private int                 size;

  public OExtendibleHashingBucket(int depth) {
    this.depth = depth;
  }

  public OPhysicalPosition get(final OClusterPosition key) {
    final int index = Arrays.binarySearch(keys, 0, size, key);

    if (index >= 0)
      return values[index];

    return null;
  }

  public int getPosition(final OClusterPosition key) {
    final int index = Arrays.binarySearch(keys, 0, size, key);

    if (index >= 0)
      return index;

    return -1;
  }

  public int size() {
    return size;
  }

  public void emptyBucket() {
    size = 0;
  }

  public Collection<OPhysicalPosition> getContent() {
    return Arrays.asList(values).subList(0, size);
  }

  public OPhysicalPosition deleteEntry(int position) {
    final OPhysicalPosition physicalPosition = values[position];

    if (position < size - 1) {
      System.arraycopy(keys, position + 1, keys, position, size - position - 1);
      System.arraycopy(values, position + 1, values, position, size - position - 1);
    } else {
      keys[position] = keys[size - 1];
      values[position] = values[size - 1];
    }

    size--;

    return physicalPosition;
  }

  public void addEntry(final OPhysicalPosition value) {
    if (BUCKET_MAX_SIZE - size <= 0) {
      throw new IllegalArgumentException("There is no enough size in bucket.");
    } else {
      final int index = Arrays.binarySearch(keys, 0, size, value.clusterPosition);

      if (index >= 0)
        throw new IllegalArgumentException("Given value is present in bucket.");

      final int insertionPoint = -index - 1;
      System.arraycopy(keys, insertionPoint, keys, insertionPoint + 1, size - insertionPoint);
      keys[insertionPoint] = value.clusterPosition;

      System.arraycopy(values, insertionPoint, values, insertionPoint + 1, size - insertionPoint);
      values[insertionPoint] = value;

      size++;
    }
  }

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) {
    this.depth = depth;
  }
}
