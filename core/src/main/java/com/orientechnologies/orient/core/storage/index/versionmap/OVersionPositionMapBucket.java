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

package com.orientechnologies.orient.core.storage.index.versionmap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

public final class OVersionPositionMapBucket extends ODurablePage {
  private static final int NEXT_PAGE_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = NEXT_PAGE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int POSITIONS_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  // use int for version
  private static final int VERSION_ENTRY_SIZE = OIntegerSerializer.INT_SIZE;

  public OVersionPositionMapBucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public int getVersion(final int index) {
    final int entryPosition = entryPosition(index); // ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int value = getIntValue(entryPosition);
    if (value < 0) {
      throw new OStorageException(
          "Entry with index " + index + " might be deleted and can not be used.");
    }
    return value;
  }

  public void incrementVersion(final int index) {
    final int entryPosition = entryPosition(index);
    final int value = getIntValue(entryPosition);
    if (value < 0) {
      throw new OStorageException(
          "Entry with index " + index + " might be deleted and can not be used.");
    }
    setIntValue(entryPosition, value + 1);
    final int newValue = getVersion(index);
    assert value + 1 == newValue;
  }

  static int entryPosition(int index) {
    return index * VERSION_ENTRY_SIZE + POSITIONS_OFFSET;
  }
}
