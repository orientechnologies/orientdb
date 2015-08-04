/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 5/8/14
 */
public class OHashIndexFileLevelMetadataPage extends ODurablePage {

  private final static int RECORDS_COUNT_OFFSET       = NEXT_FREE_POSITION;
  private final static int KEY_SERIALIZER_ID_OFFSET   = RECORDS_COUNT_OFFSET + OLongSerializer.LONG_SIZE;
  private final static int VALUE_SERIALIZER_ID_OFFSET = KEY_SERIALIZER_ID_OFFSET + OByteSerializer.BYTE_SIZE;
  private final static int METADATA_ARRAY_OFFSET      = VALUE_SERIALIZER_ID_OFFSET + OByteSerializer.BYTE_SIZE;

  private final static int ITEM_SIZE                  = OByteSerializer.BYTE_SIZE + 3 * OLongSerializer.LONG_SIZE;

  public OHashIndexFileLevelMetadataPage(OCacheEntry cacheEntry, OWALChangesTree changesTree, boolean isNewPage) throws IOException {
    super(cacheEntry, changesTree);

    if (isNewPage) {
      for (int i = 0; i < OLocalHashTable.HASH_CODE_SIZE; i++)
        remove(i);

      setRecordsCount(0);
      setKeySerializerId((byte) -1);
      setValueSerializerId((byte) -1);
    }
  }

  public void setRecordsCount(long recordsCount) throws IOException {
    setLongValue(RECORDS_COUNT_OFFSET, recordsCount);
  }

  public long getRecordsCount() throws IOException {
    return getLongValue(RECORDS_COUNT_OFFSET);
  }

  public void setKeySerializerId(byte keySerializerId) throws IOException {
    setByteValue(KEY_SERIALIZER_ID_OFFSET, keySerializerId);
  }

  public byte getKeySerializerId() throws IOException {
    return getByteValue(KEY_SERIALIZER_ID_OFFSET);
  }

  public void setValueSerializerId(byte valueSerializerId) throws IOException {
    setByteValue(VALUE_SERIALIZER_ID_OFFSET, valueSerializerId);
  }

  public byte getValueSerializerId() throws IOException {
    return getByteValue(VALUE_SERIALIZER_ID_OFFSET);
  }

  public void setFileMetadata(int index, long fileId, long bucketsCount, long tombstoneIndex) throws IOException {
    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    setByteValue(offset, (byte) 1);

    offset += OByteSerializer.BYTE_SIZE;

    setLongValue(offset, fileId);
    offset += OLongSerializer.LONG_SIZE;

    setLongValue(offset, bucketsCount);
    offset += OLongSerializer.LONG_SIZE;

    setLongValue(offset, tombstoneIndex);
    offset += OLongSerializer.LONG_SIZE;
  }

  public void setBucketsCount(int index, long bucketsCount) throws IOException {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE;
    setLongValue(offset, bucketsCount);
  }

  public long getBucketsCount(int index) throws IOException {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE;
    return getLongValue(offset);
  }

  public void setTombstoneIndex(int index, long tombstoneIndex) throws IOException {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += OByteSerializer.BYTE_SIZE + 2 * OLongSerializer.LONG_SIZE;
    setLongValue(offset, tombstoneIndex);
  }

  public long getTombstoneIndex(int index) {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += OByteSerializer.BYTE_SIZE + 2 * OLongSerializer.LONG_SIZE;
    return getLongValue(offset);
  }

  public long getFileId(int index) {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += OByteSerializer.BYTE_SIZE;
    return getLongValue(offset);
  }

  public boolean isRemoved(int index) {
    final int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;
    return getByteValue(offset) == 0;
  }

  public void remove(int index) {
    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;
    setByteValue(offset, (byte) 0);
  }
}
