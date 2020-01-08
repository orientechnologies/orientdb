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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/21/14
 */
public abstract class OFileCreatedWALRecord<T> extends OOperationUnitBodyRecord<T> {
  private String fileName;
  private long   fileId;

  public OFileCreatedWALRecord() {
  }

  public OFileCreatedWALRecord(T operationUnitId, String fileName, long fileId) {
    super(operationUnitId);
    this.fileName = fileName;
    this.fileId = fileId;
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileId() {
    return fileId;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OStringSerializer.INSTANCE.serializeNativeObject(fileName, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(fileName);

    OLongSerializer.INSTANCE.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    OStringSerializer.INSTANCE.serializeInByteBufferObject(fileName, buffer);
    buffer.putLong(fileId);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    fileName = OStringSerializer.INSTANCE.deserializeNativeObject(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(fileName);

    fileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OStringSerializer.INSTANCE.getObjectSize(fileName) + OLongSerializer.LONG_SIZE;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }
}
