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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.OByteArrayInputStream;
import com.orientechnologies.orient.core.serialization.OByteArrayOutputStream;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Andrey Lomakin
 * @since 24.05.13
 */
public class OAtomicUnitEndRecord extends OOperationUnitBodyRecord {
  private boolean rollback;

  private Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadataMap     = new HashMap<String, OAtomicOperationMetadata<?>>();
  private byte[]                                   atomicOperationsMetadataBinary = new byte[0];

  public OAtomicUnitEndRecord() {
  }

  OAtomicUnitEndRecord(OOperationUnitId operationUnitId, boolean rollback,
      Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadataMap) {
    super(operationUnitId);

    this.rollback = rollback;

    assert operationUnitId != null;

    if (atomicOperationMetadataMap != null && atomicOperationMetadataMap.size() > 0) {
      this.atomicOperationMetadataMap = atomicOperationMetadataMap;
      OByteArrayOutputStream byteArrayOutputStream = new OByteArrayOutputStream();
      try {
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(atomicOperationMetadataMap);
        objectOutputStream.close();
      } catch (IOException e) {
        throw new IllegalStateException("Error during metadata serialization", e);
      }

      atomicOperationsMetadataBinary = byteArrayOutputStream.toByteArray();
    }

  }

  public boolean isRollback() {
    return rollback;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    content[offset] = rollback ? (byte) 1 : 0;
    offset++;

    OIntegerSerializer.INSTANCE.serializeNative(atomicOperationsMetadataBinary.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (atomicOperationsMetadataBinary.length > 0) {
      System.arraycopy(atomicOperationsMetadataBinary, 0, content, offset, atomicOperationsMetadataBinary.length);
      offset += atomicOperationsMetadataBinary.length;
    }

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    rollback = content[offset] > 0;

    final int len = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (len > 0) {
      atomicOperationsMetadataBinary = new byte[len];
      System.arraycopy(content, offset, atomicOperationsMetadataBinary, 0, len);

      final OByteArrayInputStream byteArrayInputStream = new OByteArrayInputStream(atomicOperationsMetadataBinary);
      try {
        final ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        atomicOperationMetadataMap = (Map<String, OAtomicOperationMetadata<?>>) objectInputStream.readObject();
        objectInputStream.close();
      } catch (ClassNotFoundException cnfe) {
        throw new IllegalStateException("Error during atomic operation metadata deserialization", cnfe);
      } catch (IOException ioe) {
        throw new IllegalStateException("Error during atomic operation metadata deserialization", ioe);
      }
    } else {
      atomicOperationsMetadataBinary = new byte[0];
      atomicOperationMetadataMap = new HashMap<String, OAtomicOperationMetadata<?>>();
    }

    return offset;
  }

  public Map<String, OAtomicOperationMetadata<?>> getAtomicOperationMetadata() {
    return Collections.unmodifiableMap(atomicOperationMetadataMap);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + atomicOperationsMetadataBinary.length;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public String toString() {
    return toString("rollback=" + rollback);
  }
}
