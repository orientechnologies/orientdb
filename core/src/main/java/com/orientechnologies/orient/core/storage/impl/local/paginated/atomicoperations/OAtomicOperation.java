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
package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.OComponentOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Note: all atomic operations methods are designed in context that all operations on single files will be wrapped in shared lock.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12/3/13
 */
public class OAtomicOperation {
  /**
   * Limit in bytes of total serialized size of component operation records which can be cached in single atomic operation
   */
  private static final int OPERATIONS_CACHE_LIMIT = 100 * 1024;

  private final OLogSequenceNumber startLSN;
  private final OOperationUnitId   operationUnitId;

  private int       startCounter;
  private boolean   rollback;
  private Exception rollbackException;

  private final Set<String>                              lockedObjects = new HashSet<>();
  private final Map<String, OAtomicOperationMetadata<?>> metadata      = new LinkedHashMap<>();

  private final List<OLogSequenceNumber>  componentLSNs    = new ArrayList<>();
  private final List<OComponentOperation> componentRecords = new ArrayList<>();

  private int totalSerializedSize = 0;

  public OAtomicOperation(OLogSequenceNumber startLSN, OOperationUnitId operationUnitId) {
    this.startLSN = startLSN;
    this.operationUnitId = operationUnitId;
    startCounter = 1;
  }

  public OLogSequenceNumber getStartLSN() {
    return startLSN;
  }

  public OOperationUnitId getOperationUnitId() {
    return operationUnitId;
  }

  public void addComponentOperation(OComponentOperation operation, boolean isMemory) {
    if (isMemory) {
      componentRecords.add(operation);
    } else {
      if (totalSerializedSize > OPERATIONS_CACHE_LIMIT) {
        componentLSNs.add(operation.getLsn());
      } else {
        totalSerializedSize += operation.getBinaryContentLen();

        if (totalSerializedSize > OPERATIONS_CACHE_LIMIT) {
          for (OWriteableWALRecord record : componentRecords) {
            componentLSNs.add(record.getLsn());
          }

          componentLSNs.add(operation.getLsn());

          componentRecords.clear();
        } else {
          componentRecords.add(operation);
        }
      }
    }
  }

  public List<OLogSequenceNumber> getComponentLSNs() {
    return Collections.unmodifiableList(componentLSNs);
  }

  public List<OComponentOperation> getComponentRecords() {
    return Collections.unmodifiableList(componentRecords);
  }

  /**
   * Add metadata with given key inside of atomic operation. If metadata with the same key insist inside of atomic operation it will
   * be overwritten.
   *
   * @param metadata Metadata to add.
   *
   * @see OAtomicOperationMetadata
   */
  public void addMetadata(OAtomicOperationMetadata<?> metadata) {
    this.metadata.put(metadata.getKey(), metadata);
  }

  /**
   * @param key Key of metadata which is looking for.
   *
   * @return Metadata by associated key or <code>null</code> if such metadata is absent.
   */
  public OAtomicOperationMetadata<?> getMetadata(String key) {
    return metadata.get(key);
  }

  /**
   * @return All keys and associated metadata contained inside of atomic operation
   */
  public Map<String, OAtomicOperationMetadata<?>> getMetadata() {
    return Collections.unmodifiableMap(metadata);
  }

  void incrementCounter() {
    startCounter++;
  }

  void decrementCounter() {
    startCounter--;
  }

  int getCounter() {
    return startCounter;
  }

  void rollback(Exception e) {
    rollback = true;
    rollbackException = e;
  }

  Exception getRollbackException() {
    return rollbackException;
  }

  boolean isRollback() {
    return rollback;
  }

  void addLockedObject(String lockedObject) {
    lockedObjects.add(lockedObject);
  }

  boolean containsInLockedObjects(String objectToLock) {
    return lockedObjects.contains(objectToLock);
  }

  Iterable<String> lockedObjects() {
    return lockedObjects;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OAtomicOperation operation = (OAtomicOperation) o;

    if (!operationUnitId.equals(operation.operationUnitId))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return operationUnitId.hashCode();
  }
}
