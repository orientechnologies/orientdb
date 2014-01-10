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
package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 12/3/13
 */
public class OAtomicOperation {
  private final OLogSequenceNumber startLSN;
  private final OOperationUnitId   operationUnitId;

  private int                      startCounter;
  private boolean                  rollback;

  private Set<Object>              lockedObjects = new HashSet<Object>();

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

  void incrementCounter() {
    startCounter++;
  }

  int decrementCounter() {
    startCounter--;
    return startCounter;
  }

  void rollback() {
    rollback = true;
  }

  boolean isRollback() {
    return rollback;
  }

  void addLockedObject(Object lockedObject) {
    lockedObjects.add(lockedObject);
  }

  boolean containsInLockedObjects(Object objectToLock) {
    return lockedObjects.contains(objectToLock);
  }

  Iterable<Object> lockedObjects() {
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
