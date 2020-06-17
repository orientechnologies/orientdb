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

package com.orientechnologies.orient.core.db.record.ridbag;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.record.impl.OSimpleMultiValueTracker;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.UUID;

public interface ORidBagDelegate
    extends Iterable<OIdentifiable>,
        ORecordLazyMultiValue,
        OTrackedMultiValue<OIdentifiable, OIdentifiable>,
        ORecordElement {
  void addAll(Collection<OIdentifiable> values);

  void add(OIdentifiable identifiable);

  void remove(OIdentifiable identifiable);

  boolean isEmpty();

  int getSerializedSize();

  int getSerializedSize(byte[] stream, int offset);

  /**
   * Writes content of bag to stream.
   *
   * <p>OwnerUuid is needed to notify db about changes of collection pointer if some happens during
   * serialization.
   *
   * @param stream to write content
   * @param offset in stream where start to write content
   * @param ownerUuid id of delegate owner
   * @return offset where content of stream is ended
   */
  int serialize(byte[] stream, int offset, UUID ownerUuid);

  int deserialize(byte[] stream, int offset);

  void requestDelete();

  /**
   * THIS IS VERY EXPENSIVE METHOD AND CAN NOT BE CALLED IN REMOTE STORAGE.
   *
   * @param identifiable Object to check.
   * @return true if ridbag contains at leas one instance with the same rid as passed in
   *     identifiable.
   */
  boolean contains(OIdentifiable identifiable);

  public void setOwner(ORecordElement owner);

  public ORecordElement getOwner();

  public String toString();

  NavigableMap<OIdentifiable, Change> getChanges();

  void setSize(int size);

  OSimpleMultiValueTracker<OIdentifiable, OIdentifiable> getTracker();

  void setTracker(OSimpleMultiValueTracker<OIdentifiable, OIdentifiable> tracker);

  void setTransactionModified(boolean transactionModified);
}
