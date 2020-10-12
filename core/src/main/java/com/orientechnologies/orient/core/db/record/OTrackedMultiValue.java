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
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import java.util.Iterator;
import java.util.List;

/**
 * Interface that indicates that collection will send notifications about operations that are
 * performed on it to the listeners.
 *
 * @param <K> Value that indicates position of item inside collection.
 * @param <V> Value that is hold by collection.
 */
public interface OTrackedMultiValue<K, V> {

  /**
   * Reverts all operations that were performed on collection and return original collection state.
   *
   * @param changeEvents List of operations that were performed on collection.
   * @return Original collection state.
   */
  Object returnOriginalState(List<OMultiValueChangeEvent<K, V>> changeEvents);

  Class<?> getGenericClass();

  void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue);

  void enableTracking(ORecordElement parent);

  void disableTracking(ORecordElement document);

  boolean isModified();

  boolean isTransactionModified();

  OMultiValueChangeTimeLine<Object, Object> getTimeLine();

  static <X> void nestedEnabled(Iterator<X> iterator, ORecordElement parent) {
    while (iterator.hasNext()) {
      X x = iterator.next();
      if (x instanceof OTrackedMultiValue) {
        ((OTrackedMultiValue) x).enableTracking(parent);
      }
    }
  }

  static <X> void nestedDisable(Iterator<X> iterator, ORecordElement parent) {
    while (iterator.hasNext()) {
      X x = iterator.next();
      if (x instanceof OTrackedMultiValue) {
        ((OTrackedMultiValue) x).disableTracking(parent);
      } else if (x instanceof ODocument) {
        if (((ODocument) x).isEmbedded()) {
          ODocumentInternal.clearTrackData((ODocument) x);
          ORecordInternal.unsetDirty((ODocument) x);
        }
      }
    }
  }

  static <X> void nestedTransactionClear(Iterator<X> iterator) {
    while (iterator.hasNext()) {
      X x = iterator.next();
      if (x instanceof OTrackedMultiValue) {
        ((OTrackedMultiValue) x).transactionClear();
      } else if (x instanceof ODocument) {
        if (((ODocument) x).isEmbedded()) {
          ODocumentInternal.clearTransactionTrackData((ODocument) x);
        }
      }
    }
  }

  void transactionClear();

  boolean addInternal(final V e);

  OMultiValueChangeTimeLine<K, V> getTransactionTimeLine();
}
