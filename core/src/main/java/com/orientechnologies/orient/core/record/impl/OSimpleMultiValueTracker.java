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

package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import java.lang.ref.WeakReference;

/**
 * Perform gathering of all operations performed on tracked collection and create mapping between
 * list of collection operations and field name that contains collection that was changed.
 *
 * @param <K> Value that uniquely identifies position of item in collection
 * @param <V> Item value.
 */
public final class OSimpleMultiValueTracker<K, V> {
  private final WeakReference<ORecordElement> element;
  private OMultiValueChangeTimeLine<Object, Object> timeLine;
  private boolean enabled;
  private OMultiValueChangeTimeLine<K, V> transactionTimeLine;

  public OSimpleMultiValueTracker(ORecordElement element) {
    this.element = new WeakReference<ORecordElement>(element);
  }

  public void addNoDirty(K key, V value) {
    onAfterRecordChanged(
        new OMultiValueChangeEvent<K, V>(OMultiValueChangeEvent.OChangeType.ADD, key, value, null),
        false);
  }

  public void removeNoDirty(K key, V value) {
    onAfterRecordChanged(
        new OMultiValueChangeEvent<K, V>(
            OMultiValueChangeEvent.OChangeType.REMOVE, key, null, value),
        false);
  }

  public void add(K key, V value) {
    onAfterRecordChanged(
        new OMultiValueChangeEvent<K, V>(OMultiValueChangeEvent.OChangeType.ADD, key, value), true);
  }

  public void updated(K key, V newValue, V oldValue) {
    onAfterRecordChanged(
        new OMultiValueChangeEvent<K, V>(
            OMultiValueChangeEvent.OChangeType.UPDATE, key, newValue, oldValue),
        true);
  }

  public void remove(K key, V value) {
    onAfterRecordChanged(
        new OMultiValueChangeEvent<K, V>(
            OMultiValueChangeEvent.OChangeType.REMOVE, key, null, value),
        true);
  }

  public void onAfterRecordChanged(final OMultiValueChangeEvent<K, V> event, boolean changeOwner) {
    if (!enabled) {
      return;
    }

    final ORecordElement document = this.element.get();
    if (document == null) {
      // doc not alive anymore, do nothing.
      return;
    }

    if (changeOwner) {
      document.setDirty();
    } else {
      document.setDirtyNoChanged();
    }

    if (timeLine == null) {
      timeLine = new OMultiValueChangeTimeLine<Object, Object>();
    }
    timeLine.addCollectionChangeEvent((OMultiValueChangeEvent<Object, Object>) event);

    if (transactionTimeLine == null) {
      transactionTimeLine = new OMultiValueChangeTimeLine<K, V>();
    }
    transactionTimeLine.addCollectionChangeEvent(event);
  }

  public void enable() {
    if (!this.enabled) {
      this.enabled = true;
    }
  }

  public void disable() {
    if (this.enabled) {
      this.timeLine = null;
      this.enabled = false;
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void sourceFrom(OSimpleMultiValueTracker<K, V> tracker) {
    this.timeLine = tracker.timeLine;
    this.transactionTimeLine = tracker.transactionTimeLine;
    this.enabled = tracker.enabled;
  }

  public OMultiValueChangeTimeLine<Object, Object> getTimeLine() {
    return timeLine;
  }

  public OMultiValueChangeTimeLine<K, V> getTransactionTimeLine() {
    return transactionTimeLine;
  }

  public void transactionClear() {
    transactionTimeLine = null;
  }
}
