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

package com.orientechnologies.orient.core.record.impl;

import java.util.HashMap;

import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.ORecordElement.STATUS;

/**
 * Perform gathering of all operations performed on tracked collection and create mapping between list of collection operations and
 * field name that contains collection that was changed.
 * 
 * @param <K>
 *          Value that uniquely identifies position of item in collection
 * @param <V>
 *          Item value.
 */
final class OSimpleMultiValueChangeListener<K, V> implements OMultiValueChangeListener<K, V> {
  /**
   * 
   */
  private final ODocument oDocument;
  private final String    fieldName;

  OSimpleMultiValueChangeListener(ODocument oDocument, final String fieldName) {
    this.oDocument = oDocument;
    this.fieldName = fieldName;
  }

  public void onAfterRecordChanged(final OMultiValueChangeEvent<K, V> event) {
    if (this.oDocument.getInternalStatus() != STATUS.UNMARSHALLING) {
      if (event.isChangesOwnerContent())
        this.oDocument.setDirty();
      else
        this.oDocument.setDirtyNoChanged();
    }

    if (!(this.oDocument._trackingChanges && this.oDocument.getIdentity().isValid())
        || this.oDocument.getInternalStatus() == STATUS.UNMARSHALLING)
      return;

    if (this.oDocument._fieldOriginalValues != null && this.oDocument._fieldOriginalValues.containsKey(fieldName))
      return;

    if (this.oDocument._fieldCollectionChangeTimeLines == null)
      this.oDocument._fieldCollectionChangeTimeLines = new HashMap<String, OMultiValueChangeTimeLine<Object, Object>>();

    OMultiValueChangeTimeLine<Object, Object> timeLine = this.oDocument._fieldCollectionChangeTimeLines.get(fieldName);
    if (timeLine == null) {
      timeLine = new OMultiValueChangeTimeLine<Object, Object>();
      this.oDocument._fieldCollectionChangeTimeLines.put(fieldName, timeLine);
    }

    timeLine.addCollectionChangeEvent((OMultiValueChangeEvent<Object, Object>) event);
  }
}
