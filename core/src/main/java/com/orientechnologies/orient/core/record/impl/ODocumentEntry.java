/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(-at-)orientdb.com)
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

import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;

import java.util.List;
import java.util.Map;

/**
 * Document entry. Used by ODocument.
 *
 * @author Emanuele Tagliaferri
 * @since 2.1
 */
public class ODocumentEntry {

  public  Object    value;
  public  Object    original;
  public  OType     type;
  public  OProperty property;
  private boolean   changed = false;
  private boolean   exist   = true;
  private boolean   created = false;

  public ODocumentEntry() {

  }

  public boolean isChanged() {
    return changed;
  }

  public boolean isChangedTree() {
    if ((changed || isTrackedModified()) && exist ) {
      return true;
    }
    return false;
  }

  public boolean hasNonExistingTree() {
    if (!exist) {
      return true;
    }

    if (value instanceof ODocument) {
      ODocument doc = (ODocument) value;
      for (Map.Entry<String, ODocumentEntry> field : doc.fields.entrySet()) {
        if (field.getValue().hasNonExistingTree()) {
          return true;
        }
      }
    }

    if (value instanceof List) {
      List list = (List) value;
      for (Object element : list) {
        if (element instanceof ODocument) {
          ODocument doc = (ODocument) element;
          for (Map.Entry<String, ODocumentEntry> field : doc.fields.entrySet()) {
            if (field.getValue().hasNonExistingTree()) {
              return true;
            }
          }
        } else if (element instanceof List) {
          if (ODocumentHelper.hasNonExistingInList((List) element)) {
            return true;
          }
        }
      }
    }

    return false;
  }

  public void setChanged(final boolean changed) {
    this.changed = changed;
  }

  public boolean exist() {
    return exist;
  }

  public void setExist(final boolean exist) {
    this.exist = exist;
  }

  public boolean isCreated() {
    return created;
  }

  protected ODocumentEntry clone() {
    final ODocumentEntry entry = new ODocumentEntry();
    entry.type = type;
    entry.property = property;
    entry.value = value;
    entry.changed = changed;
    entry.created = created;
    entry.exist = exist;
    return entry;
  }

  public OMultiValueChangeTimeLine<Object, Object> getTimeLine() {
    if (!isChanged() && value instanceof OTrackedMultiValue) {
      return ((OTrackedMultiValue) value).getTimeLine();
    } else {
      return null;
    }
  }

  public void clear() {
    this.created = false;
    this.changed = false;
    original = null;
    removeTimeline();
  }

  public void clearNotExists() {
    original = null;
    removeTimeline();
  }

  public void removeTimeline() {
    if (!(value instanceof OTrackedMultiValue))
      return;
    ((OTrackedMultiValue) value).disableTracking(null);
  }

  public void replaceListener(ODocument document, Object oldValue) {
    enableTracking(document);
  }

  public boolean enableTracking(ODocument document) {
    if (!(value instanceof OTrackedMultiValue))
      return false;
    ((OTrackedMultiValue) value).enableTracking(document);
    return true;
  }

  public void disableTracking(ODocument document, Object fieldValue) {
    if (!(fieldValue instanceof OTrackedMultiValue))
      return;
    ((OTrackedMultiValue) fieldValue).disableTracking(document);
  }

  public boolean isTrackedModified() {
    if (value instanceof OTrackedMultiValue) {
      return ((OTrackedMultiValue) value).isModified();
    }
    if (value instanceof ODocument) {
      return ((ODocument) value).isDirty();
    }
    return false;
  }

  public boolean isTxTrackedModified() {
    if (value instanceof OTrackedMultiValue) {
      return ((OTrackedMultiValue) value).isTransactionModified();
    }
    if (value instanceof ODocument && ((ODocument) value).isEmbedded()) {
      return ((ODocument) value).isDirty();
    }
    return false;
  }

  public void markChanged() {
    this.changed = true;
  }

  public void unmarkChanged() {
    this.changed = false;
  }

  public void markCreated() {
    this.created = true;
  }

  public void unmarkCreated() {
    this.created = false;
  }

  public void undo() {
    if (isChanged()) {
      value = original;
      unmarkChanged();
      original = null;
      exist = true;
    }
  }

  public void transactionClear() {
    if (value instanceof OTrackedMultiValue) {
      ((OTrackedMultiValue) value).transactionClear();
    }
  }
}
