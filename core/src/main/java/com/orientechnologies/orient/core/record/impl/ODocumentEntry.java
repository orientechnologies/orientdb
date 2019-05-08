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

import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Document entry. Used by ODocument.
 *
 * @author Emanuele Tagliaferri
 * @since 2.1
 */
public class ODocumentEntry {

  public Object                                          value;
  public Object                                          original;
  public OType                                           type;
  public OProperty                                       property;
  public OSimpleMultiValueChangeListener<Object, Object> changeListener;
  public OMultiValueChangeTimeLine<Object, Object>       timeLine;
  public boolean                                         changed = false;
  public boolean                                         exist   = true;
  public boolean                                         created = false;

  public ODocumentEntry() {

  }

  public boolean isChanged() {
    return changed;
  }

  public boolean isChangedTree(List<Object> ownersTrace) {
    if (changed && exist) {
      return true;
    }

    ownersTrace.add(value);

    if (value instanceof ODocument) {
      ODocument doc = (ODocument) value;
      return doc.isChangedInDepth();
    }

    if (value instanceof Collection) {
      Collection list = (Collection) value;
      for (Object element : list) {
        if (element instanceof ODocument) {
          ODocument doc = (ODocument) element;
          for (Map.Entry<String, ODocumentEntry> field : doc.fields.entrySet()) {
            if (field.getValue().isChangedTree(new ArrayList<>())) {
              return true;
            }
          }
        } else if (element instanceof Collection) {
          if (ODocumentHelper.isChangedCollection((Collection) element, this, ownersTrace, 1)) {
            return true;
          }
        } else if (element instanceof Map) {
          if (ODocumentHelper.isChangedMap((Map<Object, Object>) element, this, ownersTrace, 1)) {
            return true;
          }
        }
      }
    }

    if (value instanceof Map) {
      Map<Object, Object> map = (Map) value;
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        Object element = entry.getValue();
        if (element instanceof ODocument) {
          ODocument doc = (ODocument) element;
          for (Map.Entry<String, ODocumentEntry> field : doc.fields.entrySet()) {
            if (field.getValue().isChangedTree(new ArrayList<>())) {
              return true;
            }
          }
        } else if (element instanceof Collection) {
          if (ODocumentHelper.isChangedCollection((List) element, this, ownersTrace, 1)) {
            return true;
          }
        } else if (element instanceof Map) {
          if (ODocumentHelper.isChangedMap((Map<Object, Object>) element, this, ownersTrace, 1)) {
            return true;
          }
        }
      }
    }

    if (timeLine != null) {
      List<OMultiValueChangeEvent<Object, Object>> timeline = timeLine.getMultiValueChangeEvents();
      if (timeline != null) {
        for (OMultiValueChangeEvent<Object, Object> event : timeline) {
          if (event.getChangeType() == OMultiValueChangeEvent.OChangeType.ADD
              || event.getChangeType() == OMultiValueChangeEvent.OChangeType.NESTED
              || event.getChangeType() == OMultiValueChangeEvent.OChangeType.UPDATE
              || event.getChangeType() == OMultiValueChangeEvent.OChangeType.REMOVE) {
            return true;
          }
        }
      }
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

  public void setCreated(final boolean created) {
    this.created = created;
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

}
