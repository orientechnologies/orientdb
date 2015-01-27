/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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

import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;

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
