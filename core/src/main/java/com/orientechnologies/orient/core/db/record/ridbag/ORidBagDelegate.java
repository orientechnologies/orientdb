/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.db.record.ridbag;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;

public interface ORidBagDelegate extends Iterable<OIdentifiable>, ORecordLazyMultiValue,
    OTrackedMultiValue<OIdentifiable, OIdentifiable> {
  public void addAll(Collection<OIdentifiable> values);

  public void add(OIdentifiable identifiable);

  public void remove(OIdentifiable identifiable);

  public boolean isEmpty();

  public int getSerializedSize();

  public int getSerializedSize(byte[] stream, int offset);

  public int serialize(byte[] stream, int offset);

  public int deserialize(byte[] stream, int offset);

  public void requestDelete();

  public void setOwner(ORecord<?> owner);

  public ORecord<?> getOwner();

  public String toString();
}
