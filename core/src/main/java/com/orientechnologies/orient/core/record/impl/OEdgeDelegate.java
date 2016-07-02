/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.Optional;
import java.util.Set;

/**
 * @author Luigi Dell'Aquila
 */
public class OEdgeDelegate implements OEdge {
  protected OVertex vOut;
  protected OVertex vIn;
  protected OClass  lightweightEdgeType;

  protected ODocument element;

  public OEdgeDelegate(OVertex out, OVertex in, OClass lightweightEdgeType) {
    vOut = out;
    vIn = in;
    this.lightweightEdgeType = lightweightEdgeType;
  }

  public OEdgeDelegate(ODocument elem) {
    this.element = elem;
  }

  @Override public OVertex getFrom() {
    if (vIn != null)
      // LIGHTWEIGHT EDGE
      return vIn;

    final ODocument doc = getRecord();
    if (doc == null)
      return null;

    Object result = doc.getProperty(DIRECITON_OUT);
    if (!(result instanceof OElement)) {
      return null;
    }
    OElement v = (OElement) result;
    if (!v.isVertex()) {
      return null;//TODO optional...?
    }
    return v.asVertex().get();
  }

  @Override public OVertex getTo() {
    if (vIn != null)
      // LIGHTWEIGHT EDGE
      return vIn;

    final ODocument doc = getRecord();
    if (doc == null)
      return null;

    Object result = doc.getProperty(DIRECITON_IN);
    if (!(result instanceof OElement)) {
      return null;
    }
    OElement v = (OElement) result;
    if (!v.isVertex()) {
      return null;//TODO optional...?
    }
    return v.asVertex().get();
  }

  @Override public Set<String> getPropertyNames() {
    if (element != null) {
      return element.getPropertyNames();
    }
    return null;
  }

  public void delete() {
    ((OVertexDelegate) getFrom()).detachOutgointEdge(this);
    ((OVertexDelegate) getTo()).detachIncomingEdge(this);
    if (element != null) {
      element.delete();
    }
  }

  @Override public <RET> RET getProperty(String name) {
    return element == null ? null : element.getProperty(name);
  }

  @Override public void setProperty(String name, Object value) {
    if (element == null) {
      //promote to regular edge
      ODatabase db = getDatabase();
      OVertexDelegate from = (OVertexDelegate) getFrom();
      OVertexDelegate to = (OVertexDelegate) getTo();
      from.detachOutgointEdge(this);
      to.detachIncomingEdge(this);
      this.element = db.newEdge(from, to, lightweightEdgeType).getRecord();
      this.lightweightEdgeType = null;
    }
    element.setProperty(name, value);
  }

  private ODatabase getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  @Override public Optional<OVertex> asVertex() {
    return Optional.empty();
  }

  @Override public Optional<OEdge> asEdge() {
    return Optional.of(this);
  }

  @Override public boolean isDocument() {
    return true;
  }

  @Override public boolean isVertex() {
    return false;
  }

  @Override public boolean isEdge() {
    return true;
  }

  @Override public Optional<OClass> getType() {
    if (element == null) {
      return Optional.of(lightweightEdgeType);
    }
    return Optional.ofNullable(element.getSchemaClass());
  }

  @Override public ORID getIdentity() {
    if (element == null) {
      return null;
    }
    return element.getIdentity();
  }

  @Override public <T extends ORecord> T getRecord() {
    if (element == null) {
      return null;
    }
    return (T) element;
  }

  @Override public void lock(boolean iExclusive) {
    if (element != null)
      element.lock(iExclusive);
  }

  @Override public boolean isLocked() {
    if (element != null)
      return element.isLocked();
    return false;
  }

  @Override public OStorage.LOCKING_STRATEGY lockingStrategy() {
    if (element != null)
      return element.lockingStrategy();
    return OStorage.LOCKING_STRATEGY.NONE;
  }

  @Override public void unlock() {
    if (element != null)
      element.unlock();
  }

  @Override public int compare(OIdentifiable o1, OIdentifiable o2) {
    return o1.compareTo(o2);
  }

  @Override public int compareTo(OIdentifiable o) {
    return 0;
  }

  @Override public boolean equals(Object obj) {
    if (element == null) {
      return this == obj;
      //TODO double-check this logic for lightweight edges
    }
    if (!(obj instanceof OIdentifiable)) {
      return false;
    }
    if (!(obj instanceof OElement)) {
      obj = ((OIdentifiable) obj).getRecord();
    }

    return element.equals(((OElement) obj).getRecord());
  }

  @Override public int hashCode() {
    if (element == null) {
      return super.hashCode();
    }

    return element.hashCode();
  }
}
