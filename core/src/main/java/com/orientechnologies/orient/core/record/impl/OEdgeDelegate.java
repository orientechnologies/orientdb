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
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
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

  @Override
  public OVertex getFrom() {
    if (vOut != null)
      // LIGHTWEIGHT EDGE
      return vOut;

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

  @Override
  public OVertex getTo() {
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

  @Override
  public boolean isLightweight() {
    return this.element == null;
  }

  @Override
  public Set<String> getPropertyNames() {
    if (element != null) {
      return element.getPropertyNames();
    }
    return null;
  }

  public OEdge delete() {
    deleteLinks(this);
    if (element != null) {
      element.delete();
    }
    return this;
  }

  public static void deleteLinks(OEdgeDelegate delegate) {
    OVertexDelegate from = ((OVertexDelegate) delegate.getFrom());
    from.detachOutgointEdge(delegate);
    OVertexDelegate to = ((OVertexDelegate) delegate.getTo());
    to.detachIncomingEdge(delegate);
    from.save();
    to.save();
  }

  @Override
  public <RET> RET getProperty(String name) {
    return element == null ? null : element.getProperty(name);
  }

  @Override
  public void setProperty(String name, Object value) {
    if (element == null) {
      promoteToRegularEdge();
    }
    element.setProperty(name, value);
  }

  @Override
  public void setProperty(String name, Object value, OType... fieldType) {
    if (element == null) {
      promoteToRegularEdge();
    }
    element.setProperty(name, value, fieldType);
  }

  private void promoteToRegularEdge() {
    ODatabaseDocument db = getDatabase();
    OVertexDelegate from = (OVertexDelegate) getFrom();
    OVertexDelegate to = (OVertexDelegate) getTo();
    from.detachOutgointEdge(this);
    to.detachIncomingEdge(this);
    this.element = db.newEdge(from, to, lightweightEdgeType).getRecord();
    this.lightweightEdgeType = null;
    this.vOut = null;
    this.vIn = null;
  }

  @Override
  public <RET> RET removeProperty(String name) {
    return element.removeProperty(name);
  }

  @Override
  public Optional<OVertex> asVertex() {
    return Optional.empty();
  }

  @Override
  public Optional<OEdge> asEdge() {
    return Optional.of(this);
  }

  @Override
  public boolean isVertex() {
    return false;
  }

  @Override
  public boolean isEdge() {
    return true;
  }

  @Override
  public Optional<OClass> getSchemaType() {
    if (element == null) {
      return Optional.of(lightweightEdgeType);
    }
    return Optional.ofNullable(element.getSchemaClass());
  }

  @Override
  public ORID getIdentity() {
    if (element == null) {
      return null;
    }
    return element.getIdentity();
  }

  @Override
  public <T extends ORecord> T getRecord() {

    if (element == null) {
      return null;
    }
    return (T) element;
  }

  @Override
  public void lock(boolean iExclusive) {
    if (element != null)
      element.lock(iExclusive);
  }

  @Override
  public boolean isLocked() {
    if (element != null)
      return element.isLocked();
    return false;
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy() {
    if (element != null)
      return element.lockingStrategy();
    return OStorage.LOCKING_STRATEGY.NONE;
  }

  @Override
  public void unlock() {
    if (element != null)
      element.unlock();
  }

  @Override
  public int compare(OIdentifiable o1, OIdentifiable o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int compareTo(OIdentifiable o) {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
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

  @Override
  public int hashCode() {
    if (element == null) {
      return super.hashCode();
    }

    return element.hashCode();
  }

  @Override
  public STATUS getInternalStatus() {
    if (element == null) {
      return STATUS.LOADED;
    }
    return element.getInternalStatus();
  }

  @Override
  public void setInternalStatus(STATUS iStatus) {
    if (element != null)
      element.setInternalStatus(iStatus);
  }

  @Override
  public <RET> RET setDirty() {
    if (element != null)
      element.setDirty();
    return (RET) this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (element != null)
      element.setDirtyNoChanged();
  }

  @Override
  public ORecordElement getOwner() {
    if (element != null)
      return element.getOwner();
    return null;
  }

  @Override
  public byte[] toStream() throws OSerializationException {
    if (element != null)
      return element.toStream();
    return null;
  }

  @Override
  public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
    if (element != null)
      return element.fromStream(iStream);
    return null;
  }

  @Override
  public boolean detach() {
    if (element != null)
      return element.detach();
    return true;
  }

  @Override
  public <RET extends ORecord> RET reset() {
    if (element != null)
      element.reset();
    return (RET) this;
  }

  @Override
  public OEdge unload() {
    if (element != null)
      element.unload();
    return this;
  }

  @Override
  public OEdge clear() {
    if (element != null)
      element.clear();
    return this;
  }

  @Override
  public OEdge copy() {
    if (element != null) {
      return new OEdgeDelegate(element.copy());
    } else {
      return new OEdgeDelegate(vOut, vIn, lightweightEdgeType);

    }
  }

  @Override
  public int getVersion() {
    if (element != null)
      return element.getVersion();
    return 1;
  }

  @Override
  public ODatabaseDocument getDatabase() {
    if (element != null) {
      return element.getDatabase();
    } else {
      return ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    }
  }

  @Override
  public boolean isDirty() {
    if (element != null)
      return element.isDirty();
    return false;
  }

  @Override
  public <RET extends ORecord> RET load() throws ORecordNotFoundException {
    ORecord newItem = element.load();
    if (newItem == null) {
      return null;
    }
    return (RET) new OVertexDelegate((ODocument) newItem);
  }

  @Override
  public <RET extends ORecord> RET reload() throws ORecordNotFoundException {
    if (element != null)
      element.reload();
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET reload(String fetchPlan, boolean ignoreCache, boolean force) throws ORecordNotFoundException {
    if (element != null)
      element.reload(fetchPlan, ignoreCache, force);
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET save() {
    if (element != null) {
      element.save();
    } else {
      vIn.save();
      vOut.save();
    }
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET save(String iCluster) {
    if (element != null) {
      element.save(iCluster);
    } else {
      vIn.save();
      vOut.save();
    }
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET save(boolean forceCreate) {
    if (element != null) {
      element.save(forceCreate);
    } else {
      vIn.save();
      vOut.save();
    }
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET save(String iCluster, boolean forceCreate) {
    if (element != null) {
      element.save(iCluster, forceCreate);
    } else {
      vIn.save();
      vOut.save();
    }
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET fromJSON(String iJson) {
    if (element == null) {
      promoteToRegularEdge();
    }
    element.fromJSON(iJson);
    return (RET) this;
  }

  @Override
  public String toJSON() {
    if (element != null) {
      return element.toJSON();
    } else {
      return "{'out':'" + vOut.getIdentity() + "', 'in':'" + vIn.getIdentity() + "', '@class':'" + OStringSerializerHelper
          .encode(lightweightEdgeType.getName()) + "'}";
    }
  }

  @Override
  public String toJSON(String iFormat) {
    if (element != null) {
      return element.toJSON(iFormat);
    } else {
      return "{'out':'" + vOut.getIdentity() + "', 'in':'" + vIn.getIdentity() + "', '@class':'" + OStringSerializerHelper
          .encode(lightweightEdgeType.getName()) + "'}";
    }
  }

  @Override
  public int getSize() {
    if (element != null)
      return element.getSize();
    return 0;
  }
}
