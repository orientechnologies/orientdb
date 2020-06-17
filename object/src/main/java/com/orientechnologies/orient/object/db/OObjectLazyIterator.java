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
package com.orientechnologies.orient.object.db;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;
import java.io.Serializable;
import java.util.Iterator;
import javassist.util.proxy.ProxyObject;

/**
 * Lazy implementation of Iterator that load the records only when accessed. It keep also track of
 * changes to the source record avoiding to call setDirty() by hand.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings({"unchecked"})
public class OObjectLazyIterator<TYPE> implements Iterator<TYPE>, Serializable {
  private static final long serialVersionUID = -4012483076050044405L;

  private final ProxyObject sourceRecord;
  private final OObjectDatabaseTx database;
  private final Iterator<? extends Object> underlying;
  private String fetchPlan;
  private final boolean autoConvert2Object;
  private OIdentifiable currentElement;
  private boolean orphanRemoval = false;

  public OObjectLazyIterator(
      final OObjectDatabaseTx database,
      final ProxyObject iSourceRecord,
      final Iterator<? extends Object> iIterator,
      final boolean iConvertToRecord,
      boolean iOrphanRemoval) {
    this.database = database;
    this.sourceRecord = iSourceRecord;
    this.underlying = iIterator;
    autoConvert2Object = iConvertToRecord;
    this.orphanRemoval = iOrphanRemoval;
  }

  public TYPE next() {
    return next(fetchPlan);
  }

  public TYPE next(final String iFetchPlan) {
    final Object value = underlying.next();

    if (value == null) return null;

    if (value instanceof ORID && autoConvert2Object) {
      currentElement = (OIdentifiable) value;
      ORecord record =
          ((ODatabaseDocument) database.getUnderlying()).load((ORID) value, iFetchPlan);
      if (record == null) {
        OLogManager.instance()
            .warn(
                this,
                "Record "
                    + ((OObjectProxyMethodHandler) sourceRecord.getHandler()).getDoc().getIdentity()
                    + " references a deleted instance");
        return null;
      }
      TYPE o = (TYPE) database.getUserObjectByRecord(record, iFetchPlan);
      ((OObjectProxyMethodHandler) (((ProxyObject) o)).getHandler()).setParentObject(sourceRecord);
      return o;
    } else if (value instanceof ODocument && autoConvert2Object) {
      currentElement = (OIdentifiable) value;
      TYPE o = (TYPE) database.getUserObjectByRecord((ODocument) value, iFetchPlan);
      ((OObjectProxyMethodHandler) (((ProxyObject) o)).getHandler()).setParentObject(sourceRecord);
      return o;
    } else {
      currentElement = database.getRecordByUserObject(value, false);
    }

    if (OObjectEntitySerializer.isToSerialize(value.getClass()))
      return (TYPE) OObjectEntitySerializer.deserializeFieldValue(value.getClass(), value);
    return (TYPE) value;
  }

  public boolean hasNext() {
    return underlying.hasNext();
  }

  public void remove() {
    underlying.remove();
    if (sourceRecord != null) {
      if (orphanRemoval)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .add(currentElement.getIdentity());
      ((OObjectProxyMethodHandler) sourceRecord.getHandler()).setDirty();
    }
  }

  public String getFetchPlan() {
    return fetchPlan;
  }

  public void setFetchPlan(String fetchPlan) {
    this.fetchPlan = fetchPlan;
  }
}
