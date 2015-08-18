/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.object.fetch;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.reflection.OReflectionHelper;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.orientechnologies.orient.object.db.OObjectLazyList;
import com.orientechnologies.orient.object.db.OObjectLazyMap;
import com.orientechnologies.orient.object.db.OObjectLazySet;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.enumerations.OObjectEnumLazyList;
import com.orientechnologies.orient.object.enumerations.OObjectEnumLazyMap;
import com.orientechnologies.orient.object.enumerations.OObjectEnumLazySet;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerList;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerMap;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerSet;
import com.orientechnologies.orient.object.serialization.OObjectSerializerHelper;

/**
 * @author luca.molino
 * 
 */
public class OObjectFetchContext implements OFetchContext {

  protected final String                    fetchPlan;
  protected final boolean                   lazyLoading;
  protected final OEntityManager            entityManager;
  protected final OUserObject2RecordHandler obj2RecHandler;

  public OObjectFetchContext(final String iFetchPlan, final boolean iLazyLoading, final OEntityManager iEntityManager,
      final OUserObject2RecordHandler iObj2RecHandler) {
    fetchPlan = iFetchPlan;
    lazyLoading = iLazyLoading;
    obj2RecHandler = iObj2RecHandler;
    entityManager = iEntityManager;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void onBeforeMap(ODocument iRootRecord, String iFieldName, final Object iUserObject) throws OFetchException {
    final Map map = (Map) iRootRecord.field(iFieldName);
    Map target = null;
    final Field f = OObjectEntitySerializer.getField(iFieldName, iUserObject.getClass());
    final boolean customSerialization = OObjectEntitySerializer.isSerializedType(f);
    final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
    if (map instanceof ORecordLazyMap
        || (map instanceof OTrackedMap<?> && !OReflectionHelper.isJavaType(genericType) && !customSerialization && !genericType
            .isEnum())) {
      target = new OObjectLazyMap(iUserObject, (OTrackedMap<?>) map, OObjectEntitySerializer.isCascadeDeleteField(
          iUserObject.getClass(), f.getName()));
    } else if (customSerialization) {
      target = new OObjectCustomSerializerMap<TYPE>(OObjectEntitySerializer.getSerializedType(f), iRootRecord,
          (Map<Object, Object>) map);
    } else if (genericType.isEnum()) {
      target = new OObjectEnumLazyMap(genericType, iRootRecord, (Map<Object, Object>) map);
    } else {
      target = new HashMap();
    }
    OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName, target);
  }

  public void onBeforeArray(ODocument iRootRecord, String iFieldName, Object iUserObject, OIdentifiable[] iArray)
      throws OFetchException {
    OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName,
        Array.newInstance(iRootRecord.getSchemaClass().getProperty(iFieldName).getLinkedClass().getJavaClass(), iArray.length));
  }

  public void onAfterDocument(ODocument iRootRecord, ODocument iDocument, String iFieldName, Object iUserObject)
      throws OFetchException {
  }

  public void onBeforeDocument(ODocument iRecord, ODocument iDocument, String iFieldName, Object iUserObject)
      throws OFetchException {
  }

  public void onAfterArray(ODocument iRootRecord, String iFieldName, Object iUserObject) throws OFetchException {
  }

  public void onAfterMap(ODocument iRootRecord, String iFieldName, final Object iUserObject) throws OFetchException {
  }

  public void onBeforeDocument(ODocument iRecord, String iFieldName, final Object iUserObject) throws OFetchException {
  }

  public void onAfterDocument(ODocument iRootRecord, String iFieldName, final Object iUserObject) throws OFetchException {
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void onBeforeCollection(ODocument iRootRecord, String iFieldName, final Object iUserObject, final Iterable<?> iterable)
      throws OFetchException {
    if (iterable instanceof ORidBag)
      throw new IllegalStateException(OType.LINKBAG.name() + " cannot be directly mapped to any Java collection.");

    final Field f = OObjectEntitySerializer.getField(iFieldName, iUserObject.getClass());
    final boolean customSerialization = OObjectEntitySerializer.isSerializedType(f);
    final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
    Collection target;
    if (iterable instanceof ORecordLazyList
        || (iterable instanceof OTrackedList<?> && !OReflectionHelper.isJavaType(genericType) && !customSerialization && !genericType
            .isEnum())) {
      target = new OObjectLazyList(iUserObject, (List<OIdentifiable>) iterable, OObjectEntitySerializer.isCascadeDeleteField(
          iUserObject.getClass(), f.getName()));
    } else if (iterable instanceof ORecordLazySet
        || iterable instanceof OMVRBTreeRIDSet
        || (iterable instanceof OTrackedSet<?> && !OReflectionHelper.isJavaType(genericType) && !customSerialization && !genericType
            .isEnum())) {
      target = new OObjectLazySet(iUserObject, (Set) iterable, OObjectEntitySerializer.isCascadeDeleteField(iUserObject.getClass(),
          f.getName()));
    } else if (customSerialization) {
      if (iterable instanceof List<?>) {
        target = new OObjectCustomSerializerList(OObjectEntitySerializer.getSerializedType(f), iRootRecord, (List<Object>) iterable);
      } else {
        target = new OObjectCustomSerializerSet(OObjectEntitySerializer.getSerializedType(f), iRootRecord, (Set<Object>) iterable);
      }
    } else if (genericType.isEnum()) {
      if (iterable instanceof List<?>) {
        target = new OObjectEnumLazyList(genericType, iRootRecord, (List<Object>) iterable);
      } else {
        target = new OObjectEnumLazySet(genericType, iRootRecord, (Set<Object>) iterable);
      }
    } else {
      if (iterable instanceof List<?>) {
        target = new ArrayList();
      } else {
        target = new HashSet();
      }
    }
    OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName, target);
  }

  public void onAfterCollection(ODocument iRootRecord, String iFieldName, final Object iUserObject) throws OFetchException {
  }

  public void onAfterFetch(ODocument iRootRecord) throws OFetchException {
  }

  public void onBeforeFetch(ODocument iRootRecord) throws OFetchException {
  }

  public void onBeforeStandardField(Object iFieldValue, String iFieldName, Object iUserObject) {
  }

  public void onAfterStandardField(Object iFieldValue, String iFieldName, Object iUserObject) {
  }

  public OUserObject2RecordHandler getObj2RecHandler() {
    return obj2RecHandler;
  }

  public OEntityManager getEntityManager() {
    return entityManager;
  }

  public boolean isLazyLoading() {
    return lazyLoading;
  }

  public String getFetchPlan() {
    return fetchPlan;
  }

  public boolean fetchEmbeddedDocuments() {
    return true;
  }
}
