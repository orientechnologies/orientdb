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
package com.orientechnologies.orient.object.enhancement;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.reflection.OReflectionHelper;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.object.OLazyObjectMultivalueElement;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.orientechnologies.orient.object.db.OObjectLazyList;
import com.orientechnologies.orient.object.db.OObjectLazyMap;
import com.orientechnologies.orient.object.db.OObjectLazySet;
import com.orientechnologies.orient.object.enumerations.OLazyObjectEnumSerializer;
import com.orientechnologies.orient.object.enumerations.OObjectEnumLazyList;
import com.orientechnologies.orient.object.enumerations.OObjectEnumLazyMap;
import com.orientechnologies.orient.object.enumerations.OObjectEnumLazySet;
import com.orientechnologies.orient.object.serialization.OLazyObjectCustomSerializer;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerList;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerMap;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerSet;

/**
 * @author Luca Molino (molino.luca--at--gmail.com)
 * 
 */
public class OObjectProxyMethodHandler implements MethodHandler {

  protected ODocument            doc;

  protected ProxyObject          parentObject;

  protected Map<String, Integer> loadedFields;

  protected Set<ORID>            orphans = new HashSet<ORID>();

  public OObjectProxyMethodHandler(ODocument iDocument) {
    doc = iDocument;
    if (!((ODatabaseObject) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner()).isLazyLoading())
      doc.detach();
    loadedFields = new HashMap<String, Integer>();
  }

  public ODocument getDoc() {
    return doc;
  }

  public void setDoc(ODocument iDoc) {
    doc = iDoc;
  }

  public ProxyObject getParentObject() {
    return parentObject;
  }

  public void setParentObject(ProxyObject parentDoc) {
    this.parentObject = parentDoc;
  }

  public Set<ORID> getOrphans() {
    return orphans;
  }

  public Object invoke(Object self, Method m, Method proceed, Object[] args) throws Throwable {
    OObjectMethodFilter filter = OObjectEntityEnhancer.getInstance().getMethodFilter(self.getClass());
    if (filter.isSetterMethod(m.getName(), m)) {
      return manageSetMethod(self, m, proceed, args);
    } else if (filter.isGetterMethod(m.getName(), m)) {
      return manageGetMethod(self, m, proceed, args);
    }
    return proceed.invoke(self, args);
  }

  /**
   * Method that detaches all fields contained in the document to the given object
   * 
   * @param self
   *          :- The object containing this handler instance
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws NoSuchMethodException
   */
  public void detach(Object self, boolean nonProxiedInstance) throws NoSuchMethodException, IllegalAccessException,
      InvocationTargetException {
    for (String fieldName : doc.fieldNames()) {
      Object value = getValue(self, fieldName, false, null);
      if (value instanceof OLazyObjectMultivalueElement) {
        ((OLazyObjectMultivalueElement<?>) value).detach(nonProxiedInstance);
        if (nonProxiedInstance)
          value = ((OLazyObjectMultivalueElement<?>) value).getNonOrientInstance();
      }
      OObjectEntitySerializer.setFieldValue(OObjectEntitySerializer.getField(fieldName, self.getClass()), self, value);
    }
    OObjectEntitySerializer.setIdField(self.getClass(), self, doc.getIdentity());
    OObjectEntitySerializer.setVersionField(self.getClass(), self, doc.getVersion());
  }

  /**
   * Method that detaches all fields contained in the document to the given object
   * 
   * @param self
   *          :- The object containing this handler instance
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws NoSuchMethodException
   */
  public void detachAll(Object self, boolean nonProxiedInstance) throws NoSuchMethodException, IllegalAccessException,
      InvocationTargetException {
    for (String fieldName : doc.fieldNames()) {
      Field field = OObjectEntitySerializer.getField(fieldName, self.getClass());
      if (field != null) {
        Object value = getValue(self, fieldName, false, null);
        if (value instanceof OLazyObjectMultivalueElement) {
          ((OLazyObjectMultivalueElement<?>) value).detachAll(nonProxiedInstance);
          if (nonProxiedInstance)
            value = ((OLazyObjectMultivalueElement<?>) value).getNonOrientInstance();
        } else if (value instanceof Proxy) {
          OObjectProxyMethodHandler handler = (OObjectProxyMethodHandler) ((ProxyObject) value).getHandler();
          if (nonProxiedInstance) {
            value = OObjectEntitySerializer.getNonProxiedInstance(value);
          }
          handler.detachAll(value, nonProxiedInstance);
        }
        OObjectEntitySerializer.setFieldValue(field, self, value);
      }
    }
    OObjectEntitySerializer.setIdField(self.getClass(), self, doc.getIdentity());
    OObjectEntitySerializer.setVersionField(self.getClass(), self, doc.getVersion());
  }

  /**
   * Method that attaches all data contained in the object to the associated document
   * 
   * 
   * @param self
   *          :- The object containing this handler instance
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   * @throws InvocationTargetException
   * @throws NoSuchMethodException
   */
  public void attach(Object self) throws IllegalArgumentException, IllegalAccessException, NoSuchMethodException,
      InvocationTargetException {
    for (Class<?> currentClass = self.getClass(); currentClass != Object.class;) {
      if (Proxy.class.isAssignableFrom(currentClass)) {
        currentClass = currentClass.getSuperclass();
        continue;
      }
      for (Field f : currentClass.getDeclaredFields()) {
        if (OObjectEntitySerializer.isTransientField(f.getDeclaringClass(), f.getName())
            || OObjectEntitySerializer.isVersionField(f.getDeclaringClass(), f.getName())
            || OObjectEntitySerializer.isIdField(f.getDeclaringClass(), f.getName()))
          continue;
        Object value = OObjectEntitySerializer.getFieldValue(f, self);
        value = setValue(self, f.getName(), value);
        OObjectEntitySerializer.setFieldValue(f, self, value);
      }
      currentClass = currentClass.getSuperclass();

      if (currentClass == null || currentClass.equals(ODocument.class))
        // POJO EXTENDS ODOCUMENT: SPECIAL CASE: AVOID TO CONSIDER
        // ODOCUMENT FIELDS
        currentClass = Object.class;
    }
  }

  public void setDirty() {
    doc.setDirty();
    if (parentObject != null)
      ((OObjectProxyMethodHandler) parentObject.getHandler()).setDirty();
  }

  public void updateLoadedFieldMap() {
    Set<String> fields = new HashSet<String>(loadedFields.keySet());
    for (String key : fields) {
      loadedFields.put(key, doc.getVersion());
    }
    fields.clear();
    fields = null;
  }

  protected Object manageGetMethod(Object self, Method m, Method proceed, Object[] args) throws IllegalAccessException,
      InvocationTargetException, NoSuchMethodException, SecurityException, IllegalArgumentException, NoSuchFieldException {
    final String fieldName;
    fieldName = OObjectEntityEnhancer.getInstance().getMethodFilter(self.getClass()).getFieldName(m);
    boolean idOrVersionField = false;
    if (OObjectEntitySerializer.isIdField(m.getDeclaringClass(), fieldName)) {
      idOrVersionField = true;
      OObjectEntitySerializer.setIdField(m.getDeclaringClass(), self, (ORID) doc.getIdentity());
    } else if (OObjectEntitySerializer.isVersionField(m.getDeclaringClass(), fieldName)) {
      idOrVersionField = true;
      if (doc.getIdentity().isValid() && !doc.getIdentity().isTemporary())
        OObjectEntitySerializer.setVersionField(m.getDeclaringClass(), self, doc.getVersion());
    }
    Object value = proceed.invoke(self, args);

    value = getValue(self, fieldName, idOrVersionField, value);
    if (doc.getIdentity().isValid() && !doc.getIdentity().isTemporary())
      loadedFields.put(fieldName, doc.getVersion());
    else
      loadedFields.put(fieldName, 0);
    return value;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected Object getValue(Object self, final String fieldName, boolean idOrVersionField, Object value)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    if (!idOrVersionField) {
      if (value == null) {
        Object docValue = doc.field(fieldName, OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
        if (docValue != null) {
          value = lazyLoadField(self, fieldName, docValue, value);
        }
      } else {
        if (((value instanceof Collection<?> || value instanceof Map<?, ?>) && !(value instanceof OLazyObjectMultivalueElement))
            || value.getClass().isArray()) {
          Class<?> genericMultiValueType = OReflectionHelper.getGenericMultivalueType(OObjectEntitySerializer.getField(fieldName,
              self.getClass()));
          if (genericMultiValueType != null && !OReflectionHelper.isJavaType(genericMultiValueType)) {
            Field f = OObjectEntitySerializer.getField(fieldName, self.getClass());
            if (OObjectEntitySerializer.isSerializedType(f) && !(value instanceof OLazyObjectCustomSerializer)) {
              value = manageSerializedCollections(self, fieldName, value);
            } else if (genericMultiValueType.isEnum() && !(value instanceof OLazyObjectEnumSerializer)) {
              value = manageEnumCollections(self, f.getName(), genericMultiValueType, value);
            } else {
              value = manageObjectCollections(self, fieldName, value);
            }
          } else {
            Object docValue = doc.field(fieldName, OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
            if (docValue == null) {
              if (value.getClass().isArray()) {
                OClass schemaClass = doc.getSchemaClass();
                OProperty schemaProperty = null;
                if (schemaClass != null)
                  schemaProperty = schemaClass.getProperty(fieldName);

                doc.field(fieldName, OObjectEntitySerializer.typeToStream(value, schemaProperty != null ? schemaProperty.getType()
                    : null, getDatabase(), doc));
              } else
                doc.field(fieldName, value);

            } else if (!loadedFields.containsKey(fieldName)) {
              value = manageArrayFieldObject(OObjectEntitySerializer.getField(fieldName, self.getClass()), self, docValue);
              Method setMethod = getSetMethod(self.getClass().getSuperclass(), getSetterFieldName(fieldName), value);
              setMethod.invoke(self, value);
            } else if ((value instanceof Set || value instanceof Map) && loadedFields.get(fieldName).intValue() < doc.getVersion()) {
              if (value instanceof Set)
                value = new OObjectLazySet(self, (Set<?>) docValue, OObjectEntitySerializer.isCascadeDeleteField(self.getClass(),
                    fieldName));
              else
                value = new OObjectLazyMap(self, (Map<?, ?>) docValue, OObjectEntitySerializer.isCascadeDeleteField(
                    self.getClass(), fieldName));
              Method setMethod = getSetMethod(self.getClass().getSuperclass(), getSetterFieldName(fieldName), value);
              setMethod.invoke(self, value);
            }
          }
        } else if (!loadedFields.containsKey(fieldName)) {
          Object docValue = doc.field(fieldName, OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
          if (docValue != null && !docValue.equals(value)) {
            value = lazyLoadField(self, fieldName, docValue, value);
          }
        }
      }
    }
    return value;
  }

  protected Object manageObjectCollections(Object self, final String fieldName, Object value) throws NoSuchMethodException,
      IllegalAccessException, InvocationTargetException {
    boolean customSerialization = false;
    Field f = OObjectEntitySerializer.getField(fieldName, self.getClass());
    if (OObjectEntitySerializer.isSerializedType(f)) {
      customSerialization = true;
    }
    if (value instanceof Collection<?>) {
      value = manageCollectionSave(self, f, (Collection<?>) value, customSerialization);
    } else if (value instanceof Map<?, ?>) {
      value = manageMapSave(self, f, (Map<?, ?>) value, customSerialization);
    } else if (value.getClass().isArray()) {
      value = manageArraySave(fieldName, (Object[]) value);
    }
    OObjectEntitySerializer.setFieldValue(OObjectEntitySerializer.getField(fieldName, self.getClass()), self, value);
    return value;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected Object manageSerializedCollections(Object self, final String fieldName, Object value) throws NoSuchMethodException,
      IllegalAccessException, InvocationTargetException {
    if (value instanceof Collection<?>) {
      if (value instanceof List) {
        List<Object> docList = doc.field(fieldName, OType.EMBEDDEDLIST);
        if (docList == null) {
          docList = new ArrayList<Object>();
          doc.field(fieldName, docList, OType.EMBEDDEDLIST);
        }
        value = new OObjectCustomSerializerList(OObjectEntitySerializer.getSerializedType(OObjectEntitySerializer.getField(
            fieldName, self.getClass())), doc, docList, (List<?>) value);
      } else if (value instanceof Set) {
        Set<Object> docSet = doc.field(fieldName, OType.EMBEDDEDSET);
        if (docSet == null) {
          docSet = new HashSet<Object>();
          doc.field(fieldName, docSet, OType.EMBEDDEDSET);
        }
        value = new OObjectCustomSerializerSet(OObjectEntitySerializer.getSerializedType(OObjectEntitySerializer.getField(
            fieldName, self.getClass())), doc, docSet, (Set<?>) value);
      }
    } else if (value instanceof Map<?, ?>) {
      Map<Object, Object> docMap = doc.field(fieldName, OType.EMBEDDEDMAP);
      if (docMap == null) {
        docMap = new HashMap<Object, Object>();
        doc.field(fieldName, docMap, OType.EMBEDDEDMAP);
      }
      value = new OObjectCustomSerializerMap(OObjectEntitySerializer.getSerializedType(OObjectEntitySerializer.getField(fieldName,
          self.getClass())), doc, docMap, (Map<?, ?>) value);
    } else if (value.getClass().isArray()) {
      value = manageArraySave(fieldName, (Object[]) value);
    }
    OObjectEntitySerializer.setFieldValue(OObjectEntitySerializer.getField(fieldName, self.getClass()), self, value);
    return value;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected Object manageEnumCollections(Object self, final String fieldName, final Class<?> enumClass, Object value)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    if (value instanceof Collection<?>) {
      if (value instanceof List) {
        List<Object> docList = doc.field(fieldName, OType.EMBEDDEDLIST);
        if (docList == null) {
          docList = new ArrayList<Object>();
          doc.field(fieldName, docList, OType.EMBEDDEDLIST);
        }
        value = new OObjectEnumLazyList(enumClass, doc, docList, (List<?>) value);
      } else if (value instanceof Set) {
        Set<Object> docSet = doc.field(fieldName, OType.EMBEDDEDSET);
        if (docSet == null) {
          docSet = new HashSet<Object>();
          doc.field(fieldName, docSet, OType.EMBEDDEDSET);
        }
        value = new OObjectEnumLazySet(enumClass, doc, docSet, (Set<?>) value);
      }
    } else if (value instanceof Map<?, ?>) {
      Map<Object, Object> docMap = doc.field(fieldName, OType.EMBEDDEDMAP);
      if (docMap == null) {
        docMap = new HashMap<Object, Object>();
        doc.field(fieldName, docMap, OType.EMBEDDEDMAP);
      }
      value = new OObjectEnumLazyMap(enumClass, doc, docMap, (Map<?, ?>) value);
    } else if (value.getClass().isArray()) {
      value = manageArraySave(fieldName, (Object[]) value);
    }
    OObjectEntitySerializer.setFieldValue(OObjectEntitySerializer.getField(fieldName, self.getClass()), self, value);
    return value;
  }

  protected Object manageArraySave(String iFieldName, Object[] value) {
    if (value.length > 0) {
      Object o = ((Object[]) value)[0];
      if (o instanceof Proxy) {
        ODocument[] newValue = new ODocument[value.length];
        for (int i = 0; i < value.length; i++) {
          newValue[i] = value[i] != null ? OObjectEntitySerializer.getDocument((Proxy) value[i]) : null;
        }
        doc.field(iFieldName, newValue);
      }
    }
    return value;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected Object manageMapSave(Object self, Field f, Map<?, ?> value, boolean customSerialization) {
    final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
    if (customSerialization) {
      Map<Object, Object> map = new HashMap<Object, Object>();
      doc.field(f.getName(), map, OType.EMBEDDEDMAP);
      value = new OObjectCustomSerializerMap<TYPE>(OObjectEntitySerializer.getSerializedType(f), doc, map,
          (Map<Object, Object>) value);
    } else if (genericType.isEnum()) {
      Map<Object, Object> map = new HashMap<Object, Object>();
      doc.field(f.getName(), map, OType.EMBEDDEDMAP);
      value = new OObjectEnumLazyMap(genericType, doc, map, (Map<Object, Object>) value);
    } else if (!(value instanceof OLazyObjectMultivalueElement)) {
      Map<Object, OIdentifiable> docMap = doc.field(f.getName(), OType.LINKMAP);
      if (docMap == null) {
        docMap = new ORecordLazyMap(doc);
        doc.field(f.getName(), docMap, OType.LINKMAP);
      }
      value = new OObjectLazyMap(self, docMap, value, OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), f.getName()));
    }
    return value;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected Object manageCollectionSave(Object self, Field f, Collection<?> value, boolean customSerialization) {
    final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
    if (customSerialization) {
      if (value instanceof List<?>) {
        List<Object> list = new ArrayList<Object>();
        doc.field(f.getName(), list, OType.EMBEDDEDLIST);
        value = new OObjectCustomSerializerList(OObjectEntitySerializer.getSerializedType(f), doc, new ArrayList<Object>(),
            (List<Object>) value);
      } else {
        Set<Object> set = new HashSet<Object>();
        doc.field(f.getName(), set, OType.EMBEDDEDSET);
        value = new OObjectCustomSerializerSet(OObjectEntitySerializer.getSerializedType(f), doc, set, (Set<Object>) value);
      }
    } else if (genericType.isEnum()) {
      if (value instanceof List<?>) {
        List<Object> list = new ArrayList<Object>();
        doc.field(f.getName(), list, OType.EMBEDDEDLIST);
        value = new OObjectEnumLazyList(genericType, doc, list, (List<Object>) value);
      } else {
        Set<Object> set = new HashSet<Object>();
        doc.field(f.getName(), set, OType.EMBEDDEDSET);
        value = new OObjectEnumLazySet(genericType, doc, set, (Set<Object>) value);
      }
    } else if (!(value instanceof OLazyObjectMultivalueElement)) {
      if (value instanceof List) {
        List<OIdentifiable> docList = doc.field(f.getName(), OType.LINKLIST);
        if (docList == null) {
          docList = new ORecordLazyList(doc);
          doc.field(f.getName(), docList, OType.LINKLIST);
        }
        value = new OObjectLazyList(self, docList, value,
            OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), f.getName()));
      } else if (value instanceof Set) {
        Set<OIdentifiable> docSet = doc.field(f.getName(), OType.LINKSET);
        if (docSet == null) {
          docSet = new ORecordLazySet(doc);
          doc.field(f.getName(), docSet, OType.LINKSET);
        }
        value = new OObjectLazySet(self, docSet, (Set<?>) value, OObjectEntitySerializer.isCascadeDeleteField(self.getClass(),
            f.getName()));
      }
    }
    if (!((ODatabaseObject) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner()).isLazyLoading())
      ((OLazyObjectMultivalueElement) value).detach(false);
    return value;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected Object lazyLoadField(Object self, final String fieldName, Object docValue, Object currentValue)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    boolean customSerialization = false;
    Field f = OObjectEntitySerializer.getField(fieldName, self.getClass());
    if (f == null)
      return currentValue;
    if (OObjectEntitySerializer.isSerializedType(f)) {
      customSerialization = true;
    }
    if (docValue instanceof OIdentifiable) {
      if (OIdentifiable.class.isAssignableFrom(f.getType())) {
        if (ORecordAbstract.class.isAssignableFrom(f.getType())) {
          ORecordAbstract record = ((OIdentifiable) docValue).getRecord();
          OObjectEntitySerializer.setFieldValue(f, self, record);
          return record;
        } else {
          OObjectEntitySerializer.setFieldValue(f, self, docValue);
          return docValue;
        }
      } else {
        docValue = convertDocumentToObject((ODocument) ((OIdentifiable) docValue).getRecord(), self);
      }
    } else if (docValue instanceof Collection<?>) {
      docValue = manageCollectionLoad(f, self, docValue, customSerialization);
    } else if (docValue instanceof Map<?, ?>) {
      docValue = manageMapLoad(f, self, docValue, customSerialization);
    } else if (docValue.getClass().isArray() && !docValue.getClass().getComponentType().isPrimitive()) {
      docValue = manageArrayLoad(docValue);
    } else if (customSerialization) {
      docValue = OObjectEntitySerializer.deserializeFieldValue(OObjectEntitySerializer.getField(fieldName, self.getClass())
          .getType(), docValue);
    } else {
      if (f.getType().isEnum()) {
        if (docValue instanceof Number)
          docValue = ((Class<Enum>) f.getType()).getEnumConstants()[((Number) docValue).intValue()];
        else
          docValue = Enum.valueOf((Class<Enum>) f.getType(), docValue.toString());
      }
    }
    OObjectEntitySerializer.setFieldValue(f, self, docValue);
    return docValue;
  }

  protected Object manageArrayLoad(Object value) {
    if (((Object[]) value).length > 0) {
      Object o = ((Object[]) value)[0];
      if (o instanceof OIdentifiable) {
        Object[] newValue = new Object[((Object[]) value).length];
        for (int i = 0; i < ((Object[]) value).length; i++) {
          ODocument doc = ((OIdentifiable) ((Object[]) value)[i]).getRecord();
          newValue[i] = OObjectEntitySerializer.getDocument((Proxy) doc);
        }
        value = newValue;
      }
    }
    return value;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected Object manageMapLoad(Field f, Object self, Object value, boolean customSerialization) {
    final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
    if (value instanceof ORecordLazyMap) {
      value = new OObjectLazyMap(self, (ORecordLazyMap) value, OObjectEntitySerializer.isCascadeDeleteField(self.getClass(),
          f.getName()));
    } else if (customSerialization) {
      value = new OObjectCustomSerializerMap<TYPE>(OObjectEntitySerializer.getSerializedType(f), doc, (Map<Object, Object>) value);
    } else if (genericType.isEnum()) {
      value = new OObjectEnumLazyMap(genericType, doc, (Map<Object, Object>) value);
    }
    return value;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected Object manageCollectionLoad(Field f, Object self, Object value, boolean customSerialization) {
    final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
    if (value instanceof ORecordLazyList) {
      value = new OObjectLazyList(self, (ORecordLazyList) value, OObjectEntitySerializer.isCascadeDeleteField(self.getClass(),
          f.getName()));
    } else if (value instanceof ORecordLazySet || value instanceof OMVRBTreeRIDSet) {
      value = new OObjectLazySet(self, (Set) value, OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), f.getName()));
    } else if (customSerialization) {
      if (value instanceof List<?>) {
        value = new OObjectCustomSerializerList(OObjectEntitySerializer.getSerializedType(f), doc, (List<Object>) value);
      } else {
        value = new OObjectCustomSerializerSet(OObjectEntitySerializer.getSerializedType(f), doc, (Set<Object>) value);
      }
    } else if (genericType.isEnum()) {
      if (value instanceof List<?>) {
        value = new OObjectEnumLazyList(genericType, doc, (List<Object>) value);
      } else {
        value = new OObjectEnumLazySet(genericType, doc, (Set<Object>) value);
      }
    }

    return manageArrayFieldObject(f, self, value);
  }

  protected Object manageArrayFieldObject(Field field, Object self, Object value) {
    if (field.getType().isArray()) {
      final Collection<?> collectionValue = ((Collection<?>) value);
      final Object newArray = Array.newInstance(field.getType().getComponentType(), collectionValue.size());
      int i = 0;
      for (final Object collectionItem : collectionValue) {
        Array.set(newArray, i, collectionItem);
        i++;
      }

      return newArray;
    } else
      return value;
  }

  protected Object convertDocumentToObject(ODocument value, Object self) {
    return OObjectEntityEnhancer.getInstance().getProxiedInstance(value.getClassName(), getDatabase().getEntityManager(), value,
        (self instanceof ProxyObject ? (ProxyObject) self : null));
  }

  protected Object manageSetMethod(Object self, Method m, Method proceed, Object[] args) throws IllegalAccessException,
      InvocationTargetException {
    final String fieldName;
    fieldName = OObjectEntityEnhancer.getInstance().getMethodFilter(self.getClass()).getFieldName(m);
    args[0] = setValue(self, fieldName, args[0]);
    return proceed.invoke(self, args);
  }

  @SuppressWarnings("rawtypes")
  protected Object setValue(Object self, final String fieldName, Object valueToSet) {
    if (valueToSet == null) {
      Object oldValue = doc.field(fieldName);
      if (OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), fieldName) && oldValue instanceof OIdentifiable)
        orphans.add(((OIdentifiable) oldValue).getIdentity());
      doc.field(fieldName, valueToSet, OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
    } else if (!valueToSet.getClass().isAnonymousClass()) {
      if (valueToSet instanceof Proxy) {
        ODocument docToSet = OObjectEntitySerializer.getDocument((Proxy) valueToSet);
        if (OObjectEntitySerializer.isEmbeddedField(self.getClass(), fieldName))
          docToSet.addOwner(doc);
        Object oldValue = doc.field(fieldName);
        if (OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), fieldName) && oldValue instanceof OIdentifiable)
          orphans.add(((OIdentifiable) oldValue).getIdentity());
        doc.field(fieldName, docToSet, OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
      } else if (valueToSet instanceof OIdentifiable) {
        if (valueToSet instanceof ODocument && OObjectEntitySerializer.isEmbeddedField(self.getClass(), fieldName))
          ((ODocument) valueToSet).addOwner(doc);
        Object oldValue = doc.field(fieldName);
        if (OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), fieldName) && oldValue instanceof OIdentifiable)
          orphans.add(((OIdentifiable) oldValue).getIdentity());
        doc.field(fieldName, valueToSet);
      } else if (((valueToSet instanceof Collection<?> || valueToSet instanceof Map<?, ?>)) || valueToSet.getClass().isArray()) {
        Class<?> genericMultiValueType = OReflectionHelper.getGenericMultivalueType(OObjectEntitySerializer.getField(fieldName,
            self.getClass()));
        if (genericMultiValueType != null && !OReflectionHelper.isJavaType(genericMultiValueType)) {
          if (!(valueToSet instanceof OLazyObjectMultivalueElement)) {
            if (valueToSet instanceof Collection<?>) {
              boolean customSerialization = false;
              Field f = OObjectEntitySerializer.getField(fieldName, self.getClass());
              if (OObjectEntitySerializer.isSerializedType(f)) {
                customSerialization = true;
              }
              valueToSet = manageCollectionSave(self, f, (Collection<?>) valueToSet, customSerialization);
            } else if (valueToSet instanceof Map<?, ?>) {
              boolean customSerialization = false;
              Field f = OObjectEntitySerializer.getField(fieldName, self.getClass());
              if (OObjectEntitySerializer.isSerializedType(f)) {
                customSerialization = true;
              }
              valueToSet = manageMapSave(self, f, (Map<?, ?>) valueToSet, customSerialization);
            } else if (valueToSet.getClass().isArray()) {
              valueToSet = manageArraySave(fieldName, (Object[]) valueToSet);
            }
          }
        } else {
          if (OObjectEntitySerializer.isToSerialize(valueToSet.getClass())) {
            doc.field(fieldName, OObjectEntitySerializer.serializeFieldValue(
                OObjectEntitySerializer.getField(fieldName, self.getClass()).getType(), valueToSet));
          } else {
            if (valueToSet.getClass().isArray()) {
              OClass schemaClass = doc.getSchemaClass();
              OProperty schemaProperty = null;
              if (schemaClass != null)
                schemaProperty = schemaClass.getProperty(fieldName);

              doc.field(fieldName, OObjectEntitySerializer.typeToStream(valueToSet,
                  schemaProperty != null ? schemaProperty.getType() : null, getDatabase(), doc));
            } else
              doc.field(fieldName, valueToSet, OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
          }
        }
      } else if (valueToSet.getClass().isEnum()) {
        doc.field(fieldName, ((Enum) valueToSet).name());
      } else {
        if (OObjectEntitySerializer.isToSerialize(valueToSet.getClass())) {
          doc.field(fieldName, OObjectEntitySerializer.serializeFieldValue(
              OObjectEntitySerializer.getField(fieldName, self.getClass()).getType(), valueToSet));
        } else if (getDatabase().getEntityManager().getEntityClass(valueToSet.getClass().getSimpleName()) != null
            && !valueToSet.getClass().isEnum()) {
          valueToSet = OObjectEntitySerializer.serializeObject(valueToSet, getDatabase());
          ODocument docToSet = OObjectEntitySerializer.getDocument((Proxy) valueToSet);
          if (OObjectEntitySerializer.isEmbeddedField(self.getClass(), fieldName))
            docToSet.addOwner(doc);
          doc.field(fieldName, docToSet);
        } else {
          doc.field(fieldName, valueToSet, OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
        }
      }
      loadedFields.put(fieldName, doc.getVersion());
      setDirty();
    } else {
      OLogManager.instance().warn(this,
          "Setting property '%s' in proxied class '%s' with an anonymous class '%s'. The document won't have this property.",
          fieldName, self.getClass().getName(), valueToSet.getClass().getName());
    }
    return valueToSet;
  }

  protected String getSetterFieldName(String fieldName) {
    StringBuffer methodName = new StringBuffer("set");
    methodName.append(Character.toUpperCase(fieldName.charAt(0)));
    for (int i = 1; i < fieldName.length(); i++) {
      methodName.append(fieldName.charAt(i));
    }
    return methodName.toString();
  }

  protected Method getSetMethod(Class<?> iClass, final String fieldName, Object value) throws NoSuchMethodException {
    for (Method m : iClass.getDeclaredMethods()) {
      if (m.getName().equals(fieldName)) {
        if (m.getParameterTypes().length == 1)
          if (m.getParameterTypes()[0].isAssignableFrom(value.getClass()))
            return m;
      }
    }
    if (iClass.getSuperclass().equals(Object.class))
      return null;
    return getSetMethod(iClass.getSuperclass(), fieldName, value);
  }

  private ODatabaseObject getDatabase() {
    return (ODatabaseObject) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();
  }
}
