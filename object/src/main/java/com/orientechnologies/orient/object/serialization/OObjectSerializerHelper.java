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
package com.orientechnologies.orient.object.serialization;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.reflection.OReflectionHelper;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.annotation.OAccess;
import com.orientechnologies.orient.core.annotation.OAfterDeserialization;
import com.orientechnologies.orient.core.annotation.OAfterSerialization;
import com.orientechnologies.orient.core.annotation.OBeforeDeserialization;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.annotation.OId;
import com.orientechnologies.orient.core.annotation.OVersion;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.object.db.OObjectLazyList;
import com.orientechnologies.orient.object.db.OObjectLazyMap;
import com.orientechnologies.orient.object.db.OObjectNotDetachedException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@SuppressWarnings("unchecked")
/**
 * Helper class to manage POJO by using the reflection.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @author Luca Molino
 * @author Jacques Desodt
 */
public class OObjectSerializerHelper {
  public static final Class<?>[] callbackAnnotationClasses =
      new Class[] {
        OBeforeDeserialization.class,
        OAfterDeserialization.class,
        OBeforeSerialization.class,
        OAfterSerialization.class
      };
  private static final Class<?>[] NO_ARGS = new Class<?>[] {};
  private static final HashMap<String, List<Field>> classes = new HashMap<String, List<Field>>();
  private static final HashMap<String, Method> callbacks = new HashMap<String, Method>();
  private static final HashMap<String, Object> getters = new HashMap<String, Object>();
  private static final HashMap<String, Object> setters = new HashMap<String, Object>();
  private static final HashMap<Class<?>, Field> boundDocumentFields =
      new HashMap<Class<?>, Field>();
  private static final HashMap<Class<?>, Field> fieldIds = new HashMap<Class<?>, Field>();
  private static final HashMap<Class<?>, Field> fieldVersions = new HashMap<Class<?>, Field>();
  private static final HashMap<Class<?>, List<String>> embeddedFields =
      new HashMap<Class<?>, List<String>>();
  public static HashMap<Class<?>, OObjectSerializerContext> serializerContexts =
      new LinkedHashMap<Class<?>, OObjectSerializerContext>();

  @SuppressWarnings("rawtypes")
  public static Class jpaIdClass;

  @SuppressWarnings("rawtypes")
  public static Class jpaVersionClass;

  @SuppressWarnings("rawtypes")
  public static Class jpaAccessClass;

  @SuppressWarnings("rawtypes")
  public static Class jpaEmbeddedClass;

  @SuppressWarnings("rawtypes")
  public static Class jpaTransientClass;

  @SuppressWarnings("rawtypes")
  public static Class jpaOneToOneClass;

  @SuppressWarnings("rawtypes")
  public static Class jpaOneToManyClass;

  @SuppressWarnings("rawtypes")
  public static Class jpaManyToManyClass;

  static {
    try {
      // DETERMINE IF THERE IS AVAILABLE JPA 1
      jpaIdClass = Class.forName("javax.persistence.Id");
      jpaVersionClass = Class.forName("javax.persistence.Version");
      jpaEmbeddedClass = Class.forName("javax.persistence.Embedded");
      jpaTransientClass = Class.forName("javax.persistence.Transient");
      jpaOneToOneClass = Class.forName("javax.persistence.OneToOne");
      jpaOneToManyClass = Class.forName("javax.persistence.OneToMany");
      jpaManyToManyClass = Class.forName("javax.persistence.ManyToMany");
      // DETERMINE IF THERE IS AVAILABLE JPA 2
      jpaAccessClass = Class.forName("javax.persistence.Access");

    } catch (Exception e) {
      // IGNORE THE EXCEPTION: JPA NOT FOUND
    }
  }

  public static boolean hasField(final Object iPojo, final String iProperty) {
    final Class<?> c = iPojo.getClass();
    final String className = c.getName();

    getClassFields(c);

    return getters.get(className + "." + iProperty) != null;
  }

  public static String getDocumentBoundField(final Class<?> iClass) {
    getClassFields(iClass);
    final Field f = boundDocumentFields.get(iClass);
    return f != null ? f.getName() : null;
  }

  public static Class<?> getFieldType(final Object iPojo, final String iProperty) {
    final Class<?> c = iPojo.getClass();
    final String className = c.getName();

    getClassFields(c);

    try {
      final Object o = getters.get(className + "." + iProperty);

      if (o == null) return null;
      else if (o instanceof Field) return ((Field) o).getType();
      else return ((Method) o).getReturnType();
    } catch (Exception e) {
      throw OException.wrapException(
          new OSchemaException("Cannot get the value of the property: " + iProperty), e);
    }
  }

  public static Class<?> getFieldType(ODocument iDocument, final OEntityManager iEntityManager) {
    if (iDocument.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
      iDocument = (ODocument) iDocument.load();

    if (iDocument.getClassName() == null) {
      return null;
    } else {
      return iEntityManager.getEntityClass(iDocument.getClassName());
    }
  }

  public static Object getFieldValue(final Object iPojo, final String iProperty) {
    final Class<?> c = iPojo.getClass();
    final String className = c.getName();

    getClassFields(c);

    try {
      Object o = getters.get(className + "." + iProperty);

      if (o instanceof Method) return ((Method) o).invoke(iPojo);
      else if (o instanceof Field) return ((Field) o).get(iPojo);
      return null;
    } catch (Exception e) {
      throw OException.wrapException(
          new OSchemaException("Cannot get the value of the property: " + iProperty), e);
    }
  }

  public static void setFieldValue(
      final Object iPojo, final String iProperty, final Object iValue) {
    final Class<?> c = iPojo.getClass();
    final String className = c.getName();

    getClassFields(c);

    try {
      Object o = setters.get(className + "." + iProperty);

      if (o instanceof Method) {
        ((Method) o)
            .invoke(
                iPojo,
                OObjectSerializerHelper.convertInObject(
                    iPojo, iProperty, iValue, ((Method) o).getParameterTypes()[0]));
      } else if (o instanceof Field) {
        ((Field) o).set(iPojo, OType.convert(iValue, ((Field) o).getType()));
      }

    } catch (Exception e) {

      throw OException.wrapException(
          new OSchemaException(
              "Cannot set the value '"
                  + iValue
                  + "' to the property '"
                  + iProperty
                  + "' for the pojo: "
                  + iPojo),
          e);
    }
  }

  public static String setObjectID(final ORID iIdentity, final Object iPojo) {
    if (iPojo == null) return null;

    final Class<?> pojoClass = iPojo.getClass();

    getClassFields(pojoClass);

    final Field idField = fieldIds.get(pojoClass);
    if (idField != null) {
      Class<?> fieldType = idField.getType();

      final String idFieldName = idField.getName();

      if (ORID.class.isAssignableFrom(fieldType)) setFieldValue(iPojo, idFieldName, iIdentity);
      else if (Number.class.isAssignableFrom(fieldType))
        setFieldValue(
            iPojo, idFieldName, iIdentity != null ? iIdentity.getClusterPosition() : null);
      else if (fieldType.equals(String.class))
        setFieldValue(iPojo, idFieldName, iIdentity != null ? iIdentity.toString() : null);
      else if (fieldType.equals(Object.class)) setFieldValue(iPojo, idFieldName, iIdentity);
      else
        OLogManager.instance()
            .warn(
                OObjectSerializerHelper.class,
                "@Id field has been declared as %s while the supported are: ORID, Number, String, Object",
                fieldType);
      return idFieldName;
    }
    return null;
  }

  public static ORecordId getObjectID(final ODatabaseObject iDb, final Object iPojo) {
    getClassFields(iPojo.getClass());

    final Field idField = fieldIds.get(iPojo.getClass());
    if (idField != null) {
      final Object id = getFieldValue(iPojo, idField.getName());

      if (id != null) {
        // FOUND
        if (id instanceof ORecordId) {
          return (ORecordId) id;
        } else if (id instanceof Number) {
          // TREATS AS CLUSTER POSITION
          final OClass cls = iDb.getMetadata().getSchema().getClass(iPojo.getClass());
          if (cls == null)
            throw new OConfigurationException(
                "Class " + iPojo.getClass() + " is not managed by current database");

          return new ORecordId(cls.getDefaultClusterId(), ((Number) id).longValue());
        } else if (id instanceof String) return new ORecordId((String) id);
      }
    }
    return null;
  }

  public static String getObjectIDFieldName(final Object iPojo) {
    getClassFields(iPojo.getClass());

    final Field idField = fieldIds.get(iPojo.getClass());
    if (idField != null) {
      return idField.getName();
    }
    return null;
  }

  public static boolean hasObjectID(final Object iPojo) {
    getClassFields(iPojo.getClass());
    return fieldIds.get(iPojo.getClass()) != null;
  }

  public static String setObjectVersion(final int iVersion, final Object iPojo) {
    if (iPojo == null) return null;

    final Class<?> pojoClass = iPojo.getClass();
    getClassFields(pojoClass);

    final Field vField = fieldVersions.get(pojoClass);
    if (vField != null) {
      Class<?> fieldType = vField.getType();

      final String vFieldName = vField.getName();

      if (Integer.TYPE.isAssignableFrom(fieldType)) {
        setFieldValue(iPojo, vFieldName, iVersion);
      } else if (Number.class.isAssignableFrom(fieldType)) {
        setFieldValue(iPojo, vFieldName, iVersion);
      } else if (fieldType.equals(String.class))
        setFieldValue(iPojo, vFieldName, String.valueOf(iVersion));
      else
        OLogManager.instance()
            .warn(
                OObjectSerializerHelper.class,
                "@Version field has been declared as %s while the supported are: Number, String, Object",
                fieldType);
      return vFieldName;
    }
    return null;
  }

  public static int getObjectVersion(final Object iPojo) {
    getClassFields(iPojo.getClass());
    final Field idField = fieldVersions.get(iPojo.getClass());
    if (idField != null) {
      final Object ver = getFieldValue(iPojo, idField.getName());

      return convertVersion(ver);
    }
    throw new OObjectNotDetachedException(
        "Cannot retrieve the object's VERSION for '" + iPojo + "' because has not been detached");
  }

  private static int convertVersion(final Object ver) {
    if (ver != null) {
      if (ver instanceof Number) {
        // TREATS AS CLUSTER POSITION
        return ((Number) ver).intValue();

      } else if (ver instanceof String) {
        return Integer.parseInt((String) ver);
      } else
        OLogManager.instance()
            .warn(
                OObjectSerializerHelper.class,
                "@Version field has been declared as %s while the supported are: Number, String, Object",
                ver.getClass());
    }
    return -1;
  }

  public static String getObjectVersionFieldName(final Object iPojo) {
    getClassFields(iPojo.getClass());

    final Field idField = fieldVersions.get(iPojo.getClass());
    if (idField != null) {
      return idField.getName();
    }
    return null;
  }

  public static boolean hasObjectVersion(final Object iPojo) {
    getClassFields(iPojo.getClass());
    return fieldVersions.get(iPojo.getClass()) != null;
  }

  /**
   * Serialize the user POJO to a ORecordDocument instance.
   *
   * @param iPojo User pojo to serialize
   * @param iRecord Record where to update
   * @param iObj2RecHandler
   */
  public static ODocument toStream(
      final Object iPojo,
      final ODocument iRecord,
      final OEntityManager iEntityManager,
      final OClass schemaClass,
      final OUserObject2RecordHandler iObj2RecHandler,
      final ODatabaseObject db,
      final boolean iSaveOnlyDirty) {
    if (iSaveOnlyDirty && !iRecord.isDirty()) return iRecord;

    final long timer = Orient.instance().getProfiler().startChrono();

    final Integer identityRecord = System.identityHashCode(iRecord);

    if (OSerializationThreadLocal.INSTANCE.get().contains(identityRecord)) return iRecord;

    OSerializationThreadLocal.INSTANCE.get().add(identityRecord);

    OProperty schemaProperty;

    final Class<?> pojoClass = iPojo.getClass();

    final List<Field> properties = getClassFields(pojoClass);

    // CHECK FOR ID BINDING
    final Field idField = fieldIds.get(pojoClass);
    if (idField != null) {
      Object id = getFieldValue(iPojo, idField.getName());
      if (id != null) {
        // FOUND
        if (id instanceof ORecordId) {
          ORecordInternal.setIdentity(iRecord, (ORecordId) id);
        } else if (id instanceof Number) {
          // TREATS AS CLUSTER POSITION
          ((ORecordId) iRecord.getIdentity()).setClusterId(schemaClass.getDefaultClusterId());
          ((ORecordId) iRecord.getIdentity()).setClusterPosition(((Number) id).longValue());
        } else if (id instanceof String)
          ((ORecordId) iRecord.getIdentity()).fromString((String) id);
        else if (id.getClass().equals(Object.class))
          ORecordInternal.setIdentity(iRecord, (ORecordId) id);
        else
          OLogManager.instance()
              .warn(
                  OObjectSerializerHelper.class,
                  "@Id field has been declared as %s while the supported are: ORID, Number, String, Object",
                  id.getClass());
      }
    }

    // CHECK FOR VERSION BINDING
    final Field vField = fieldVersions.get(pojoClass);
    boolean versionConfigured = false;
    if (vField != null) {
      versionConfigured = true;
      Object ver = getFieldValue(iPojo, vField.getName());

      final int version = convertVersion(ver);
      ORecordInternal.setVersion(iRecord, version);
    }

    if (db.isMVCC() && !versionConfigured && db.getTransaction() instanceof OTransactionOptimistic)
      throw new OTransactionException(
          "Cannot involve an object of class '"
              + pojoClass
              + "' in an Optimistic Transaction commit because it does not define @Version or @OVersion and therefore cannot handle MVCC");

    // SET OBJECT CLASS
    iRecord.setClassName(schemaClass != null ? schemaClass.getName() : null);

    String fieldName;
    Object fieldValue;

    // CALL BEFORE MARSHALLING
    invokeCallback(iPojo, iRecord, OBeforeSerialization.class);

    for (Field p : properties) {
      fieldName = p.getName();

      if (idField != null && fieldName.equals(idField.getName())) continue;

      if (vField != null && fieldName.equals(vField.getName())) continue;

      fieldValue =
          serializeFieldValue(getFieldType(iPojo, fieldName), getFieldValue(iPojo, fieldName));

      schemaProperty = schemaClass != null ? schemaClass.getProperty(fieldName) : null;

      if (fieldValue != null) {
        if (isEmbeddedObject(iPojo.getClass(), fieldValue.getClass(), fieldName, iEntityManager)) {
          // AUTO CREATE SCHEMA PROPERTY
          if (schemaClass == null) {
            db.getMetadata().getSchema().createClass(iPojo.getClass());
            iRecord.setClassNameIfExists(iPojo.getClass().getSimpleName());
          }

          if (schemaProperty == null) {
            OType t = OType.getTypeByClass(fieldValue.getClass());
            if (t == null) t = OType.EMBEDDED;
            schemaProperty = iRecord.getSchemaClass().createProperty(fieldName, t);
          }
        }
      }

      fieldValue =
          typeToStream(
              fieldValue,
              schemaProperty != null ? schemaProperty.getType() : null,
              iEntityManager,
              iObj2RecHandler,
              db,
              iRecord,
              iSaveOnlyDirty);

      iRecord.field(fieldName, fieldValue);
    }

    iObj2RecHandler.registerUserObject(iPojo, iRecord);

    // CALL AFTER MARSHALLING
    invokeCallback(iPojo, iRecord, OAfterSerialization.class);

    OSerializationThreadLocal.INSTANCE.get().remove(identityRecord);

    Orient.instance()
        .getProfiler()
        .stopChrono("Object.toStream", "Serialize object to stream", timer);

    return iRecord;
  }

  public static Object serializeFieldValue(final Class<?> type, final Object iFieldValue) {
    for (Class<?> classContext : serializerContexts.keySet()) {
      if (classContext != null && classContext.isAssignableFrom(type)) {
        return serializerContexts.get(classContext).serializeFieldValue(type, iFieldValue);
      }
    }

    if (serializerContexts.get(null) != null)
      return serializerContexts.get(null).serializeFieldValue(type, iFieldValue);

    return iFieldValue;
  }

  public static Object unserializeFieldValue(final Class<?> type, final Object iFieldValue) {
    for (Class<?> classContext : serializerContexts.keySet()) {
      if (classContext != null && classContext.isAssignableFrom(type)) {
        return serializerContexts.get(classContext).unserializeFieldValue(type, iFieldValue);
      }
    }

    if (serializerContexts.get(null) != null)
      return serializerContexts.get(null).unserializeFieldValue(type, iFieldValue);

    return iFieldValue;
  }

  private static Object typeToStream(
      Object iFieldValue,
      OType iType,
      final OEntityManager iEntityManager,
      final OUserObject2RecordHandler iObj2RecHandler,
      final ODatabaseObject db,
      final ODocument iRecord,
      final boolean iSaveOnlyDirty) {
    if (iFieldValue == null) return null;

    if (!OType.isSimpleType(iFieldValue)) {
      Class<?> fieldClass = iFieldValue.getClass();

      if (fieldClass.isArray()) {
        // ARRAY
        iFieldValue =
            multiValueToStream(
                Arrays.asList(iFieldValue),
                iType,
                iEntityManager,
                iObj2RecHandler,
                db,
                iRecord,
                iSaveOnlyDirty);
      } else if (Collection.class.isAssignableFrom(fieldClass)) {
        // COLLECTION (LIST OR SET)
        iFieldValue =
            multiValueToStream(
                iFieldValue, iType, iEntityManager, iObj2RecHandler, db, iRecord, iSaveOnlyDirty);
      } else if (Map.class.isAssignableFrom(fieldClass)) {
        // MAP
        iFieldValue =
            multiValueToStream(
                iFieldValue, iType, iEntityManager, iObj2RecHandler, db, iRecord, iSaveOnlyDirty);
      } else if (fieldClass.isEnum()) {
        // ENUM
        iFieldValue = ((Enum<?>) iFieldValue).name();
      } else {
        // LINK OR EMBEDDED
        fieldClass = iEntityManager.getEntityClass(fieldClass.getSimpleName());
        if (fieldClass != null) {
          // RECOGNIZED TYPE, SERIALIZE IT
          final ODocument linkedDocument =
              (ODocument) iObj2RecHandler.getRecordByUserObject(iFieldValue, true);

          final Object pojo = iFieldValue;
          iFieldValue =
              toStream(
                  pojo,
                  linkedDocument,
                  iEntityManager,
                  ODocumentInternal.getImmutableSchemaClass(linkedDocument),
                  iObj2RecHandler,
                  db,
                  iSaveOnlyDirty);

          iObj2RecHandler.registerUserObject(pojo, linkedDocument);

        } else {
          final Object result = serializeFieldValue(null, iFieldValue);
          if (iFieldValue == result)
            throw new OSerializationException(
                "Linked type ["
                    + iFieldValue.getClass()
                    + ":"
                    + iFieldValue
                    + "] cannot be serialized because is not part of registered entities. To fix this error register this class");

          iFieldValue = result;
        }
      }
    }
    return iFieldValue;
  }

  private static Object multiValueToStream(
      final Object iMultiValue,
      OType iType,
      final OEntityManager iEntityManager,
      final OUserObject2RecordHandler iObj2RecHandler,
      final ODatabaseObject db,
      final ODocument iRecord,
      final boolean iSaveOnlyDirty) {
    if (iMultiValue == null) return null;

    final Collection<Object> sourceValues;
    if (iMultiValue instanceof Collection<?>) {
      sourceValues = (Collection<Object>) iMultiValue;
    } else {
      sourceValues = (Collection<Object>) ((Map<?, ?>) iMultiValue).values();
    }

    if (iType == null) {
      if (sourceValues.size() == 0) return iMultiValue;

      // TRY TO UNDERSTAND THE COLLECTION TYPE BY ITS CONTENT
      final Object firstValue = sourceValues.iterator().next();

      if (firstValue == null) return iMultiValue;

      // DETERMINE THE RIGHT TYPE BASED ON SOURCE MULTI VALUE OBJECT
      if (OType.isSimpleType(firstValue)) {
        if (iMultiValue instanceof List) iType = OType.EMBEDDEDLIST;
        else if (iMultiValue instanceof Set) iType = OType.EMBEDDEDSET;
        else iType = OType.EMBEDDEDMAP;
      } else {
        if (iMultiValue instanceof List) iType = OType.LINKLIST;
        else if (iMultiValue instanceof Set) iType = OType.LINKSET;
        else iType = OType.LINKMAP;
      }
    }

    Object result = iMultiValue;
    final OType linkedType;

    // CREATE THE RETURN MULTI VALUE OBJECT BASED ON DISCOVERED TYPE
    if (iType.equals(OType.EMBEDDEDSET) || iType.equals(OType.LINKSET)) {
      if (iRecord != null && iType.equals(OType.EMBEDDEDSET))
        result = new OTrackedSet<Object>(iRecord);
      else result = new ORecordLazySet(iRecord);
    } else if (iType.equals(OType.EMBEDDEDLIST) || iType.equals(OType.LINKLIST)) {
      if (iRecord != null && iType.equals(OType.EMBEDDEDLIST))
        result = new OTrackedList<Object>(iRecord);
      else result = new ArrayList<Object>();
    }
    // } else if (iType.equals(OType.EMBEDDEDLIST) ||
    // iType.equals(OType.LINKLIST)) {
    // result = new ArrayList<Object>();
    // } else if (iType.equals(OType.EMBEDDEDMAP) ||
    // iType.equals(OType.LINKMAP)) {
    // result = new HashMap<String, Object>();
    // } else
    // throw new IllegalArgumentException("Type " + iType +
    // " must be a collection");

    if (iType.equals(OType.LINKLIST) || iType.equals(OType.LINKSET) || iType.equals(OType.LINKMAP))
      linkedType = OType.LINK;
    else if (iType.equals(OType.EMBEDDEDLIST)
        || iType.equals(OType.EMBEDDEDSET)
        || iType.equals(OType.EMBEDDEDMAP)) linkedType = OType.EMBEDDED;
    else
      throw new IllegalArgumentException(
          "Type " + iType + " must be a multi value type (collection or map)");

    if (iMultiValue instanceof Set<?>) {
      for (Object o : sourceValues) {
        ((Collection<Object>) result)
            .add(
                typeToStream(
                    o, linkedType, iEntityManager, iObj2RecHandler, db, null, iSaveOnlyDirty));
      }
    } else if (iMultiValue instanceof List<?>) {
      for (int i = 0; i < sourceValues.size(); i++) {
        ((List<Object>) result)
            .add(
                typeToStream(
                    ((List<?>) sourceValues).get(i),
                    linkedType,
                    iEntityManager,
                    iObj2RecHandler,
                    db,
                    null,
                    iSaveOnlyDirty));
      }
    } else {
      if (iMultiValue instanceof OObjectLazyMap<?>) {
        result = ((OObjectLazyMap<?>) iMultiValue).getUnderlying();
      } else {
        if (iRecord != null && iType.equals(OType.EMBEDDEDMAP))
          result = new OTrackedMap<Object>(iRecord);
        else result = new HashMap<Object, Object>();
        for (Entry<Object, Object> entry : ((Map<Object, Object>) iMultiValue).entrySet()) {
          ((Map<Object, Object>) result)
              .put(
                  entry.getKey(),
                  typeToStream(
                      entry.getValue(),
                      linkedType,
                      iEntityManager,
                      iObj2RecHandler,
                      db,
                      null,
                      iSaveOnlyDirty));
        }
      }
    }

    return result;
  }

  public static List<Field> getClassFields(final Class<?> iClass) {
    if (iClass.getName().startsWith("java.lang")) return null;

    synchronized (classes) {
      if (classes.containsKey(iClass.getName())) return classes.get(iClass.getName());

      return analyzeClass(iClass);
    }
  }

  /**
   * Returns the declared generic types of a class.
   *
   * @param iObject Class to examine
   * @return The array of Type if any, otherwise null
   */
  public static Type[] getGenericTypes(final Object iObject) {
    if (iObject instanceof OTrackedMultiValue) {
      final Class<?> cls = ((OTrackedMultiValue<?, ?>) iObject).getGenericClass();
      if (cls != null) return new Type[] {cls};
    }

    return OReflectionHelper.getGenericTypes(iObject.getClass());
  }

  public static void invokeCallback(
      final Object iPojo, final ODocument iDocument, final Class<?> iAnnotation) {
    final Method m =
        callbacks.get(iPojo.getClass().getSimpleName() + "." + iAnnotation.getSimpleName());

    if (m != null)
      try {
        if (m.getParameterTypes().length > 0) m.invoke(iPojo, iDocument);
        else m.invoke(iPojo);
      } catch (Exception e) {
        throw OException.wrapException(
            new OConfigurationException(
                "Error on executing user callback '"
                    + m.getName()
                    + "' annotated with '"
                    + iAnnotation.getSimpleName()
                    + "'"),
            e);
      }
  }

  public static void bindSerializerContext(
      final Class<?> iClassContext, final OObjectSerializerContext iSerializerContext) {
    serializerContexts.put(iClassContext, iSerializerContext);
  }

  public static void unbindSerializerContext(final Class<?> iClassContext) {
    serializerContexts.remove(iClassContext);
  }

  protected static List<Field> analyzeClass(final Class<?> iClass) {

    final List<Field> properties = new ArrayList<Field>();
    classes.put(iClass.getName(), properties);

    String fieldName;
    Class<?> fieldType;
    int fieldModifier;
    boolean autoBinding;

    for (Class<?> currentClass = iClass; currentClass != Object.class; ) {
      for (Field f : currentClass.getDeclaredFields()) {
        fieldModifier = f.getModifiers();
        if (Modifier.isStatic(fieldModifier)
            || Modifier.isNative(fieldModifier)
            || Modifier.isTransient(fieldModifier)) continue;

        if (f.getName().equals("this$0")) continue;

        if (jpaTransientClass != null) {
          Annotation ann = f.getAnnotation(jpaTransientClass);
          if (ann != null)
            // @Transient DEFINED, JUMP IT
            continue;
        }

        fieldName = f.getName();
        fieldType = f.getType();
        properties.add(f);

        // CHECK FOR AUTO-BINDING
        autoBinding = true;
        if (f.getAnnotation(OAccess.class) == null
            || f.getAnnotation(OAccess.class).value() == OAccess.OAccessType.PROPERTY)
          autoBinding = true;
        // JPA 2+ AVAILABLE?
        else if (jpaAccessClass != null) {
          Annotation ann = f.getAnnotation(jpaAccessClass);
          if (ann != null) {
            // TODO: CHECK IF CONTAINS VALUE=FIELD
            autoBinding = true;
          }
        }

        if (f.getAnnotation(ODocumentInstance.class) != null)
          // BOUND DOCUMENT ON IT
          boundDocumentFields.put(iClass, f);

        boolean idFound = false;
        if (f.getAnnotation(OId.class) != null) {
          // RECORD ID
          fieldIds.put(iClass, f);
          idFound = true;
        } else if (jpaIdClass != null && f.getAnnotation(jpaIdClass) != null) {
          // JPA 1+ AVAILABLE?
          // RECORD ID
          fieldIds.put(iClass, f);
          idFound = true;
        }
        if (idFound) {
          // CHECK FOR TYPE
          if (fieldType.isPrimitive())
            OLogManager.instance()
                .warn(
                    OObjectSerializerHelper.class,
                    "Field '%s' cannot be a literal to manage the Record Id",
                    f.toString());
          else if (!ORID.class.isAssignableFrom(fieldType)
              && fieldType != String.class
              && fieldType != Object.class
              && !Number.class.isAssignableFrom(fieldType))
            OLogManager.instance()
                .warn(
                    OObjectSerializerHelper.class,
                    "Field '%s' cannot be managed as type: %s",
                    f.toString(),
                    fieldType);
        }

        boolean vFound = false;
        if (f.getAnnotation(OVersion.class) != null) {
          // RECORD ID
          fieldVersions.put(iClass, f);
          vFound = true;
        } else if (jpaVersionClass != null && f.getAnnotation(jpaVersionClass) != null) {
          // JPA 1+ AVAILABLE?
          // RECORD ID
          fieldVersions.put(iClass, f);
          vFound = true;
        }
        if (vFound) {
          // CHECK FOR TYPE
          if (fieldType.isPrimitive())
            OLogManager.instance()
                .warn(
                    OObjectSerializerHelper.class,
                    "Field '%s' cannot be a literal to manage the Version",
                    f.toString());
          else if (fieldType != String.class
              && fieldType != Object.class
              && !Number.class.isAssignableFrom(fieldType))
            OLogManager.instance()
                .warn(
                    OObjectSerializerHelper.class,
                    "Field '%s' cannot be managed as type: %s",
                    f.toString(),
                    fieldType);
        }

        // JPA 1+ AVAILABLE?
        if (jpaEmbeddedClass != null && f.getAnnotation(jpaEmbeddedClass) != null) {
          if (embeddedFields.get(iClass) == null)
            embeddedFields.put(iClass, new ArrayList<String>());
          embeddedFields.get(iClass).add(fieldName);
        }

        if (autoBinding)
          // TRY TO GET THE VALUE BY THE GETTER (IF ANY)
          try {
            String getterName = "get" + OUtils.camelCase(fieldName);
            Method m = currentClass.getMethod(getterName, NO_ARGS);
            getters.put(iClass.getName() + "." + fieldName, m);
          } catch (Exception e) {
            registerFieldGetter(iClass, fieldName, f);
          }
        else registerFieldGetter(iClass, fieldName, f);

        if (autoBinding)
          // TRY TO GET THE VALUE BY THE SETTER (IF ANY)
          try {
            String getterName = "set" + OUtils.camelCase(fieldName);
            Method m = currentClass.getMethod(getterName, f.getType());
            setters.put(iClass.getName() + "." + fieldName, m);
          } catch (Exception e) {
            registerFieldSetter(iClass, fieldName, f);
          }
        else registerFieldSetter(iClass, fieldName, f);
      }

      registerCallbacks(iClass, currentClass);

      currentClass = currentClass.getSuperclass();

      if (currentClass.equals(ODocument.class))
        // POJO EXTENDS ODOCUMENT: SPECIAL CASE: AVOID TO CONSIDER
        // ODOCUMENT FIELDS
        currentClass = Object.class;
    }
    return properties;
  }

  @SuppressWarnings("rawtypes")
  private static void registerCallbacks(final Class<?> iRootClass, final Class<?> iCurrentClass) {
    // FIND KEY METHODS
    for (Method m : iCurrentClass.getDeclaredMethods()) {
      // SEARCH FOR CALLBACK ANNOTATIONS
      for (Class annotationClass : callbackAnnotationClasses) {
        if (m.getAnnotation(annotationClass) != null)
          callbacks.put(iRootClass.getSimpleName() + "." + annotationClass.getSimpleName(), m);
      }
    }
  }

  private static void registerFieldSetter(final Class<?> iClass, String fieldName, Field f) {
    // TRY TO GET THE VALUE BY ACCESSING DIRECTLY TO THE PROPERTY
    if (!f.isAccessible()) f.setAccessible(true);

    setters.put(iClass.getName() + "." + fieldName, f);
  }

  private static void registerFieldGetter(final Class<?> iClass, String fieldName, Field f) {
    // TRY TO GET THE VALUE BY ACCESSING DIRECTLY TO THE PROPERTY
    if (!f.isAccessible()) f.setAccessible(true);

    getters.put(iClass.getName() + "." + fieldName, f);
  }

  private static boolean isEmbeddedObject(
      final Class<?> iPojoClass,
      final Class<?> iFieldClass,
      final String iFieldName,
      final OEntityManager iEntityManager) {
    return embeddedFields.get(iPojoClass) != null
        && embeddedFields.get(iPojoClass).contains(iFieldName);
  }

  public static Object convertDocumentInType(final ODocument oDocument, final Class<?> type) {
    Object pojo = null;
    try {
      pojo = type.newInstance();
      final List<Field> fields = OObjectSerializerHelper.analyzeClass(type);
      for (Field aField : fields) {
        OObjectSerializerHelper.setFieldFromDocument(oDocument, pojo, aField);
      }
    } catch (Exception e) {
      OLogManager.instance().error(null, "Error on converting document in object", e);
    }
    return pojo;
  }

  private static void setFieldFromDocument(
      final ODocument iDocument, final Object iPojo, final Field iField) throws Exception {
    final String idFieldName = OObjectSerializerHelper.setObjectID(iDocument.getIdentity(), iPojo);
    final String vFieldName =
        OObjectSerializerHelper.setObjectVersion(iDocument.getVersion(), iPojo);
    final String fieldName = iField.getName();
    // Don't assign id and version fields, used by Orient
    if (!fieldName.equals(idFieldName) && !fieldName.equals(vFieldName)) {
      // Assign only fields that are in the document
      if (iDocument.containsField(fieldName)) {
        Class<?> aClass = (Class<?>) iField.getGenericType();
        Object fieldValue = iDocument.field(fieldName);
        Object realValue = OObjectSerializerHelper.getObject(fieldValue, aClass);
        String setterName =
            "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        final Method m = iPojo.getClass().getMethod(setterName, aClass);
        m.invoke(iPojo, realValue);
      }
    }
  }

  private static Object getObject(final Object fieldValue, final Class<?> aClass) {
    if (fieldValue instanceof ODocument)
      return OObjectSerializerHelper.convertDocumentInType((ODocument) fieldValue, aClass);
    return fieldValue;
  }

  public static Object convertInObject(
      final Object iPojo, final String iField, final Object iValue, final Class<?> parameterType) {
    // New conversion method working with OLazyObjectList
    if (!(iValue instanceof OObjectLazyList<?>)) return OType.convert(iValue, parameterType);

    List<Object> aSubList = null;
    try {
      final Field aField = OObjectSerializerHelper.getField(iPojo, iField);
      final Class<?> listClass = aField.getType();
      final ParameterizedType aType = (ParameterizedType) aField.getGenericType();
      final Class<?> objectClass = (Class<?>) aType.getActualTypeArguments()[0];
      final OObjectLazyList<?> aList = (OObjectLazyList<?>) iValue;
      // Instantiation of the list
      if (listClass.isInterface()) {
        aSubList = new ArrayList<Object>();
      } else {
        aSubList = (List<Object>) listClass.newInstance();
      }
      for (final Object value : aList) {
        if (value instanceof ODocument) {
          final ODocument aDocument = (ODocument) value;
          aSubList.add(OObjectSerializerHelper.convertDocumentInType(aDocument, objectClass));
        } else {
          aSubList.add(value);
        }
      }
    } catch (Exception e) {
      OLogManager.instance().error(null, "Error on convertInObject()", e);
    }
    return aSubList;
  }

  private static Field getField(final Object iPojo, final String iField) {
    final List<Field> fields = OObjectSerializerHelper.getClassFields(iPojo.getClass());
    if (fields != null) {
      for (Field f : fields) if (f.getName().equals(iField)) return f;
    }
    return null;
  }
}
