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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.reflection.OReflectionHelper;
import com.orientechnologies.orient.core.annotation.OAccess;
import com.orientechnologies.orient.core.annotation.OAfterDeserialization;
import com.orientechnologies.orient.core.annotation.OAfterSerialization;
import com.orientechnologies.orient.core.annotation.OBeforeDeserialization;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.annotation.OId;
import com.orientechnologies.orient.core.annotation.OVersion;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.object.db.OObjectLazyMap;
import com.orientechnologies.orient.object.serialization.OObjectSerializationThreadLocal;
import com.orientechnologies.orient.object.serialization.OObjectSerializerHelper;

/**
 * @author luca.molino
 * 
 */
public class OObjectEntitySerializer {

	private static final Set<Class<?>>														classes							= new HashSet<Class<?>>();
	private static final HashMap<Class<?>, List<String>>					embeddedFields			= new HashMap<Class<?>, List<String>>();
	private static final HashMap<Class<?>, List<String>>					directAccessFields	= new HashMap<Class<?>, List<String>>();
	private static final HashMap<Class<?>, Field>									boundDocumentFields	= new HashMap<Class<?>, Field>();
	private static final HashMap<Class<?>, List<String>>					transientFields			= new HashMap<Class<?>, List<String>>();
	private static final HashMap<Class<?>, Map<Field, Class<?>>>	serializedFields		= new HashMap<Class<?>, Map<Field, Class<?>>>();
	private static final HashMap<Class<?>, Field>									fieldIds						= new HashMap<Class<?>, Field>();
	private static final HashMap<Class<?>, Field>									fieldVersions				= new HashMap<Class<?>, Field>();
	private static final HashMap<String, Method>									callbacks						= new HashMap<String, Method>();

	/**
	 * Method that given an object serialize it an creates a proxy entity, in case the object isn't generated using the
	 * ODatabaseObject.newInstance()
	 * 
	 * @param o
	 *          - the object to serialize
	 * @return the proxied object
	 */
	public static <T> T serializeObject(T o, ODatabaseObject db) {
		if (o instanceof Proxy)
			return o;
		Proxy proxiedObject = (Proxy) db.newInstance(o.getClass());
		try {
			return toStream(o, proxiedObject, db);
		} catch (IllegalArgumentException e) {
			throw new OSerializationException("Error serializing object of class " + o.getClass(), e);
		} catch (IllegalAccessException e) {
			throw new OSerializationException("Error serializing object of class " + o.getClass(), e);
		}
	}

	/**
	 * Method that given a proxied entity returns the associated ODocument
	 * 
	 * @param proxiedObject
	 *          - the proxied entity object
	 * @return The ODocument associated with the object
	 */
	public static ODocument getDocument(Proxy proxiedObject) {
		return ((OObjectProxyMethodHandler) ((ProxyObject) proxiedObject).getHandler()).getDoc();
	}

	/**
	 * Method that given a proxied entity returns the associated ODocument RID
	 * 
	 * @param proxiedObject
	 *          - the proxied entity object
	 * @return The ORID of associated ODocument
	 */
	public static ORID getRid(Proxy proxiedObject) {
		return getDocument(proxiedObject).getIdentity();
	}

	/**
	 * Method that given a proxied entity returns the associated ODocument version
	 * 
	 * @param proxiedObject
	 *          - the proxied entity object
	 * @return The version of associated ODocument
	 */
	public static int getVersion(Proxy proxiedObject) {
		return getDocument(proxiedObject).getVersion();
	}

	public static boolean isTransientField(Class<?> iClass, String iField) {
		if (!classes.contains(iClass))
			registerClass(iClass);
		boolean isTransientField = false;
		for (Class<?> currentClass = iClass; currentClass != null && currentClass != Object.class
				&& !currentClass.equals(ODocument.class) && !isTransientField;) {
			List<String> classEmbeddedFields = transientFields.get(iClass);
			isTransientField = classEmbeddedFields != null && classEmbeddedFields.contains(iField);
			currentClass = currentClass.getSuperclass();
		}
		return isTransientField;
	}

	public static boolean isEmbeddedField(Class<?> iClass, String iField) {
		if (!classes.contains(iClass))
			registerClass(iClass);
		boolean isEmbeddedField = false;
		for (Class<?> currentClass = iClass; currentClass != null && currentClass != Object.class
				&& !currentClass.equals(ODocument.class) && !isEmbeddedField;) {
			List<String> classEmbeddedFields = embeddedFields.get(iClass);
			isEmbeddedField = classEmbeddedFields != null && classEmbeddedFields.contains(iField);
			currentClass = currentClass.getSuperclass();
		}
		return isEmbeddedField;
	}

	/**
	 * Registers the class informations that will be used in serialization, deserialization and lazy loading of it. If already
	 * registered does nothing.
	 * 
	 * @param iClass
	 *          :- the Class<?> to register
	 */
	@SuppressWarnings("unchecked")
	public static synchronized void registerClass(Class<?> iClass) {
		if (classes.contains(iClass))
			return;

		if (ODatabaseRecordThreadLocal.INSTANCE.isDefined() && !ODatabaseRecordThreadLocal.INSTANCE.get().isClosed()
				&& !ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().existsClass(iClass.getSimpleName()))
			ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().createClass(iClass.getSimpleName());

		for (Class<?> currentClass = iClass; currentClass != Object.class;) {
			classes.add(currentClass);

			Class<?> fieldType;
			for (Field f : currentClass.getDeclaredFields()) {
				final String fieldName = f.getName();
				final int fieldModifier = f.getModifiers();
				if (Modifier.isStatic(fieldModifier) || Modifier.isNative(fieldModifier) || Modifier.isTransient(fieldModifier))
					continue;

				if (fieldName.equals("this$0")) {
					List<String> classTransientFields = transientFields.get(currentClass);
					if (classTransientFields == null)
						classTransientFields = new ArrayList<String>();
					classTransientFields.add(fieldName);
					transientFields.put(currentClass, classTransientFields);
				}

				if (OObjectSerializerHelper.jpaTransientClass != null) {
					Annotation ann = f.getAnnotation(OObjectSerializerHelper.jpaTransientClass);
					if (ann != null) {
						// @Transient DEFINED
						List<String> classTransientFields = transientFields.get(currentClass);
						if (classTransientFields == null)
							classTransientFields = new ArrayList<String>();
						classTransientFields.add(fieldName);
						transientFields.put(currentClass, classTransientFields);
					}
				}

				fieldType = f.getType();
				if (Collection.class.isAssignableFrom(fieldType) || fieldType.isArray() || Map.class.isAssignableFrom(fieldType)) {
					fieldType = OReflectionHelper.getGenericMultivalueType(f);
				}
				if (isToSerialize(fieldType)) {
					Map<Field, Class<?>> serializeClass = serializedFields.get(currentClass);
					if (serializeClass == null)
						serializeClass = new HashMap<Field, Class<?>>();
					serializeClass.put(f, fieldType);
					serializedFields.put(currentClass, serializeClass);
				}

				// CHECK FOR DIRECT-BINDING
				boolean directBinding = true;
				if (f.getAnnotation(OAccess.class) == null || f.getAnnotation(OAccess.class).value() == OAccess.OAccessType.PROPERTY)
					directBinding = true;
				// JPA 2+ AVAILABLE?
				else if (OObjectSerializerHelper.jpaAccessClass != null) {
					Annotation ann = f.getAnnotation(OObjectSerializerHelper.jpaAccessClass);
					if (ann != null) {
						directBinding = true;
					}
				}
				if (directBinding) {
					List<String> classDirectAccessFields = directAccessFields.get(currentClass);
					if (classDirectAccessFields == null)
						classDirectAccessFields = new ArrayList<String>();
					classDirectAccessFields.add(fieldName);
					directAccessFields.put(currentClass, classDirectAccessFields);
				}

				if (f.getAnnotation(ODocumentInstance.class) != null)
					// BOUND DOCUMENT ON IT
					boundDocumentFields.put(currentClass, f);

				boolean idFound = false;
				if (f.getAnnotation(OId.class) != null) {
					// RECORD ID
					fieldIds.put(currentClass, f);
					idFound = true;
				}
				// JPA 1+ AVAILABLE?
				else if (OObjectSerializerHelper.jpaIdClass != null && f.getAnnotation(OObjectSerializerHelper.jpaIdClass) != null) {
					// RECORD ID
					fieldIds.put(currentClass, f);
					idFound = true;
				}
				if (idFound) {
					// CHECK FOR TYPE
					if (fieldType.isPrimitive())
						OLogManager.instance().warn(OObjectSerializerHelper.class, "Field '%s' cannot be a literal to manage the Record Id",
								f.toString());
					else if (!ORID.class.isAssignableFrom(fieldType) && fieldType != String.class && fieldType != Object.class
							&& !Number.class.isAssignableFrom(fieldType))
						OLogManager.instance().warn(OObjectSerializerHelper.class, "Field '%s' cannot be managed as type: %s", f.toString(),
								fieldType);
				}

				boolean vFound = false;
				if (f.getAnnotation(OVersion.class) != null) {
					// RECORD ID
					fieldVersions.put(currentClass, f);
					vFound = true;
				}
				// JPA 1+ AVAILABLE?
				else if (OObjectSerializerHelper.jpaVersionClass != null
						&& f.getAnnotation(OObjectSerializerHelper.jpaVersionClass) != null) {
					// RECORD ID
					fieldVersions.put(currentClass, f);
					vFound = true;
				}
				if (vFound) {
					// CHECK FOR TYPE
					if (fieldType.isPrimitive())
						OLogManager.instance().warn(OObjectSerializerHelper.class, "Field '%s' cannot be a literal to manage the Version",
								f.toString());
					else if (fieldType != String.class && fieldType != Object.class && !Number.class.isAssignableFrom(fieldType))
						OLogManager.instance().warn(OObjectSerializerHelper.class, "Field '%s' cannot be managed as type: %s", f.toString(),
								fieldType);
				}

				// JPA 1+ AVAILABLE?
				if (OObjectSerializerHelper.jpaEmbeddedClass != null && f.getAnnotation(OObjectSerializerHelper.jpaEmbeddedClass) != null) {
					List<String> classEmbeddedFields = embeddedFields.get(currentClass);
					if (classEmbeddedFields == null)
						classEmbeddedFields = new ArrayList<String>();
					classEmbeddedFields.add(fieldName);
				}

			}

			registerCallbacks(currentClass);

			String iClassName = iClass.getSimpleName();
			currentClass = currentClass.getSuperclass();

			if (currentClass == null || currentClass.equals(ODocument.class))
				// POJO EXTENDS ODOCUMENT: SPECIAL CASE: AVOID TO CONSIDER
				// ODOCUMENT FIELDS
				currentClass = Object.class;

			if (ODatabaseRecordThreadLocal.INSTANCE.get() != null && !ODatabaseRecordThreadLocal.INSTANCE.get().isClosed()
					&& !currentClass.equals(Object.class)) {
				OClass oSuperClass;
				OClass currentOClass = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().getClass(iClassName);
				if (!ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().existsClass(currentClass.getSimpleName()))
					oSuperClass = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema()
							.createClass(currentClass.getSimpleName());
				else
					oSuperClass = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().getClass(currentClass.getSimpleName());

				if (currentOClass.getSuperClass() == null || !currentOClass.getSuperClass().equals(oSuperClass))
					currentOClass.setSuperClass(oSuperClass);

			}
		}
		if (ODatabaseRecordThreadLocal.INSTANCE.get() != null && !ODatabaseRecordThreadLocal.INSTANCE.get().isClosed())
			ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().save();
	}

	public static boolean isSerializedType(final Field iField) {
		if (!classes.contains(iField.getDeclaringClass()))
			registerCallbacks(iField.getDeclaringClass());
		Map<Field, Class<?>> serializerFields = serializedFields.get(iField.getDeclaringClass());
		return serializerFields != null && serializerFields.get(iField) != null;
	}

	public static Class<?> getSerializedType(final Field iField) {
		if (!classes.contains(iField.getDeclaringClass()))
			registerCallbacks(iField.getDeclaringClass());
		return serializedFields.get(iField.getDeclaringClass()) != null ? serializedFields.get(iField.getDeclaringClass()).get(iField)
				: null;
	}

	public static boolean isToSerialize(final Class<?> type) {
		for (Class<?> classContext : OObjectSerializerHelper.serializerContexts.keySet()) {
			if (classContext != null && classContext.isAssignableFrom(type)) {
				return true;
			}
		}
		return OObjectSerializerHelper.serializerContexts.get(null) != null
				&& OObjectSerializerHelper.serializerContexts.get(null).isClassBinded(type);
	}

	public static Object serializeFieldValue(final Class<?> type, final Object iFieldValue) {

		for (Class<?> classContext : OObjectSerializerHelper.serializerContexts.keySet()) {
			if (classContext != null && classContext.isAssignableFrom(type)) {
				return OObjectSerializerHelper.serializerContexts.get(classContext).serializeFieldValue(type, iFieldValue);
			}
		}

		if (OObjectSerializerHelper.serializerContexts.get(null) != null)
			return OObjectSerializerHelper.serializerContexts.get(null).serializeFieldValue(type, iFieldValue);

		return iFieldValue;
	}

	public static Object deserializeFieldValue(final Class<?> type, final Object iFieldValue) {
		for (Class<?> classContext : OObjectSerializerHelper.serializerContexts.keySet()) {
			if (classContext != null && classContext.isAssignableFrom(type)) {
				return OObjectSerializerHelper.serializerContexts.get(classContext).unserializeFieldValue(type, iFieldValue);
			}
		}

		if (OObjectSerializerHelper.serializerContexts.get(null) != null)
			return OObjectSerializerHelper.serializerContexts.get(null).unserializeFieldValue(type, iFieldValue);

		return iFieldValue;
	}

	public static Object typeToStream(Object iFieldValue, OType iType, final ODatabaseObject db, final ODocument iRecord) {
		if (iFieldValue == null)
			return null;
		if (iFieldValue instanceof Proxy)
			return getDocument((Proxy) iFieldValue);

		if (!OType.isSimpleType(iFieldValue)) {
			Class<?> fieldClass = iFieldValue.getClass();

			if (fieldClass.isArray()) {
				// ARRAY
				iFieldValue = multiValueToStream(Arrays.asList(iFieldValue), iType, db, iRecord);
			} else if (Collection.class.isAssignableFrom(fieldClass)) {
				// COLLECTION (LIST OR SET)
				iFieldValue = multiValueToStream(iFieldValue, iType, db, iRecord);
			} else if (Map.class.isAssignableFrom(fieldClass)) {
				// MAP
				iFieldValue = multiValueToStream(iFieldValue, iType, db, iRecord);
			} else if (fieldClass.isEnum()) {
				// ENUM
				iFieldValue = ((Enum<?>) iFieldValue).name();
			} else {
				// LINK OR EMBEDDED
				fieldClass = db.getEntityManager().getEntityClass(fieldClass.getSimpleName());
				if (fieldClass != null) {
					// RECOGNIZED TYPE, SERIALIZE IT
					iFieldValue = getDocument((Proxy) serializeObject(iFieldValue, db));

				} else {
					final Object result = serializeFieldValue(null, iFieldValue);
					if (iFieldValue == result)
						throw new OSerializationException("Linked type [" + iFieldValue.getClass() + ":" + iFieldValue
								+ "] cannot be serialized because is not part of registered entities. To fix this error register this class");

					iFieldValue = result;
				}
			}
		}
		return iFieldValue;
	}

	public static boolean hasBoundedDocumentField(final Class<?> iClass) {
		if (!classes.contains(iClass)) {
			registerClass(iClass);
		}
		boolean hasBoundedField = false;
		for (Class<?> currentClass = iClass; currentClass != null && currentClass != Object.class
				&& !currentClass.equals(ODocument.class) && !hasBoundedField;) {
			hasBoundedField = boundDocumentFields.get(currentClass) != null;
			currentClass = currentClass.getSuperclass();
		}
		return hasBoundedField;
	}

	public static Field getBoundedDocumentField(final Class<?> iClass) {
		if (!classes.contains(iClass)) {
			registerClass(iClass);
		}
		for (Class<?> currentClass = iClass; currentClass != null && currentClass != Object.class
				&& !currentClass.equals(ODocument.class);) {
			Field f = boundDocumentFields.get(currentClass);
			if (f != null)
				return f;
			currentClass = currentClass.getSuperclass();
		}
		return null;
	}

	public static boolean isIdField(final Class<?> iClass, String iFieldName) {
		if (!classes.contains(iClass)) {
			registerClass(iClass);
		}
		boolean isIdField = false;
		for (Class<?> currentClass = iClass; currentClass != null && currentClass != Object.class
				&& !currentClass.equals(ODocument.class) && !isIdField;) {
			Field f = fieldIds.get(currentClass);
			isIdField = f != null && f.getName().equals(iFieldName);
			currentClass = currentClass.getSuperclass();
		}
		return isIdField;
	}

	public static boolean isIdField(Field iField) {
		if (!classes.contains(iField.getDeclaringClass())) {
			registerClass(iField.getDeclaringClass());
		}
		return fieldIds.containsValue(iField);
	}

	public static void setIdField(final Class<?> iClass, Object iObject, ORID iValue) throws IllegalArgumentException,
			IllegalAccessException {
		if (!classes.contains(iClass)) {
			registerClass(iClass);
		}
		Field f = null;
		for (Class<?> currentClass = iClass; currentClass != null && currentClass != Object.class
				&& !currentClass.equals(ODocument.class);) {
			f = fieldIds.get(currentClass);
			if (f != null)
				break;
			currentClass = currentClass.getSuperclass();
		}
		if (f != null) {
			if (f.getType().equals(String.class))
				setFieldValue(f, iObject, iValue.toString());
			else if (f.getType().equals(Long.class))
				setFieldValue(f, iObject, Long.valueOf(iValue.getClusterPosition()));
			else if (f.getType().equals(Object.class))
				setFieldValue(f, iObject, iValue);
		}
	}

	public static boolean isVersionField(final Class<?> iClass, String iFieldName) {
		if (!classes.contains(iClass)) {
			registerClass(iClass);
		}
		boolean isVersionField = false;
		for (Class<?> currentClass = iClass; currentClass != null && currentClass != Object.class
				&& !currentClass.equals(ODocument.class) && !isVersionField;) {
			Field f = fieldVersions.get(currentClass);
			isVersionField = f != null && f.getName().equals(iFieldName);
			currentClass = currentClass.getSuperclass();
		}
		return isVersionField;
	}

	public static void setVersionField(final Class<?> iClass, Object iObject, int iValue) throws IllegalArgumentException,
			IllegalAccessException {
		if (!classes.contains(iClass)) {
			registerClass(iClass);
		}
		Field f = null;
		for (Class<?> currentClass = iClass; currentClass != null && currentClass != Object.class
				&& !currentClass.equals(ODocument.class);) {
			f = fieldVersions.get(currentClass);
			if (f != null)
				break;
			currentClass = currentClass.getSuperclass();
		}
		if (f != null)
			setFieldValue(f, iObject, iValue);
	}

	public static Object getFieldValue(Field iField, Object iInstance) throws IllegalArgumentException, IllegalAccessException {
		if (!iField.isAccessible()) {
			iField.setAccessible(true);
		}
		return iField.get(iInstance);
	}

	public static void setFieldValue(Field iField, Object iInstance, Object iValue) throws IllegalArgumentException,
			IllegalAccessException {
		if (!iField.isAccessible()) {
			iField.setAccessible(true);
		}
		iField.set(iInstance, iValue);
	}

	public static void invokeBeforeSerializationCallbacks(Class<?> iClass, Object iInstance, ODocument iDocument) {
		invokeCallback(iClass, iInstance, iDocument, OBeforeSerialization.class);
	}

	public static void invokeAfterSerializationCallbacks(Class<?> iClass, Object iInstance, ODocument iDocument) {
		invokeCallback(iClass, iInstance, iDocument, OAfterSerialization.class);
	}

	public static void invokeAfterDeserializationCallbacks(Class<?> iClass, Object iInstance, ODocument iDocument) {
		invokeCallback(iClass, iInstance, iDocument, OAfterDeserialization.class);
	}

	public static void invokeBeforeDeserializationCallbacks(Class<?> iClass, Object iInstance, ODocument iDocument) {
		invokeCallback(iClass, iInstance, iDocument, OBeforeDeserialization.class);
	}

	/**
	 * Serialize the user POJO to a ORecordDocument instance.
	 * 
	 * @param iPojo
	 *          User pojo to serialize
	 * @param iRecord
	 *          Record where to update
	 * @param iObj2RecHandler
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	@SuppressWarnings("unchecked")
	protected static <T> T toStream(final T iPojo, final Proxy iProxiedPojo, ODatabaseObject db) throws IllegalArgumentException,
			IllegalAccessException {

		final ODocument iRecord = getDocument(iProxiedPojo);
		final long timer = OProfiler.getInstance().startChrono();

		final Integer identityRecord = System.identityHashCode(iPojo);

		if (OObjectSerializationThreadLocal.INSTANCE.get().containsKey(identityRecord))
			return (T) OObjectSerializationThreadLocal.INSTANCE.get().get(identityRecord);

		OObjectSerializationThreadLocal.INSTANCE.get().put(identityRecord, iProxiedPojo);

		OProperty schemaProperty;

		final Class<?> pojoClass = iPojo.getClass();
		final OClass schemaClass = iRecord.getSchemaClass();

		// CHECK FOR ID BINDING
		final Field idField = getIdField(pojoClass);
		if (idField != null) {

			Object id = getFieldValue(idField, iPojo);
			if (id != null) {
				// FOUND
				if (id instanceof ORecordId) {
					iRecord.setIdentity((ORecordId) id);
				} else if (id instanceof Number) {
					// TREATS AS CLUSTER POSITION
					((ORecordId) iRecord.getIdentity()).clusterId = schemaClass.getDefaultClusterId();
					((ORecordId) iRecord.getIdentity()).clusterPosition = ((Number) id).longValue();
				} else if (id instanceof String)
					((ORecordId) iRecord.getIdentity()).fromString((String) id);
				else if (id.getClass().equals(Object.class))
					iRecord.setIdentity((ORecordId) id);
				else
					OLogManager.instance().warn(OObjectSerializerHelper.class,
							"@Id field has been declared as %s while the supported are: ORID, Number, String, Object", id.getClass());
			}
		}

		// CHECK FOR VERSION BINDING
		final Field vField = getVersionField(pojoClass);
		boolean versionConfigured = false;
		if (vField != null) {
			versionConfigured = true;
			Object ver = getFieldValue(vField, iPojo);
			if (ver != null) {
				// FOUND
				if (ver instanceof Number) {
					// TREATS AS CLUSTER POSITION
					iRecord.setVersion(((Number) ver).intValue());
				} else if (ver instanceof String)
					iRecord.setVersion(Integer.parseInt((String) ver));
				else if (ver.getClass().equals(Object.class))
					iRecord.setVersion((Integer) ver);
				else
					OLogManager.instance().warn(OObjectSerializerHelper.class,
							"@Version field has been declared as %s while the supported are: Number, String, Object", ver.getClass());
			}
		}

		if (db.isMVCC() && !versionConfigured && db.getTransaction() instanceof OTransactionOptimistic)
			throw new OTransactionException(
					"Cannot involve an object of class '"
							+ pojoClass
							+ "' in an Optimistic Transaction commit because it does not define @Version or @OVersion and therefore cannot handle MVCC");

		String fieldName;
		Object fieldValue;

		// CALL BEFORE MARSHALLING
		invokeCallback(pojoClass, iProxiedPojo, iRecord, OBeforeSerialization.class);

		Class<?> currentClass = pojoClass;

		while (!currentClass.equals(Object.class) && classes.contains(pojoClass)) {
			for (Field p : currentClass.getDeclaredFields()) {
				if (Modifier.isStatic(p.getModifiers()) || Modifier.isNative(p.getModifiers()) || Modifier.isTransient(p.getModifiers()))
					continue;

				fieldName = p.getName();

				List<String> classTransientFields = transientFields.get(pojoClass);

				if ((idField != null && fieldName.equals(idField.getName()) || (vField != null && fieldName.equals(vField.getName())) || (classTransientFields != null && classTransientFields
						.contains(fieldName))))
					continue;

				fieldValue = getFieldValue(p, iPojo);
				if (isSerializedType(p))
					fieldValue = serializeFieldValue(p.getType(), fieldValue);

				schemaProperty = schemaClass != null ? schemaClass.getProperty(fieldName) : null;

				if (fieldValue != null) {
					if (isEmbeddedObject(p)) {
						// AUTO CREATE SCHEMA PROPERTY
						if (iRecord.getSchemaClass() == null) {
							db.getMetadata().getSchema().createClass(iPojo.getClass());
							iRecord.setClassNameIfExists(iPojo.getClass().getSimpleName());
						}

						if (schemaProperty == null) {
							OType t = OType.getTypeByClass(fieldValue.getClass());
							if (t == null)
								t = OType.EMBEDDED;
							schemaProperty = iRecord.getSchemaClass().createProperty(fieldName, t);
						}
					}
				}

				fieldValue = typeToStream(fieldValue, schemaProperty != null ? schemaProperty.getType() : null, db, iRecord);

				iRecord.field(fieldName, fieldValue);
			}

			currentClass = currentClass.getSuperclass();

			if (currentClass == null || currentClass.equals(ODocument.class))
				// POJO EXTENDS ODOCUMENT: SPECIAL CASE: AVOID TO CONSIDER
				// ODOCUMENT FIELDS
				currentClass = Object.class;

		}

		// CALL AFTER MARSHALLING
		invokeCallback(pojoClass, iProxiedPojo, iRecord, OAfterSerialization.class);

		OObjectSerializationThreadLocal.INSTANCE.get().remove(identityRecord);

		OProfiler.getInstance().stopChrono("Object.toStream", timer);

		return (T) iProxiedPojo;
	}

	protected static Field getIdField(final Class<?> iClass) {
		if (!classes.contains(iClass)) {
			registerClass(iClass);
		}
		return fieldIds.get(iClass);
	}

	protected static Field getVersionField(final Class<?> iClass) {
		if (!classes.contains(iClass)) {
			registerClass(iClass);
		}
		return fieldVersions.get(iClass);
	}

	protected static void invokeCallback(final Object iPojo, final ODocument iDocument, final Class<?> iAnnotation) {
		invokeCallback(iPojo.getClass(), iPojo, iDocument, iAnnotation);
	}

	protected static void invokeCallback(final Class<?> iClass, final Object iPojo, final ODocument iDocument,
			final Class<?> iAnnotation) {
		final Method m = getCallbackMethod(iAnnotation, iClass);
		if (m != null)
			try {
				if (m.getParameterTypes().length > 0)
					m.invoke(iPojo, iDocument);
				else
					m.invoke(iPojo);
			} catch (Exception e) {
				throw new OConfigurationException("Error on executing user callback '" + m.getName() + "' annotated with '"
						+ iAnnotation.getSimpleName() + "'", e);
			}
	}

	protected static Method getCallbackMethod(final Class<?> iAnnotation, final Class<?> iClass) {
		if (!classes.contains(iClass)) {
			registerClass(iClass);
		}
		return callbacks.get(iClass.getSimpleName() + "." + iAnnotation.getSimpleName());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void registerCallbacks(final Class<?> iRootClass) {
		// FIND KEY METHODS
		for (Method m : iRootClass.getDeclaredMethods()) {
			// SEARCH FOR CALLBACK ANNOTATIONS
			for (Class annotationClass : OObjectSerializerHelper.callbackAnnotationClasses) {
				if (m.getAnnotation(annotationClass) != null)
					callbacks.put(iRootClass.getSimpleName() + "." + annotationClass.getSimpleName(), m);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static Object multiValueToStream(final Object iMultiValue, OType iType, final ODatabaseObject db, final ODocument iRecord) {
		if (iMultiValue == null)
			return null;

		final Collection<Object> sourceValues;
		if (iMultiValue instanceof Collection<?>) {
			sourceValues = (Collection<Object>) iMultiValue;
		} else {
			sourceValues = (Collection<Object>) ((Map<?, ?>) iMultiValue).values();
		}

		if (sourceValues.size() == 0)
			return iMultiValue;

		// TRY TO UNDERSTAND THE COLLECTION TYPE BY ITS CONTENT
		final Object firstValue = sourceValues.iterator().next();

		if (firstValue == null)
			return iMultiValue;

		if (iType == null) {

			// DETERMINE THE RIGHT TYPE BASED ON SOURCE MULTI VALUE OBJECT
			if (OType.isSimpleType(firstValue)) {
				if (iMultiValue instanceof List)
					iType = OType.EMBEDDEDLIST;
				else if (iMultiValue instanceof Set)
					iType = OType.EMBEDDEDSET;
				else
					iType = OType.EMBEDDEDMAP;
			} else {
				if (iMultiValue instanceof List)
					iType = OType.LINKLIST;
				else if (iMultiValue instanceof Set)
					iType = OType.LINKSET;
				else
					iType = OType.LINKMAP;
			}
		}

		Object result = iMultiValue;
		final OType linkedType;

		// CREATE THE RETURN MULTI VALUE OBJECT BASED ON DISCOVERED TYPE
		if (iType.equals(OType.EMBEDDEDSET) || iType.equals(OType.LINKSET)) {
			if (isToSerialize(firstValue.getClass()))
				result = new HashSet<Object>();
			else if (iRecord != null && iType.equals(OType.EMBEDDEDSET))
				result = new OTrackedSet<Object>(iRecord);
			else
				result = new ORecordLazySet(iRecord);
		} else if (iType.equals(OType.EMBEDDEDLIST) || iType.equals(OType.LINKLIST)) {
			if (isToSerialize(firstValue.getClass()))
				result = new ArrayList<Object>();
			else if (iRecord != null && iType.equals(OType.EMBEDDEDLIST))
				result = new OTrackedList<Object>(iRecord);
			else
				result = new ORecordLazyList(iRecord);
		}

		if (iType.equals(OType.LINKLIST) || iType.equals(OType.LINKSET) || iType.equals(OType.LINKMAP))
			linkedType = OType.LINK;
		else if (iType.equals(OType.EMBEDDEDLIST) || iType.equals(OType.EMBEDDEDSET) || iType.equals(OType.EMBEDDEDMAP))
			linkedType = OType.EMBEDDED;
		else
			throw new IllegalArgumentException("Type " + iType + " must be a multi value type (collection or map)");

		if (iMultiValue instanceof Set<?>) {
			for (Object o : sourceValues) {
				((Set<Object>) result).add(typeToStream(o, linkedType, db, null));
			}
		} else if (iMultiValue instanceof List<?>) {
			for (int i = 0; i < sourceValues.size(); i++) {
				((List<Object>) result).add(typeToStream(((List<?>) sourceValues).get(i), linkedType, db, null));
			}
		} else {
			if (iMultiValue instanceof OObjectLazyMap<?>) {
				result = ((OObjectLazyMap<?>) iMultiValue).getUnderlying();
			} else {
				if (isToSerialize(firstValue.getClass()))
					result = new HashMap<Object, Object>();
				else if (iRecord != null && iType.equals(OType.EMBEDDEDMAP))
					result = new OTrackedMap<Object>(iRecord);
				else
					result = new ORecordLazyMap(iRecord);
				for (Entry<Object, Object> entry : ((Map<Object, Object>) iMultiValue).entrySet()) {
					((Map<Object, Object>) result).put(entry.getKey(), typeToStream(entry.getValue(), linkedType, db, null));
				}
			}
		}

		return result;
	}

	private static boolean isEmbeddedObject(Field f) {
		if (!classes.contains(f.getDeclaringClass()))
			registerClass(f.getDeclaringClass());
		return isEmbeddedField(f.getDeclaringClass(), f.getName());
	}

}
