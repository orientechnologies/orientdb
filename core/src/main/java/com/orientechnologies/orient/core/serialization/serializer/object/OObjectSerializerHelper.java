/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.serialization.serializer.object;

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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.annotation.OAccess;
import com.orientechnologies.orient.core.annotation.OAfterDeserialization;
import com.orientechnologies.orient.core.annotation.OAfterSerialization;
import com.orientechnologies.orient.core.annotation.OBeforeDeserialization;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.annotation.OId;
import com.orientechnologies.orient.core.annotation.OVersion;
import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.db.object.OLazyObjectList;
import com.orientechnologies.orient.core.db.object.OLazyObjectMap;
import com.orientechnologies.orient.core.db.object.OLazyObjectSet;
import com.orientechnologies.orient.core.db.object.OObjectNotDetachedException;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

@SuppressWarnings("unchecked")
/**
 * Helper class to manage POJO by using the reflection. 
 */
public class OObjectSerializerHelper {
	private static final Class<?>[]															callbackAnnotationClasses	= new Class[] {
			OBeforeDeserialization.class, OAfterDeserialization.class, OBeforeSerialization.class, OAfterSerialization.class };
	private static final Class<?>[]															NO_ARGS										= new Class<?>[] {};

	private static HashMap<Class<?>, OObjectSerializerContext>	serializerContexts				= new LinkedHashMap<Class<?>, OObjectSerializerContext>();

	private static HashMap<String, List<Field>>									classes										= new HashMap<String, List<Field>>();
	private static HashMap<String, Method>											callbacks									= new HashMap<String, Method>();
	private static HashMap<String, Object>											getters										= new HashMap<String, Object>();
	private static HashMap<String, Object>											setters										= new HashMap<String, Object>();
	private static HashMap<Class<?>, Field>											boundDocumentFields				= new HashMap<Class<?>, Field>();
	private static HashMap<Class<?>, Field>											fieldIds									= new HashMap<Class<?>, Field>();
	private static HashMap<Class<?>, Field>											fieldVersions							= new HashMap<Class<?>, Field>();
	private static HashMap<Class<?>, List<String>>							embeddedFields						= new HashMap<Class<?>, List<String>>();
	@SuppressWarnings("rawtypes")
	private static Class																				jpaIdClass;
	@SuppressWarnings("rawtypes")
	private static Class																				jpaVersionClass;
	@SuppressWarnings("rawtypes")
	private static Class																				jpaAccessClass;
	@SuppressWarnings("rawtypes")
	private static Class																				jpaEmbeddedClass;
	@SuppressWarnings("rawtypes")
	private static Class																				jpaTransientClass;

	static {
		try {
			// DETERMINE IF THERE IS AVAILABLE JPA 1
			jpaIdClass = Class.forName("javax.persistence.Id");
			jpaVersionClass = Class.forName("javax.persistence.Version");
			jpaEmbeddedClass = Class.forName("javax.persistence.Embedded");
			jpaTransientClass = Class.forName("javax.persistence.Transient");

			// DETERMINE IF THERE IS AVAILABLE JPA 2
			jpaAccessClass = Class.forName("javax.persistence.Access");

		} catch (Exception e) {
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

			if (o == null)
				return null;
			else if (o instanceof Field)
				return ((Field) o).getType();
			else
				return ((Method) o).getReturnType();
		} catch (Exception e) {
			throw new OSchemaException("Can't get the value of the property: " + iProperty, e);
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

			if (o instanceof Method)
				return ((Method) o).invoke(iPojo);
			else if (o instanceof Field)
				return ((Field) o).get(iPojo);
			return null;
		} catch (Exception e) {
			throw new OSchemaException("Can't get the value of the property: " + iProperty, e);
		}
	}

	public static void setFieldValue(final Object iPojo, final String iProperty, Object iValue) {
		final Class<?> c = iPojo.getClass();
		final String className = c.getName();

		getClassFields(c);

		try {
			Object o = setters.get(className + "." + iProperty);

			if (o instanceof Method) {
				((Method) o).invoke(iPojo, OType.convert(iValue, ((Method) o).getParameterTypes()[0]));
			} else if (o instanceof Field) {
				((Field) o).set(iPojo, OType.convert(iValue, ((Field) o).getType()));
			}

		} catch (Exception e) {

			throw new OSchemaException("Can't set the value '" + iValue + "' to the property '" + iProperty + "' for the pojo: " + iPojo,
					e);
		}
	}

	@SuppressWarnings("rawtypes")
	public static Object fromStream(final ODocument iRecord, final Object iPojo, final OEntityManager iEntityManager,
			final OUserObject2RecordHandler iObj2RecHandler, final String iFetchPlan, final boolean iLazyLoading) {
		OFetchHelper.checkFetchPlanValid(iFetchPlan);
		final long timer = OProfiler.getInstance().startChrono();

		final Class<?> pojoClass = iPojo.getClass();

		final List<Field> properties = getClassFields(pojoClass);

		String fieldName;
		Object fieldValue;

		final String idFieldName = setObjectID(iRecord.getIdentity(), iPojo);
		final String vFieldName = setObjectVersion(iRecord.getVersion(), iPojo);

		// CALL BEFORE UNMARSHALLING
		invokeCallback(iPojo, iRecord, OBeforeDeserialization.class);

		// BIND BASIC FIELDS, LINKS WILL BE BOUND BY THE FETCH API
		for (Field p : properties) {
			fieldName = p.getName();

			if (fieldName.equals(idFieldName) || fieldName.equals(vFieldName))
				continue;

			if (iRecord.containsField(fieldName)) {
				// BIND ONLY THE SPECIFIED FIELDS
				fieldValue = iRecord.field(fieldName);

				if (fieldValue == null
						|| !(fieldValue instanceof ODocument)
						|| (fieldValue instanceof Collection<?> && (((Collection<?>) fieldValue).size() == 0 || !(((Collection<?>) fieldValue)
								.iterator().next() instanceof ODocument)))
						|| (!(fieldValue instanceof Map<?, ?>) || ((Map<?, ?>) fieldValue).size() == 0 || !(((Map<?, ?>) fieldValue).values()
								.iterator().next() instanceof ODocument))) {

					final Class<?> genericTypeClass = getGenericMultivalueType(p);

					if (genericTypeClass != null)
						if (genericTypeClass.isEnum()) {
							// TRANSFORM THE MULTI-VALUE
							if (fieldValue instanceof List) {
								// LIST: TRANSFORM EACH SINGLE ITEM
								final List<Object> list = (List<Object>) fieldValue;
								Object v;
								for (int i = 0; i < list.size(); ++i) {
									v = list.get(i);
									if (v != null) {
										v = Enum.valueOf((Class<Enum>) genericTypeClass, v.toString());
										list.set(i, v);
									}
								}
							} else if (fieldValue instanceof Set) {
								// SET: CREATE A TEMP SET TO WORK WITH ITEMS
								final Set<Object> newColl = new HashSet<Object>();
								final Set<Object> set = (Set<Object>) fieldValue;
								for (Object v : set) {
									if (v != null) {
										v = Enum.valueOf((Class<Enum>) genericTypeClass, v.toString());
										newColl.add(v);
									}
								}

								fieldValue = newColl;
							} else if (fieldValue instanceof Map) {
								// MAP: TRANSFORM EACH SINGLE ITEM
								final Map<String, Object> map = (Map<String, Object>) fieldValue;
								Object v;
								for (Entry<String, ?> entry : map.entrySet()) {
									v = entry.getValue();
									if (v != null) {
										v = Enum.valueOf((Class<Enum>) genericTypeClass, v.toString());
										map.put(entry.getKey(), v);
									}
								}
							}

						}

					setFieldValue(iPojo, fieldName, unserializeFieldValue(iPojo, fieldName, fieldValue));
				}
			}

		}

		// BIND LINKS FOLLOWING THE FETCHING PLAN
		final Map<String, Integer> fetchPlan = OFetchHelper.buildFetchPlan(iFetchPlan);
		OFetchHelper.fetch(iRecord, iPojo, fetchPlan, null, 0, -1, new OFetchListener() {
			/***
			 * Doesn't matter size.
			 */
			public int size() {
				return 0;
			}

			public Object fetchLinked(final ODocument iRoot, final Object iUserObject, final String iFieldName, final Object iLinked) {
				final Class<?> type;
				if (iLinked != null && iLinked instanceof ODocument)
					// GET TYPE BY DOCUMENT'S CLASS. THIS WORKS VERY WELL FOR SUB-TYPES
					type = getFieldType((ODocument) iLinked, iEntityManager);
				else
					// DETERMINE TYPE BY REFLECTION
					type = getFieldType(iUserObject, iFieldName);

				if (type == null)
					throw new OSerializationException(
							"Linked type of field '"
									+ iRoot.getClassName()
									+ "."
									+ iFieldName
									+ "' is unknown. Probably needs to be registered with <db>.getEntityManager().registerEntityClasses(<package>) or <db>.getEntityManager().registerEntityClass(<class>) or the package can't be loaded correctly due to a classpath problem. In this case register the single classes one by one.");

				Object fieldValue = null;
				Class<?> fieldClass;
				boolean propagate = false;

				if (Set.class.isAssignableFrom(type)) {

					final Collection<Object> set = (Collection<Object>) iLinked;

					final Set<Object> target;
					if (iLazyLoading)
						target = new OLazyObjectSet<Object>(iRoot, set).setFetchPlan(iFetchPlan);
					else {
						target = new HashSet();
						if (set != null && !set.isEmpty())
							for (Object o : set) {
								if (o instanceof OIdentifiable)
									target.add(iObj2RecHandler.getUserObjectByRecord((ORecordInternal<?>) ((OIdentifiable) o).getRecord(), iFetchPlan));
								else
									target.add(o);
							}
					}
					fieldValue = target;

				} else if (Collection.class.isAssignableFrom(type)) {

					final Collection<ODocument> list = (Collection<ODocument>) iLinked;
					final List<Object> target;
					if (iLazyLoading)
						target = new OLazyObjectList<Object>(iRoot, list).setFetchPlan(iFetchPlan);
					else {
						target = new ArrayList();
						if (list != null && !list.isEmpty())
							for (Object o : list) {
								if (o instanceof OIdentifiable)
									target.add(iObj2RecHandler.getUserObjectByRecord((ORecordInternal<?>) ((OIdentifiable) o).getRecord(), iFetchPlan));
								else
									target.add(o);
							}
					}
					fieldValue = target;

				} else if (Map.class.isAssignableFrom(type)) {

					final Map<Object, Object> map = (Map<Object, Object>) iLinked;
					final Map<Object, Object> target;
					if (iLazyLoading)
						target = new OLazyObjectMap<Object>(iRoot, map).setFetchPlan(iFetchPlan);
					else {
						target = new HashMap();
						if (map != null && !map.isEmpty())
							for (Map.Entry<Object, Object> o : map.entrySet()) {
								final Object k = o.getKey() instanceof OIdentifiable ? iObj2RecHandler.getUserObjectByRecord(
										(ORecordInternal<?>) ((OIdentifiable) o.getKey()).getRecord(), iFetchPlan) : o.getKey();
								final Object v = o.getValue() instanceof OIdentifiable ? iObj2RecHandler.getUserObjectByRecord(
										(ORecordInternal<?>) ((OIdentifiable) o.getValue()).getRecord(), iFetchPlan) : o.getValue();
								target.put(k, v);
							}
					}
					fieldValue = target;

				} else if (type.isEnum()) {

					String enumName = ((ODocument) iLinked).field(iFieldName);
					Class<Enum> enumClass = (Class<Enum>) type;
					fieldValue = Enum.valueOf(enumClass, enumName);

				} else {

					fieldClass = iEntityManager.getEntityClass(type.getSimpleName());
					if (fieldClass != null) {
						// RECOGNIZED TYPE
						propagate = !iObj2RecHandler.existsUserObjectByRID(((ODocument) iLinked).getIdentity());

						fieldValue = iObj2RecHandler.getUserObjectByRecord((ODocument) iLinked, iFetchPlan);
					}
				}

				setFieldValue(iUserObject, iFieldName, unserializeFieldValue(iPojo, iFieldName, fieldValue));

				return propagate ? fieldValue : null;
			}
		});

		// CALL AFTER UNMARSHALLING
		invokeCallback(iPojo, iRecord, OAfterDeserialization.class);

		OProfiler.getInstance().stopChrono("Object.fromStream", timer);

		return iPojo;
	}

	public static String setObjectID(final ORID iIdentity, final Object iPojo) {
		if (iPojo == null)
			return null;

		final Class<?> pojoClass = iPojo.getClass();

		getClassFields(pojoClass);

		final Field idField = fieldIds.get(pojoClass);
		if (idField != null) {
			Class<?> fieldType = idField.getType();

			final String idFieldName = idField.getName();

			if (ORID.class.isAssignableFrom(fieldType))
				setFieldValue(iPojo, idFieldName, iIdentity);
			else if (Number.class.isAssignableFrom(fieldType))
				setFieldValue(iPojo, idFieldName, iIdentity != null ? iIdentity.getClusterPosition() : null);
			else if (fieldType.equals(String.class))
				setFieldValue(iPojo, idFieldName, iIdentity != null ? iIdentity.toString() : null);
			else if (fieldType.equals(Object.class))
				setFieldValue(iPojo, idFieldName, iIdentity);
			else
				OLogManager.instance().warn(OObjectSerializerHelper.class,
						"@Id field has been declared as %s while the supported are: ORID, Number, String, Object", fieldType);
			return idFieldName;
		}
		return null;
	}

	public static ORecordId getObjectID(final ODatabasePojoAbstract<?> iDb, final Object iPojo) {
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
						throw new OConfigurationException("Class " + iPojo.getClass() + " is not managed by current database");

					return new ORecordId(cls.getDefaultClusterId(), ((Number) id).longValue());
				} else if (id instanceof String)
					return new ORecordId((String) id);
			}
		}
		return null;
	}

	public static boolean hasObjectID(final Object iPojo) {
		getClassFields(iPojo.getClass());
		return fieldIds.get(iPojo.getClass()) != null;
	}

	public static String setObjectVersion(final Integer iVersion, final Object iPojo) {
		if (iPojo == null)
			return null;

		final Class<?> pojoClass = iPojo.getClass();
		getClassFields(pojoClass);

		final Field vField = fieldVersions.get(pojoClass);
		if (vField != null) {
			Class<?> fieldType = vField.getType();

			final String vFieldName = vField.getName();

			if (Number.class.isAssignableFrom(fieldType))
				setFieldValue(iPojo, vFieldName, iVersion);
			else if (fieldType.equals(String.class))
				setFieldValue(iPojo, vFieldName, String.valueOf(iVersion));
			else if (fieldType.equals(Object.class))
				setFieldValue(iPojo, vFieldName, iVersion);
			else
				OLogManager.instance().warn(OObjectSerializerHelper.class,
						"@Version field has been declared as %s while the supported are: Number, String, Object", fieldType);
			return vFieldName;
		}
		return null;
	}

	public static int getObjectVersion(final Object iPojo) {
		getClassFields(iPojo.getClass());
		final Field idField = fieldVersions.get(iPojo.getClass());
		if (idField != null) {
			final Object ver = getFieldValue(iPojo, idField.getName());

			if (ver != null) {
				// FOUND
				if (ver instanceof Number) {
					// TREATS AS CLUSTER POSITION
					return ((Number) ver).intValue();
				} else if (ver instanceof String)
					return Integer.parseInt((String) ver);
			}
		}
		throw new OObjectNotDetachedException("Can't retrieve the object's VERSION for '" + iPojo + "' because hasn't been detached");
	}

	public static boolean hasObjectVersion(final Object iPojo) {
		getClassFields(iPojo.getClass());
		return fieldVersions.get(iPojo.getClass()) != null;
	}

	/**
	 * Serialize the user POJO to a ORecordDocument instance.
	 * 
	 * @param iPojo
	 *          User pojo to serialize
	 * @param iRecord
	 *          Record where to update
	 * @param iObj2RecHandler
	 */
	public static ODocument toStream(final Object iPojo, final ODocument iRecord, final OEntityManager iEntityManager,
			final OClass schemaClass, final OUserObject2RecordHandler iObj2RecHandler, final ODatabaseObjectTx db,
			final boolean iSaveOnlyDirty) {
		if (iSaveOnlyDirty && !iRecord.isDirty())
			return iRecord;

		final long timer = OProfiler.getInstance().startChrono();

		final Integer identityRecord = System.identityHashCode(iRecord);

		if (OSerializationThreadLocal.INSTANCE.get().contains(identityRecord))
			return iRecord;

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
		final Field vField = fieldVersions.get(pojoClass);
		boolean versionConfigured = false;
		if (vField != null) {
			versionConfigured = true;
			Object ver = getFieldValue(iPojo, vField.getName());
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
			throw new OTransactionException("Can't involve an object of class '" + pojoClass
					+ "' in an Optimistic Transaction commit because it doesn't define @Version or @OVersion and therefore can't handle MVCC");

		// SET OBJECT CLASS
		iRecord.setClassName(schemaClass != null ? schemaClass.getName() : null);

		String fieldName;
		Object fieldValue;

		// CALL BEFORE MARSHALLING
		invokeCallback(iPojo, iRecord, OBeforeSerialization.class);

		for (Field p : properties) {
			fieldName = p.getName();

			if (idField != null && fieldName.equals(idField.getName()))
				continue;

			if (vField != null && fieldName.equals(vField.getName()))
				continue;

			fieldValue = serializeFieldValue(iPojo, fieldName, getFieldValue(iPojo, fieldName));

			schemaProperty = schemaClass != null ? schemaClass.getProperty(fieldName) : null;

			if (fieldValue != null
					&& getEmbeddetTypesByJPAAnnotation(iPojo.getClass(), fieldValue.getClass(), fieldName, iEntityManager) != null
					&& getEmbeddetTypesByJPAAnnotation(iPojo.getClass(), fieldValue.getClass(), fieldName, iEntityManager).equals(
							OType.EMBEDDED)) {
				if (schemaClass == null) {
					db.getMetadata().getSchema().createClass(iPojo.getClass());
					iRecord.setClassNameIfExists(iPojo.getClass().getSimpleName());
				}
				if (schemaProperty == null) {
					schemaProperty = iRecord.getSchemaClass().createProperty(fieldName, OType.EMBEDDED);
				}
			}

			fieldValue = typeToStream(fieldValue, schemaProperty != null ? schemaProperty.getType() : null, iEntityManager,
					iObj2RecHandler, db, iSaveOnlyDirty);

			iRecord.field(fieldName, fieldValue);
		}

		iObj2RecHandler.registerUserObject(iPojo, iRecord);

		// CALL AFTER MARSHALLING
		invokeCallback(iPojo, iRecord, OAfterSerialization.class);

		OSerializationThreadLocal.INSTANCE.get().remove(identityRecord);

		OProfiler.getInstance().stopChrono("Object.toStream", timer);

		return iRecord;
	}

	public static Object serializeFieldValue(final Object iPojo, final String iFieldName, final Object iFieldValue) {
		for (Class<?> classContext : serializerContexts.keySet()) {
			if (classContext != null && classContext.isInstance(iPojo)) {
				return serializerContexts.get(classContext).serializeFieldValue(iPojo, iFieldName, iFieldValue);
			}
		}

		if (serializerContexts.get(null) != null)
			return serializerContexts.get(null).serializeFieldValue(iPojo, iFieldName, iFieldValue);

		return iFieldValue;
	}

	public static Object unserializeFieldValue(final Object iPojo, final String iFieldName, Object iFieldValue) {
		for (Class<?> classContext : serializerContexts.keySet()) {
			if (classContext != null && classContext.isInstance(iPojo)) {
				return serializerContexts.get(classContext).unserializeFieldValue(iPojo, iFieldName, iFieldValue);
			}
		}

		if (serializerContexts.get(null) != null)
			return serializerContexts.get(null).unserializeFieldValue(iPojo, iFieldName, iFieldValue);

		return iFieldValue;
	}

	/**
	 * Returns the generic class of multi-value objects.
	 * 
	 * @param p
	 *          Field to examine
	 * @return The Class<?> of generic type if any, otherwise null
	 */
	public static Class<?> getGenericMultivalueType(final Field p) {
		final Type genericType = p.getGenericType();
		if (genericType != null && genericType instanceof ParameterizedType) {
			final ParameterizedType pt = (ParameterizedType) genericType;
			if (pt.getActualTypeArguments() != null && pt.getActualTypeArguments().length > 0) {
				if (pt.getActualTypeArguments()[0] instanceof Class<?>) {
					return (Class<?>) pt.getActualTypeArguments()[0];
				}
			}
		}
		return null;
	}

	private static Object typeToStream(Object iFieldValue, OType iType, final OEntityManager iEntityManager,
			final OUserObject2RecordHandler iObj2RecHandler, final ODatabaseObjectTx db, final boolean iSaveOnlyDirty) {
		if (iFieldValue == null)
			return null;

		if (!OType.isSimpleType(iFieldValue)) {
			Class<?> fieldClass = iFieldValue.getClass();

			if (fieldClass.isArray()) {
				// ARRAY
				iFieldValue = multiValueToStream(Arrays.asList(iFieldValue), iType, iEntityManager, iObj2RecHandler, db, iSaveOnlyDirty);
			} else if (Collection.class.isAssignableFrom(fieldClass)) {
				// COLLECTION (LIST OR SET)
				iFieldValue = multiValueToStream(iFieldValue, iType, iEntityManager, iObj2RecHandler, db, iSaveOnlyDirty);
			} else if (Map.class.isAssignableFrom(fieldClass)) {
				// MAP
				iFieldValue = multiValueToStream(iFieldValue, iType, iEntityManager, iObj2RecHandler, db, iSaveOnlyDirty);
			} else if (fieldClass.isEnum()) {
				// ENUM
				iFieldValue = ((Enum<?>) iFieldValue).name();
				iType = OType.STRING;
			} else {
				// LINK OR EMBEDDED
				fieldClass = iEntityManager.getEntityClass(fieldClass.getSimpleName());
				if (fieldClass != null) {
					// RECOGNIZED TYPE, SERIALIZE IT
					final ODocument linkedDocument = (ODocument) iObj2RecHandler.getRecordByUserObject(iFieldValue, true);

					final Object pojo = iFieldValue;
					iFieldValue = toStream(pojo, linkedDocument, iEntityManager, linkedDocument.getSchemaClass(), iObj2RecHandler, db,
							iSaveOnlyDirty);

					// if (linkedDocument.isDirty()) {
					// // SAVE THE DOCUMENT AND GET UDPATE THE VERSION. CALL THE UNDERLYING SAVE() TO AVOID THE SERIALIZATION THREAD IS
					// CLEANED
					// // AND GOES RECURSIVELY UP THE STACK IS EXHAUSTED
					// db.getUnderlying().save(linkedDocument);
					// }
					iObj2RecHandler.registerUserObject(pojo, linkedDocument);

				} else
					throw new OSerializationException("Linked type [" + iFieldValue.getClass() + ":" + iFieldValue
							+ "] can't be serialized because is not part of registered entities. To fix this error register this class");
			}
		}
		return iFieldValue;
	}

	private static Object multiValueToStream(final Object iMultiValue, OType iType, final OEntityManager iEntityManager,
			final OUserObject2RecordHandler iObj2RecHandler, final ODatabaseObjectTx db, final boolean iSaveOnlyDirty) {
		if (iMultiValue == null)
			return null;

		final Collection<Object> sourceValues;
		if (iMultiValue instanceof Collection<?>) {
			sourceValues = (Collection<Object>) iMultiValue;
		} else {
			sourceValues = (Collection<Object>) ((Map<?, ?>) iMultiValue).values();
		}

		if (iType == null) {
			if (sourceValues.size() == 0)
				return iMultiValue;

			// TRY TO UNDERSTAND THE COLLECTION TYPE BY ITS CONTENT
			final Object firstValue = sourceValues.iterator().next();

			if (firstValue == null)
				return iMultiValue;

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
			result = new HashSet<Object>();
		} else if (iType.equals(OType.EMBEDDEDLIST) || iType.equals(OType.LINKLIST)) {
			result = new ArrayList<Object>();
		}
		// } else if (iType.equals(OType.EMBEDDEDLIST) || iType.equals(OType.LINKLIST)) {
		// result = new ArrayList<Object>();
		// } else if (iType.equals(OType.EMBEDDEDMAP) || iType.equals(OType.LINKMAP)) {
		// result = new HashMap<String, Object>();
		// } else
		// throw new IllegalArgumentException("Type " + iType + " must be a collection");

		if (iType.equals(OType.LINKLIST) || iType.equals(OType.LINKSET) || iType.equals(OType.LINKMAP))
			linkedType = OType.LINK;
		else if (iType.equals(OType.EMBEDDEDLIST) || iType.equals(OType.EMBEDDEDSET) || iType.equals(OType.EMBEDDEDMAP))
			linkedType = OType.EMBEDDED;
		else
			throw new IllegalArgumentException("Type " + iType + " must be a multi value type (collection or map)");

		if (iMultiValue instanceof Set<?>) {
			for (Object o : sourceValues) {
				((Collection<Object>) result).add(typeToStream(o, linkedType, iEntityManager, iObj2RecHandler, db, iSaveOnlyDirty));
			}
		} else if (iMultiValue instanceof List<?>) {
			for (int i = 0; i < sourceValues.size(); i++) {
				((List<Object>) result).add(typeToStream(((List<?>) sourceValues).get(i), linkedType, iEntityManager, iObj2RecHandler, db,
						iSaveOnlyDirty));
			}
		} else {
			if (iMultiValue instanceof OLazyObjectMap<?>) {
				result = ((OLazyObjectMap<?>) iMultiValue).getUnderlying();
			} else {
				result = new HashMap<String, Object>();
				for (Entry<String, Object> entry : ((Map<String, Object>) iMultiValue).entrySet()) {
					((Map<String, Object>) result).put(entry.getKey(),
							typeToStream(entry.getValue(), linkedType, iEntityManager, iObj2RecHandler, db, iSaveOnlyDirty));
				}
			}
		}

		return result;
	}

	private static List<Field> getClassFields(final Class<?> iClass) {
		if (iClass.getPackage().getName().startsWith("java.lang"))
			return null;

		synchronized (classes) {
			if (classes.containsKey(iClass.getName()))
				return classes.get(iClass.getName());

			return analyzeClass(iClass);
		}
	}

	public static void invokeCallback(final Object iPojo, final ODocument iDocument, final Class<?> iAnnotation) {
		final Method m = callbacks.get(iPojo.getClass().getSimpleName() + "." + iAnnotation.getSimpleName());

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

	public static void bindSerializerContext(final Class<?> iClassContext, final OObjectSerializerContext iSerializerContext) {
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

		for (Class<?> currentClass = iClass; currentClass != Object.class;) {
			for (Field f : currentClass.getDeclaredFields()) {
				fieldModifier = f.getModifiers();
				if (Modifier.isStatic(fieldModifier) || Modifier.isNative(fieldModifier) || Modifier.isTransient(fieldModifier))
					continue;

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
				if (f.getAnnotation(OAccess.class) == null || f.getAnnotation(OAccess.class).value() == OAccess.OAccessType.PROPERTY)
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
				}
				// JPA 1+ AVAILABLE?
				else if (jpaIdClass != null && f.getAnnotation(jpaIdClass) != null) {
					// RECORD ID
					fieldIds.put(iClass, f);
					idFound = true;
				}
				if (idFound) {
					// CHECK FOR TYPE
					if (fieldType.isPrimitive())
						OLogManager.instance().warn(OObjectSerializerHelper.class, "Field '%s' can't be a literal to manage the Record Id",
								f.toString());
					else if (!ORID.class.isAssignableFrom(fieldType) && fieldType != String.class && fieldType != Object.class
							&& !Number.class.isAssignableFrom(fieldType))
						OLogManager.instance().warn(OObjectSerializerHelper.class, "Field '%s' can't be managed as type: %s", f.toString(),
								fieldType);
				}

				boolean vFound = false;
				if (f.getAnnotation(OVersion.class) != null) {
					// RECORD ID
					fieldVersions.put(iClass, f);
					vFound = true;
				}
				// JPA 1+ AVAILABLE?
				else if (jpaVersionClass != null && f.getAnnotation(jpaVersionClass) != null) {
					// RECORD ID
					fieldVersions.put(iClass, f);
					vFound = true;
				}
				if (vFound) {
					// CHECK FOR TYPE
					if (fieldType.isPrimitive())
						OLogManager.instance().warn(OObjectSerializerHelper.class, "Field '%s' can't be a literal to manage the Version",
								f.toString());
					else if (fieldType != String.class && fieldType != Object.class && !Number.class.isAssignableFrom(fieldType))
						OLogManager.instance().warn(OObjectSerializerHelper.class, "Field '%s' can't be managed as type: %s", f.toString(),
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
						String getterName = "get" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
						Method m = currentClass.getMethod(getterName, NO_ARGS);
						getters.put(iClass.getName() + "." + fieldName, m);
					} catch (Exception e) {
						registerFieldGetter(iClass, fieldName, f);
					}
				else
					registerFieldGetter(iClass, fieldName, f);

				if (autoBinding)
					// TRY TO GET THE VALUE BY THE SETTER (IF ANY)
					try {
						String getterName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
						Method m = currentClass.getMethod(getterName, f.getType());
						setters.put(iClass.getName() + "." + fieldName, m);
					} catch (Exception e) {
						registerFieldSetter(iClass, fieldName, f);
					}
				else
					registerFieldSetter(iClass, fieldName, f);
			}

			registerCallbacks(iClass, currentClass);

			currentClass = currentClass.getSuperclass();

			if (currentClass.equals(ODocument.class))
				// POJO EXTENDS ODOCUMENT: SPECIAL CASE: AVOID TO CONSIDER ODOCUMENT FIELDS
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
		if (!f.isAccessible())
			f.setAccessible(true);

		setters.put(iClass.getName() + "." + fieldName, f);
	}

	private static void registerFieldGetter(final Class<?> iClass, String fieldName, Field f) {
		// TRY TO GET THE VALUE BY ACCESSING DIRECTLY TO THE PROPERTY
		if (!f.isAccessible())
			f.setAccessible(true);

		getters.put(iClass.getName() + "." + fieldName, f);
	}

	private static OType getEmbeddetTypesByJPAAnnotation(final Class<?> iPojoClass, final Class<?> iFieldClass,
			final String iFieldName, final OEntityManager iEntityManager) {
		if (embeddedFields.get(iPojoClass) != null && embeddedFields.get(iPojoClass).contains(iFieldName))
			return OType.EMBEDDED;
		else if (iEntityManager.getEntityClass(iFieldClass.getSimpleName()) != null)
			return OType.LINK;
		return null;
	}
}
