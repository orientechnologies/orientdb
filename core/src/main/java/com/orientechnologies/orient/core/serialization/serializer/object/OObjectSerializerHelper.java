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
import java.util.Set;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.fetch.OFetchHelper;
import com.orientechnologies.orient.core.id.fetch.OFetchListener;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord.STATUS;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings("unchecked")
/**
 * Helper class to manage POJO by using the reflection. 
 */
public class OObjectSerializerHelper {
	private static final Class<?>[]							NO_ARGS	= new Class<?>[] {};

	private static HashMap<String, List<Field>>	classes	= new HashMap<String, List<Field>>();
	private static HashMap<String, Object>			getters	= new HashMap<String, Object>();
	private static HashMap<String, Object>			setters	= new HashMap<String, Object>();

	public static boolean hasField(final Object iPojo, final String iProperty) {
		final Class<?> c = iPojo.getClass();
		final String className = c.getName();

		if (!classes.containsKey(className))
			registerClass(c);

		return getters.get(className + "." + iProperty) != null;
	}

	public static Class<?> getFieldType(final Object iPojo, final String iProperty) {
		final Class<?> c = iPojo.getClass();
		final String className = c.getName();

		if (!classes.containsKey(className))
			registerClass(c);

		try {
			Object o = getters.get(className + "." + iProperty);

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

	public static Object getFieldValue(final Object iPojo, final String iProperty) {
		final Class<?> c = iPojo.getClass();
		final String className = c.getName();

		if (!classes.containsKey(className))
			registerClass(c);

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

		if (!classes.containsKey(className))
			registerClass(c);

		try {
			Object o = setters.get(className + "." + iProperty);

			if (o instanceof Method) {
				((Method) o).invoke(iPojo, OType.convert(iValue, ((Method) o).getParameterTypes()[0]));
			} else if (o instanceof Field) {
				((Field) o).set(iPojo, OType.convert(iValue, ((Field) o).getType()));
			}

		} catch (Exception e) {

			throw new OSchemaException("Can't set the value '" + iValue + "' to the property: " + iProperty, e);
		}
	}

	public static Object fromStream(final ODocument iRecord, final Object iPojo, final OEntityManager iEntityManager,
			final OUserObject2RecordHandler iObj2RecHandler, final String iFetchPlan) {
		final long timer = OProfiler.getInstance().startChrono();

		final Class<?> c = iPojo.getClass();

		List<Field> properties = classes.get(c.getName());
		if (properties == null)
			properties = registerClass(c);

		String fieldName;
		Object fieldValue;

		if (iRecord.getStatus() == STATUS.NOT_LOADED)
			iRecord.load();

		// BIND BASIC FIELDS, LINKS WILL BE BOUND BY THE FETCH API
		for (Field p : properties) {
			fieldName = p.getName();
			fieldValue = iRecord.field(fieldName);

			if (fieldValue == null || !(fieldValue instanceof ODocument) || !(fieldValue instanceof Collection<?>)
					|| ((Collection<?>) fieldValue).size() == 0 || !(((Collection<?>) fieldValue).iterator().next() instanceof ODocument))
				setFieldValue(iPojo, fieldName, fieldValue);
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
				final Class<?> type = getFieldType(iUserObject, iFieldName);

				Object fieldValue = null;
				Class<?> fieldClass;

				if (type.isAssignableFrom(List.class)) {

					Collection<ODocument> list = (Collection<ODocument>) iLinked;
					final List<Object> targetList = new OLazyList<Object>((ODatabaseObjectTx) iRecord.getDatabase().getDatabaseOwner())
							.setFetchPlan(iFetchPlan);
					fieldValue = targetList;

					if (list != null && list.size() > 0) {
						targetList.addAll(list);
					}

				} else if (type.isAssignableFrom(Set.class)) {

					final Collection<ODocument> set = (Collection<ODocument>) iLinked;
					final Set<Object> target = new OLazySet<Object>((ODatabaseObjectTx) iRecord.getDatabase().getDatabaseOwner(), set)
							.setFetchPlan(iFetchPlan);

					fieldValue = target;
				} else {

					fieldClass = iEntityManager.getEntityClass(type.getSimpleName());
					if (fieldClass != null) {
						// RECOGNIZED TYPE
						fieldValue = iObj2RecHandler.getUserObjectByRecord((ODocument) iLinked, iFetchPlan);
					}
				}

				Object prevValue = getFieldValue(iPojo, iFieldName);

				if (prevValue == null && fieldValue != null || fieldValue != null && !fieldValue.equals(prevValue)) {
					setFieldValue(iPojo, iFieldName, fieldValue);
					return fieldValue;
				}
				
				// NOT CHANGED
				return null;
			}
		});

		OProfiler.getInstance().stopChrono("Object.fromStream", timer);

		return iPojo;
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
			final OClass schemaClass, final OUserObject2RecordHandler iObj2RecHandler) {
		long timer = OProfiler.getInstance().startChrono();

		OProperty schemaProperty;

		Class<?> c = iPojo.getClass();

		List<Field> properties = classes.get(c.getName());
		if (properties == null)
			properties = registerClass(c);

		String fieldName;
		Object fieldValue;

		for (Field p : properties) {
			fieldName = p.getName();
			fieldValue = getFieldValue(iPojo, fieldName);

			schemaProperty = schemaClass != null ? schemaClass.getProperty(fieldName) : null;

			fieldValue = typeToStream(fieldValue, schemaProperty != null ? schemaProperty.getType() : null, iEntityManager,
					iObj2RecHandler);

			iRecord.field(fieldName, fieldValue);
		}

		OProfiler.getInstance().stopChrono("Object.toStream", timer);

		return iRecord;
	}

	private static Object typeToStream(Object iFieldValue, final OType iType, final OEntityManager iEntityManager,
			final OUserObject2RecordHandler iObj2RecHandler) {
		if (iFieldValue == null)
			return null;

		Class<?> fieldClass = iFieldValue.getClass();

		if (!OType.isSimpleType(fieldClass)) {
			if (fieldClass.isArray()) {
				// ARRAY
				iFieldValue = listToStream(Arrays.asList(iFieldValue), iType, iEntityManager, iObj2RecHandler);
			} else if (Collection.class.isAssignableFrom(fieldClass)) {
				// COLLECTION (LIST OR SET)
				iFieldValue = listToStream((Collection<Object>) iFieldValue, iType, iEntityManager, iObj2RecHandler);
			} else if (Map.class.isAssignableFrom(fieldClass)) {
				// MAP
			} else {
				// LINK OR EMBEDDED
				fieldClass = iEntityManager.getEntityClass(fieldClass.getSimpleName());
				if (fieldClass != null) {
					// RECOGNIZED TYPE
					iFieldValue = iObj2RecHandler.getRecordByUserObject(iFieldValue, false);
				}
			}
		}
		return iFieldValue;
	}

	private static Collection<Object> listToStream(final Collection<Object> iCollection, OType iType,
			final OEntityManager iEntityManager, final OUserObject2RecordHandler iObj2RecHandler) {
		if (iType == null) {
			if (iCollection.size() == 0)
				return iCollection;

			// TRY TO UNDERSTAND THE COLLECTION TYPE BY ITS CONTENT
			Object firstValue = iCollection.iterator().next();

			if (firstValue == null)
				return iCollection;

			if (OType.isSimpleType(firstValue.getClass())) {
				iType = iCollection instanceof List ? OType.EMBEDDEDLIST : OType.EMBEDDEDSET;
			} else
				iType = iCollection instanceof List ? OType.LINKLIST : OType.LINKSET;
		}

		Collection<Object> result = null;
		final OType linkedType;

		if (iType.equals(OType.EMBEDDEDLIST) || iType.equals(OType.LINKLIST)) {
			result = new ArrayList<Object>();
		} else if (iType.equals(OType.EMBEDDEDSET) || iType.equals(OType.LINKSET)) {
			result = new HashSet<Object>();
		} else
			throw new IllegalArgumentException("Type " + iType + " must be a collection");

		if (iType.equals(OType.EMBEDDEDLIST) || iType.equals(OType.LINKLIST))
			linkedType = OType.LINK;
		else if (iType.equals(OType.EMBEDDEDSET) || iType.equals(OType.LINKSET))
			linkedType = OType.EMBEDDED;
		else
			throw new IllegalArgumentException("Type " + iType + " must be a collection");

		for (Object o : iCollection) {
			result.add(typeToStream(o, linkedType, iEntityManager, iObj2RecHandler));
		}

		return result;
	}

	private static List<Field> registerClass(final Class<?> iClass) {
		synchronized (classes) {
			if (classes.containsKey(iClass.getName()))
				return classes.get(iClass.getName());

			List<Field> properties = new ArrayList<Field>();
			classes.put(iClass.getName(), properties);

			String fieldName;
			int fieldModifier;

			for (Class<?> currentClass = iClass; currentClass != Object.class; currentClass = currentClass.getSuperclass())
				for (Field f : currentClass.getDeclaredFields()) {
					fieldModifier = f.getModifiers();
					if (Modifier.isStatic(fieldModifier) || Modifier.isNative(fieldModifier) || Modifier.isTransient(fieldModifier))
						continue;

					properties.add(f);

					fieldName = f.getName();

					// TRY TO GET THE VALUE BY THE GETTER (IF ANY)
					try {
						String getterName = "get" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
						Method m = currentClass.getMethod(getterName, NO_ARGS);
						getters.put(iClass.getName() + "." + fieldName, m);
					} catch (Exception e) {
						// TRY TO GET THE VALUE BY ACCESSING DIRECTLY TO THE PROPERTY
						if (!f.isAccessible())
							f.setAccessible(true);

						getters.put(iClass.getName() + "." + fieldName, f);
					}

					// TRY TO GET THE VALUE BY THE SETTER (IF ANY)
					try {
						String getterName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
						Method m = currentClass.getMethod(getterName, f.getType());
						setters.put(iClass.getName() + "." + fieldName, m);
					} catch (Exception e) {
						// TRY TO GET THE VALUE BY ACCESSING DIRECTLY TO THE PROPERTY
						if (!f.isAccessible())
							f.setAccessible(true);

						setters.put(iClass.getName() + "." + fieldName, f);
					}
				}
			return properties;
		}
	}
}
