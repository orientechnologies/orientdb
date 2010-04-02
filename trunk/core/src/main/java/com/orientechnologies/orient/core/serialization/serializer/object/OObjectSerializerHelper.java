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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordVObject;

@SuppressWarnings("unchecked")
public class OObjectSerializerHelper {
	private static final Class<?>[]	NO_ARGS	= new Class<?>[] {};

	public static Object getFieldValue(final Object iPojo, final String iProperty) {
		Class<?> c = iPojo.getClass();

		// TRY TO GET THE VALUE BY THE GETTER (IF ANY)
		try {
			String getterName = "get" + Character.toUpperCase(iProperty.charAt(0)) + iProperty.substring(1);
			Method m = c.getMethod(getterName, NO_ARGS);
			return m.invoke(iPojo);
		} catch (Exception e) {
			// TRY TO GET THE VALUE BY ACCESSING DIRECTLY TO THE PROPERTY
			try {
				Field f = c.getDeclaredField(iProperty);
				if (!f.isAccessible())
					f.setAccessible(true);

				return f.get(iPojo);
			} catch (Exception e1) {
				throw new OSchemaException("Can't access to the property: " + iProperty, e1);
			}
		}
	}

	public static void setFieldValue(final Object iPojo, final String iProperty, final Object iValue) {
		Class<?> c = iPojo.getClass();

		// TRY TO SET THE VALUE BY THE SETTER (IF ANY)
		try {
			String setterName = "set" + Character.toUpperCase(iProperty.charAt(0)) + iProperty.substring(1);
			Method m = c.getMethod(setterName, new Class<?>[] { iValue.getClass() });
			m.invoke(iPojo, iValue);
		} catch (Exception e) {
			// TRY TO SET THE VALUE BY ACCESSING DIRECTLY TO THE PROPERTY
			try {
				Field f = c.getDeclaredField(iProperty);
				if (!f.isAccessible())
					f.setAccessible(true);

				f.set(iPojo, iValue);
			} catch (Exception e1) {
				throw new OSchemaException("Can't access to the property: " + iProperty, e1);
			}
		}
	}

	public static Object fromStream(final ORecordVObject iRecord, final Object iPojo, final OEntityManager iEntityManager,
			final OUserObject2RecordHandler iObj2RecHandler) {
		long timer = OProfiler.getInstance().startChrono();

		Class<?> c = iPojo.getClass();

		Object fieldValue;
		Class<?> fieldClass;
		for (Field f : c.getDeclaredFields()) {
			fieldValue = iRecord.field(f.getName());

			if (fieldValue instanceof ORecordVObject && !OType.isSimpleType(f.getClass())) {
				fieldClass = iEntityManager.getEntityClass(f.getType().getSimpleName());
				if (fieldClass != null) {
					// RECOGNIZED TYPE
					fieldValue = iObj2RecHandler.getUserObjectByRecord((ORecord<?>) fieldValue);
				}
			}

			setFieldValue(iPojo, f.getName(), fieldValue);
		}

		OProfiler.getInstance().stopChrono("Object.fromStream", timer);

		return iPojo;
	}

	/**
	 * Serialize the user POJO to a ORecordVObject instance.
	 * 
	 * @param iPojo
	 *          User pojo to serialize
	 * @param iRecord
	 *          Record where to update
	 * @param iObj2RecHandler
	 */
	public static ORecordVObject toStream(final Object iPojo, final ORecordVObject iRecord, final OEntityManager iEntityManager,
			final OClass schemaClass, final OUserObject2RecordHandler iObj2RecHandler) {
		long timer = OProfiler.getInstance().startChrono();

		OProperty schemaProperty;

		Class<?> c = iPojo.getClass();

		Object fieldValue;
		int fieldModifier;
		for (Field f : c.getDeclaredFields()) {
			fieldModifier = f.getModifiers();
			if (Modifier.isStatic(fieldModifier) || Modifier.isNative(fieldModifier) || Modifier.isTransient(fieldModifier))
				continue;

			fieldValue = getFieldValue(iPojo, f.getName());

			schemaProperty = schemaClass != null ? schemaClass.getProperty(f.getName()) : null;

			fieldValue = typeToStream(fieldValue, schemaProperty != null ? schemaProperty.getType() : null, iEntityManager,
					iObj2RecHandler);

			iRecord.field(f.getName(), fieldValue);
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
				iFieldValue = listToStream((List<Object>) iFieldValue, iType, iEntityManager, iObj2RecHandler);
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
}
