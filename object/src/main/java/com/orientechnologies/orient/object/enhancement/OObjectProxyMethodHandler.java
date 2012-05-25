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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.orientechnologies.orient.object.db.OObjectLazyList;
import com.orientechnologies.orient.object.db.OObjectLazyMap;
import com.orientechnologies.orient.object.db.OObjectLazySet;
import com.orientechnologies.orient.object.serialization.OLazyObjectCustomSerializer;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerList;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerMap;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerSet;

/**
 * @author Luca Molino (molino.luca--at--gmail.com)
 * 
 */
public class OObjectProxyMethodHandler implements MethodHandler {

	protected ODocument							doc;

	protected Map<String, Integer>	loadedFields;

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

	public Object invoke(Object self, Method m, Method proceed, Object[] args) throws Throwable {
		if (isSetterMethod(m.getName(), m)) {
			return manageSetMethod(self, m, proceed, args);
		} else if (isGetterMethod(m.getName(), m)) {
			return manageGetMethod(self, m, proceed, args);
		} else if (m.getName().equals("equals") && args[0] != null && args[0] instanceof Proxy) {
			return ((Boolean) proceed.invoke(self, args)) && doc.equals(OObjectEntitySerializer.getDocument((Proxy) args[0]));
		} else if (m.getName().equals("hashCode")) {
			return doc.hashCode() + ((Integer) proceed.invoke(self, args)).intValue();
		}
		return proceed.invoke(self, args);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object manageGetMethod(Object self, Method m, Method proceed, Object[] args) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException, SecurityException, IllegalArgumentException, NoSuchFieldException {
		final String fieldName;
		fieldName = getFieldName(m);
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

		if (!idOrVersionField
				&& value != null
				&& ((Number.class.isAssignableFrom(value.getClass()) && ((Number) value).doubleValue() == 0d) || (Boolean.class
						.isAssignableFrom(value.getClass())))) {
			Object docValue = doc.field(fieldName);
			if (docValue != null && !docValue.equals(value)) {
				value = lazyLoadField(self, fieldName, docValue);
			}
		}

		if (!idOrVersionField) {
			if (value == null) {
				Object docValue = doc.field(fieldName, OType.getTypeByClass(getField(fieldName, self.getClass()).getType()));
				if (docValue != null) {
					value = lazyLoadField(self, fieldName, docValue);
				}
			} else {
				if (((value instanceof Collection<?> || value instanceof Map<?, ?>) && !(value instanceof OLazyObjectMultivalueElement))
						|| value.getClass().isArray()) {
					Class<?> genericMultiValueType = OReflectionHelper.getGenericMultivalueType(getField(fieldName, self.getClass()));
					if (genericMultiValueType != null && !OReflectionHelper.isJavaType(genericMultiValueType)) {
						if (OObjectEntitySerializer.isSerializedType(getField(fieldName, self.getClass()))
								&& !(value instanceof OLazyObjectCustomSerializer)) {
							manageSerializedCollections(self, fieldName, value);
						} else {
							value = manageObjectCollections(self, fieldName, value);
						}
					} else {
						Object docValue = doc.field(fieldName);
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
							value = manageArrayFieldObject(fieldName, self, docValue);
							Method setMethod = getSetMethod(self.getClass().getSuperclass(), getSetterFieldName(fieldName), value);
							setMethod.invoke(self, value);
						} else if ((value instanceof Set || value instanceof Map) && loadedFields.get(fieldName).intValue() < doc.getVersion()) {
							if (value instanceof Set)
								value = new OObjectLazySet(doc, (Set<?>) docValue);
							else
								value = new OObjectLazyMap(doc, (Map<?, ?>) docValue);
							Method setMethod = getSetMethod(self.getClass().getSuperclass(), getSetterFieldName(fieldName), value);
							setMethod.invoke(self, value);
						}
					}
				}
			}
		}
		if (doc.getIdentity().isValid() && !doc.getIdentity().isTemporary())
			loadedFields.put(fieldName, doc.getVersion());
		else
			loadedFields.put(fieldName, 0);
		return value;
	}

	protected Object manageObjectCollections(Object self, final String fieldName, Object value) throws NoSuchMethodException,
			IllegalAccessException, InvocationTargetException {
		if (value instanceof Collection<?>) {
			value = manageCollectionSave(fieldName, (Collection<?>) value);
		} else if (value instanceof Map<?, ?>) {
			value = manageMapSave(fieldName, (Map<?, ?>) value);
		} else if (value.getClass().isArray()) {
			value = manageArraySave(fieldName, (Object[]) value);
		}
		OObjectEntitySerializer.setFieldValue(getField(fieldName, self.getClass()), self, value);
		return value;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Object manageSerializedCollections(Object self, final String fieldName, Object value) throws NoSuchMethodException,
			IllegalAccessException, InvocationTargetException {
		if (value instanceof Collection<?>) {
			if (value instanceof List) {
				List<Object> docList = doc.field(fieldName);
				if (docList == null) {
					docList = new ArrayList<Object>();
					doc.field(fieldName, docList);
				}
				value = new OObjectCustomSerializerList(OObjectEntitySerializer.getSerializedType(getField(fieldName, self.getClass())),
						doc, docList, (List<?>) value);
			} else if (value instanceof Set) {
				Set<Object> docSet = doc.field(fieldName, OType.LINKSET);
				if (docSet == null) {
					docSet = new HashSet<Object>();
					doc.field(fieldName, docSet);
				}
				value = new OObjectCustomSerializerSet(OObjectEntitySerializer.getSerializedType(getField(fieldName, self.getClass())),
						doc, docSet, (Set<?>) value);
			}
		} else if (value instanceof Map<?, ?>) {
			Map<Object, Object> docMap = doc.field(fieldName);
			if (docMap == null) {
				docMap = new HashMap<Object, Object>();
				doc.field(fieldName, docMap);
			}
			value = new OObjectCustomSerializerMap(OObjectEntitySerializer.getSerializedType(getField(fieldName, self.getClass())), doc,
					docMap, (Map<?, ?>) value);
		} else if (value.getClass().isArray()) {
			value = manageArraySave(fieldName, (Object[]) value);
		}
		OObjectEntitySerializer.setFieldValue(getField(fieldName, self.getClass()), self, value);
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
	protected Object manageMapSave(String iFieldName, Map<?, ?> value) {
		if (!(value instanceof OLazyObjectMultivalueElement)) {
			Map<Object, OIdentifiable> docMap = doc.field(iFieldName);
			if (docMap == null) {
				docMap = new ORecordLazyMap(doc);
				doc.field(iFieldName, docMap);
			}
			value = new OObjectLazyMap(doc, docMap, value);
		}
		return value;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Object manageCollectionSave(String iFieldName, Collection<?> value) {
		if (!(value instanceof OLazyObjectMultivalueElement)) {
			if (value instanceof List) {
				List<OIdentifiable> docList = doc.field(iFieldName);
				if (docList == null) {
					docList = new ORecordLazyList(doc);
					doc.field(iFieldName, docList);
				}
				value = new OObjectLazyList(doc, docList, value);
			} else if (value instanceof Set) {
				Set<OIdentifiable> docSet = doc.field(iFieldName, OType.LINKSET);
				if (docSet == null) {
					docSet = new ORecordLazySet(doc);
					doc.field(iFieldName, docSet);
				}
				value = new OObjectLazySet(doc, docSet, (Set<?>) value);
			}
		}
		if (!((ODatabaseObject) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner()).isLazyLoading())
			((OLazyObjectMultivalueElement) value).detach();
		return value;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object lazyLoadField(Object self, final String fieldName, Object docValue) throws NoSuchMethodException,
			IllegalAccessException, InvocationTargetException {
		boolean customSerialization = false;
		if (OObjectEntitySerializer.isSerializedType(getField(fieldName, self.getClass()))) {
			customSerialization = true;
		}
		Field f = getField(fieldName, self.getClass());
		if (docValue instanceof OIdentifiable) {
			docValue = convertDocumentToObject((ODocument) ((OIdentifiable) docValue).getRecord());
		} else if (docValue instanceof Collection<?>) {
			docValue = manageCollectionLoad(fieldName, self, docValue, customSerialization);
		} else if (docValue instanceof Map<?, ?>) {
			docValue = manageMapLoad(fieldName, self, docValue, customSerialization);
		} else if (docValue.getClass().isArray() && !docValue.getClass().getComponentType().isPrimitive()) {
			docValue = manageArrayLoad(docValue);
		} else if (customSerialization) {
			docValue = OObjectEntitySerializer.deserializeFieldValue(getField(fieldName, self.getClass()).getType(), docValue);
		} else {
			if (f.getType().isEnum()) {
				if (docValue instanceof Number)
					docValue = ((Class<Enum>) f.getType()).getEnumConstants()[((Number) docValue).intValue()];
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
	protected Object manageMapLoad(String fieldName, Object self, Object value, boolean customSerialization) {
		if (value instanceof ORecordLazyMap) {
			value = new OObjectLazyMap(doc, (ORecordLazyMap) value);
		} else if (customSerialization) {
			value = new OObjectCustomSerializerMap<TYPE>(OObjectEntitySerializer.getSerializedType(getField(fieldName, self.getClass())),
					doc, (Map<Object, Object>) value);
		}
		return value;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Object manageCollectionLoad(String fieldName, Object self, Object value, boolean customSerialization) {
		if (value instanceof ORecordLazyList) {
			value = new OObjectLazyList(doc, (ORecordLazyList) value);
		} else if (value instanceof ORecordLazySet || value instanceof OMVRBTreeRIDSet) {
			value = new OObjectLazySet(doc, (Set) value);
		} else if (customSerialization) {
			if (value instanceof List<?>) {
				value = new OObjectCustomSerializerList(OObjectEntitySerializer.getSerializedType(getField(fieldName, self.getClass())),
						doc, (List<Object>) value);
			} else {
				value = new OObjectCustomSerializerSet(OObjectEntitySerializer.getSerializedType(getField(fieldName, self.getClass())),
						doc, (Set<Object>) value);
			}
		}

		return manageArrayFieldObject(fieldName, self, value);
	}

	protected Object manageArrayFieldObject(String fieldName, Object self, Object value) {
		final Field field = getField(fieldName, self.getClass());
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

	protected Object convertDocumentToObject(ODocument value) {
		return OObjectEntityEnhancer.getInstance().getProxiedInstance(value.getClassName(), getDatabase().getEntityManager(), value);
	}

	protected Object manageSetMethod(Object self, Method m, Method proceed, Object[] args) throws IllegalAccessException,
			InvocationTargetException {
		final String fieldName;
		fieldName = getFieldName(m);
		Object valueToSet = args[0];
		if (valueToSet == null) {
			doc.field(fieldName, valueToSet);
		} else if (valueToSet instanceof Proxy) {
			doc.field(fieldName, OObjectEntitySerializer.getDocument((Proxy) valueToSet));
		} else if (((valueToSet instanceof Collection<?> || valueToSet instanceof Map<?, ?>)) || valueToSet.getClass().isArray()) {
			Class<?> genericMultiValueType = OReflectionHelper.getGenericMultivalueType(getField(fieldName, self.getClass()));
			if (genericMultiValueType != null && !OReflectionHelper.isJavaType(genericMultiValueType)) {
				if (!(valueToSet instanceof OLazyObjectMultivalueElement)) {
					if (valueToSet instanceof Collection<?>) {
						valueToSet = manageCollectionSave(fieldName, (Collection<?>) valueToSet);
					} else if (valueToSet instanceof Map<?, ?>) {
						valueToSet = manageMapSave(fieldName, (Map<?, ?>) valueToSet);
					} else if (valueToSet.getClass().isArray()) {
						valueToSet = manageArraySave(fieldName, (Object[]) valueToSet);
					}
				}
			} else {
				if (OObjectEntitySerializer.isToSerialize(valueToSet.getClass())) {
					doc.field(fieldName,
							OObjectEntitySerializer.serializeFieldValue(getField(fieldName, self.getClass()).getType(), valueToSet));
				} else {
					if (valueToSet.getClass().isArray()) {
						OClass schemaClass = doc.getSchemaClass();
						OProperty schemaProperty = null;
						if (schemaClass != null)
							schemaProperty = schemaClass.getProperty(fieldName);

						doc.field(fieldName, OObjectEntitySerializer.typeToStream(valueToSet, schemaProperty != null ? schemaProperty.getType()
								: null, getDatabase(), doc));
					} else
						doc.field(fieldName, valueToSet);
				}
			}
		} else {
			if (OObjectEntitySerializer.isToSerialize(valueToSet.getClass())) {
				doc.field(fieldName,
						OObjectEntitySerializer.serializeFieldValue(getField(fieldName, self.getClass()).getType(), valueToSet));
			} else {
				doc.field(fieldName, valueToSet);
			}
		}
		args[0] = valueToSet;
		loadedFields.put(fieldName, doc.getVersion());
		return proceed.invoke(self, args);
	}

	protected boolean isSetterMethod(String fieldName, Method m) {
		if (!fieldName.startsWith("set") || !checkIfFirstCharAfterPrefixIsUpperCase(fieldName, "set"))
			return false;
		if (m.getParameterTypes() != null && m.getParameterTypes().length != 1)
			return false;
		return true;
	}

	protected boolean isGetterMethod(String fieldName, Method m) {
		int prefixLength;
		if (fieldName.startsWith("get") && checkIfFirstCharAfterPrefixIsUpperCase(fieldName, "get"))
			prefixLength = "get".length();
		else if (fieldName.startsWith("is") && checkIfFirstCharAfterPrefixIsUpperCase(fieldName, "is"))
			prefixLength = "is".length();
		else
			return false;
		if (m.getParameterTypes() != null && m.getParameterTypes().length > 0)
			return false;
		if (fieldName.length() <= prefixLength)
			return false;
		return true;
	}

	protected boolean checkIfFirstCharAfterPrefixIsUpperCase(String methodName, String prefix) {
		return methodName.length() > prefix.length() ? Character.isUpperCase(methodName.charAt(prefix.length())) : false;
	}

	protected String getFieldName(Method m) {
		if (m.getName().startsWith("get"))
			return getFieldName(m.getName(), "get");
		else if (m.getName().startsWith("set"))
			return getFieldName(m.getName(), "set");
		else
			return getFieldName(m.getName(), "is");
	}

	protected String getFieldName(String methodName, String prefix) {
		StringBuffer fieldName = new StringBuffer();
		fieldName.append(Character.toLowerCase(methodName.charAt(prefix.length())));
		for (int i = (prefix.length() + 1); i < methodName.length(); i++) {
			fieldName.append(methodName.charAt(i));
		}
		return fieldName.toString();
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

	protected Field getField(String fieldName, Class<?> iClass) {
		for (Field f : iClass.getDeclaredFields()) {
			if (f.getName().equals(fieldName))
				return f;
		}
		if (iClass.getSuperclass().equals(Object.class))
			return null;
		return getField(fieldName, iClass.getSuperclass());
	}

	private ODatabaseObject getDatabase() {
		return (ODatabaseObject) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();
	}
}
