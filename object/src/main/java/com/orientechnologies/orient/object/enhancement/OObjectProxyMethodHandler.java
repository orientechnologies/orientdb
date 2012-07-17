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

	/**
	 * Method that detaches all fields contained in the document to the given object
	 * 
	 * @param self
	 *          :- The object containing this handler instance
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 * @throws NoSuchMethodException
	 */
	public void detach(Object self) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		for (String fieldName : doc.fieldNames()) {
			Object value = getValue(self, fieldName, false, null);
			if (value instanceof OLazyObjectMultivalueElement)
				((OLazyObjectMultivalueElement) value).detach();
			OObjectEntitySerializer.setFieldValue(getField(fieldName, self.getClass()), self, value);
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
				Object docValue = doc.field(fieldName, OType.getTypeByClass(getField(fieldName, self.getClass()).getType()));
				if (docValue != null) {
					value = lazyLoadField(self, fieldName, docValue);
				}
			} else {
				if (((value instanceof Collection<?> || value instanceof Map<?, ?>) && !(value instanceof OLazyObjectMultivalueElement))
						|| value.getClass().isArray()) {
					Class<?> genericMultiValueType = OReflectionHelper.getGenericMultivalueType(getField(fieldName, self.getClass()));
					if (genericMultiValueType != null && !OReflectionHelper.isJavaType(genericMultiValueType)) {
						Field f = getField(fieldName, self.getClass());
						if (OObjectEntitySerializer.isSerializedType(f) && !(value instanceof OLazyObjectCustomSerializer)) {
							value = manageSerializedCollections(self, fieldName, value);
						} else if (genericMultiValueType.isEnum() && !(value instanceof OLazyObjectEnumSerializer)) {
							value = manageEnumCollections(self, f.getName(), genericMultiValueType, value);
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
							value = manageArrayFieldObject(getField(fieldName, self.getClass()), self, docValue);
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
				} else if (!loadedFields.containsKey(fieldName)) {
					Object docValue = doc.field(fieldName);
					if (docValue != null && !docValue.equals(value)) {
						value = lazyLoadField(self, fieldName, docValue);
					}
				}
			}
		}
		return value;
	}

	protected Object manageObjectCollections(Object self, final String fieldName, Object value) throws NoSuchMethodException,
			IllegalAccessException, InvocationTargetException {
		boolean customSerialization = false;
		Field f = getField(fieldName, self.getClass());
		if (OObjectEntitySerializer.isSerializedType(f)) {
			customSerialization = true;
		}
		if (value instanceof Collection<?>) {
			value = manageCollectionSave(f, (Collection<?>) value, customSerialization);
		} else if (value instanceof Map<?, ?>) {
			value = manageMapSave(f, (Map<?, ?>) value, customSerialization);
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Object manageEnumCollections(Object self, final String fieldName, final Class<?> enumClass, Object value)
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		if (value instanceof Collection<?>) {
			if (value instanceof List) {
				List<Object> docList = doc.field(fieldName);
				if (docList == null) {
					docList = new ArrayList<Object>();
					doc.field(fieldName, docList);
				}
				value = new OObjectEnumLazyList(enumClass, doc, docList, (List<?>) value);
			} else if (value instanceof Set) {
				Set<Object> docSet = doc.field(fieldName, OType.LINKSET);
				if (docSet == null) {
					docSet = new HashSet<Object>();
					doc.field(fieldName, docSet);
				}
				value = new OObjectEnumLazySet(enumClass, doc, docSet, (Set<?>) value);
			}
		} else if (value instanceof Map<?, ?>) {
			Map<Object, Object> docMap = doc.field(fieldName);
			if (docMap == null) {
				docMap = new HashMap<Object, Object>();
				doc.field(fieldName, docMap);
			}
			value = new OObjectEnumLazyMap(enumClass, doc, docMap, (Map<?, ?>) value);
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
	protected Object manageMapSave(Field f, Map<?, ?> value, boolean customSerialization) {
		final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
		if (customSerialization) {
			Map<Object, Object> map = new HashMap<Object, Object>();
			doc.field(f.getName(), map);
			value = new OObjectCustomSerializerMap<TYPE>(OObjectEntitySerializer.getSerializedType(f), doc, map,
					(Map<Object, Object>) value);
		} else if (genericType.isEnum()) {
			Map<Object, Object> map = new HashMap<Object, Object>();
			doc.field(f.getName(), map);
			value = new OObjectEnumLazyMap(genericType, doc, map, (Map<Object, Object>) value);
		} else if (!(value instanceof OLazyObjectMultivalueElement)) {
			Map<Object, OIdentifiable> docMap = doc.field(f.getName());
			if (docMap == null) {
				docMap = new ORecordLazyMap(doc);
				doc.field(f.getName(), docMap);
			}
			value = new OObjectLazyMap(doc, docMap, value);
		}
		return value;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Object manageCollectionSave(Field f, Collection<?> value, boolean customSerialization) {
		final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
		if (customSerialization) {
			if (value instanceof List<?>) {
				List<Object> list = new ArrayList<Object>();
				doc.field(f.getName(), list);
				value = new OObjectCustomSerializerList(OObjectEntitySerializer.getSerializedType(f), doc, new ArrayList<Object>(),
						(List<Object>) value);
			} else {
				Set<Object> set = new HashSet<Object>();
				doc.field(f.getName(), set);
				value = new OObjectCustomSerializerSet(OObjectEntitySerializer.getSerializedType(f), doc, set, (Set<Object>) value);
			}
		} else if (genericType.isEnum()) {
			if (value instanceof List<?>) {
				List<Object> list = new ArrayList<Object>();
				doc.field(f.getName(), list);
				value = new OObjectEnumLazyList(genericType, doc, list, (List<Object>) value);
			} else {
				Set<Object> set = new HashSet<Object>();
				doc.field(f.getName(), set);
				value = new OObjectEnumLazySet(genericType, doc, set, (Set<Object>) value);
			}
		} else if (!(value instanceof OLazyObjectMultivalueElement)) {
			if (value instanceof List) {
				List<OIdentifiable> docList = doc.field(f.getName());
				if (docList == null) {
					docList = new ORecordLazyList(doc);
					doc.field(f.getName(), docList);
				}
				value = new OObjectLazyList(doc, docList, value);
			} else if (value instanceof Set) {
				Set<OIdentifiable> docSet = doc.field(f.getName(), OType.LINKSET);
				if (docSet == null) {
					docSet = new ORecordLazySet(doc);
					doc.field(f.getName(), docSet);
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
		Field f = getField(fieldName, self.getClass());
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
				docValue = convertDocumentToObject((ODocument) ((OIdentifiable) docValue).getRecord());
			}
		} else if (docValue instanceof Collection<?>) {
			docValue = manageCollectionLoad(f, self, docValue, customSerialization);
		} else if (docValue instanceof Map<?, ?>) {
			docValue = manageMapLoad(f, self, docValue, customSerialization);
		} else if (docValue.getClass().isArray() && !docValue.getClass().getComponentType().isPrimitive()) {
			docValue = manageArrayLoad(docValue);
		} else if (customSerialization) {
			docValue = OObjectEntitySerializer.deserializeFieldValue(getField(fieldName, self.getClass()).getType(), docValue);
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
			value = new OObjectLazyMap(doc, (ORecordLazyMap) value);
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
			value = new OObjectLazyList(doc, (ORecordLazyList) value);
		} else if (value instanceof ORecordLazySet || value instanceof OMVRBTreeRIDSet) {
			value = new OObjectLazySet(doc, (Set) value);
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

	protected Object convertDocumentToObject(ODocument value) {
		return OObjectEntityEnhancer.getInstance().getProxiedInstance(value.getClassName(), getDatabase().getEntityManager(), value);
	}

	protected Object manageSetMethod(Object self, Method m, Method proceed, Object[] args) throws IllegalAccessException,
			InvocationTargetException {
		final String fieldName;
		fieldName = getFieldName(m);
		args[0] = setValue(self, fieldName, args[0]);
		return proceed.invoke(self, args);
	}

	protected Object setValue(Object self, final String fieldName, Object valueToSet) {
		if (valueToSet == null) {
			doc.field(fieldName, valueToSet);
		} else if (!valueToSet.getClass().isAnonymousClass()) {
			if (valueToSet instanceof Proxy) {
				ODocument docToSet = OObjectEntitySerializer.getDocument((Proxy) valueToSet);
				if (OObjectEntitySerializer.isEmbeddedField(self.getClass(), fieldName))
					docToSet.addOwner(doc);
				doc.field(fieldName, docToSet);
			} else if (valueToSet instanceof OIdentifiable) {
				if (valueToSet instanceof ODocument && OObjectEntitySerializer.isEmbeddedField(self.getClass(), fieldName))
					((ODocument) valueToSet).addOwner(doc);
				doc.field(fieldName, valueToSet);
			} else if (((valueToSet instanceof Collection<?> || valueToSet instanceof Map<?, ?>)) || valueToSet.getClass().isArray()) {
				Class<?> genericMultiValueType = OReflectionHelper.getGenericMultivalueType(getField(fieldName, self.getClass()));
				if (genericMultiValueType != null && !OReflectionHelper.isJavaType(genericMultiValueType)) {
					if (!(valueToSet instanceof OLazyObjectMultivalueElement)) {
						if (valueToSet instanceof Collection<?>) {
							boolean customSerialization = false;
							Field f = getField(fieldName, self.getClass());
							if (OObjectEntitySerializer.isSerializedType(f)) {
								customSerialization = true;
							}
							valueToSet = manageCollectionSave(f, (Collection<?>) valueToSet, customSerialization);
						} else if (valueToSet instanceof Map<?, ?>) {
							boolean customSerialization = false;
							Field f = getField(fieldName, self.getClass());
							if (OObjectEntitySerializer.isSerializedType(f)) {
								customSerialization = true;
							}
							valueToSet = manageMapSave(f, (Map<?, ?>) valueToSet, customSerialization);
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

							doc.field(fieldName, OObjectEntitySerializer.typeToStream(valueToSet,
									schemaProperty != null ? schemaProperty.getType() : null, getDatabase(), doc));
						} else
							doc.field(fieldName, valueToSet);
					}
				}
			} else if (valueToSet.getClass().isEnum()) {
				doc.field(fieldName, ((Enum) valueToSet).name());
			} else {
				if (OObjectEntitySerializer.isToSerialize(valueToSet.getClass())) {
					doc.field(fieldName,
							OObjectEntitySerializer.serializeFieldValue(getField(fieldName, self.getClass()).getType(), valueToSet));
				} else if (getDatabase().getEntityManager().getEntityClass(valueToSet.getClass().getSimpleName()) != null
						&& !valueToSet.getClass().isEnum()) {
					valueToSet = OObjectEntitySerializer.serializeObject(valueToSet, getDatabase());
					ODocument docToSet = OObjectEntitySerializer.getDocument((Proxy) valueToSet);
					if (OObjectEntitySerializer.isEmbeddedField(self.getClass(), fieldName))
						docToSet.addOwner(doc);
					doc.field(fieldName, docToSet);
				} else {
					doc.field(fieldName, valueToSet);
				}
			}
			loadedFields.put(fieldName, doc.getVersion());
		} else {
			OLogManager.instance().warn(this,
					"Setting property '%s' in proxied class '%s' with an anonymous class '%s'. The document won't have this property.",
					fieldName, self.getClass().getName(), valueToSet.getClass().getName());
		}
		return valueToSet;
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
