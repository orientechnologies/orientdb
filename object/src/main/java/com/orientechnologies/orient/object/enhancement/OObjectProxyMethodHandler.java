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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.reflection.OReflectionHelper;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.object.OObjectLazyMultivalueElement;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.object.db.OObjectLazyList;
import com.orientechnologies.orient.object.db.OObjectLazyMap;
import com.orientechnologies.orient.object.db.OObjectLazySet;
import com.orientechnologies.orient.object.enhancement.field.ODocumentFieldHandler;
import com.orientechnologies.orient.object.enumerations.OObjectEnumLazyList;
import com.orientechnologies.orient.object.enumerations.OObjectEnumLazyMap;
import com.orientechnologies.orient.object.enumerations.OObjectEnumLazySet;
import com.orientechnologies.orient.object.enumerations.OObjectLazyEnumSerializer;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerList;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerMap;
import com.orientechnologies.orient.object.serialization.OObjectCustomSerializerSet;
import com.orientechnologies.orient.object.serialization.OObjectLazyCustomSerializer;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

/**
 * @author Luca Molino (molino.luca--at--gmail.com)
 *
 */
public class OObjectProxyMethodHandler implements MethodHandler {

    protected final Map<String, Integer> loadedFields;
    protected final Set<ORID> orphans = new HashSet<ORID>();
    protected ODocument doc;
    protected ProxyObject parentObject;

    public OObjectProxyMethodHandler(ODocument iDocument) {
        doc = iDocument;
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
        if (db.getDatabaseOwner() instanceof ODatabaseObject
                && !((ODatabaseObject) db.getDatabaseOwner()).isLazyLoading())
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

    public Object invoke(final Object self, final Method m, final Method proceed, final Object[] args)
            throws Throwable {
        final OObjectMethodFilter filter = OObjectEntityEnhancer.getInstance().getMethodFilter(self.getClass());
        if (filter.isSetterMethod(m)) {
            return manageSetMethod(self, m, proceed, args);
        } else if (filter.isGetterMethod(m)) {
            return manageGetMethod(self, m, proceed, args);
        }
        return proceed.invoke(self, args);
    }

    /**
     * Method that detaches all fields contained in the document to the given object
     *
     * @param self :- The object containing this handler instance
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     */
    public void detach(final Object self, final boolean nonProxiedInstance)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        final Class<?> selfClass = self.getClass();

        for (String fieldName : doc.fieldNames()) {
            Object value = getValue(self, fieldName, false, null, true);
            if (value instanceof OObjectLazyMultivalueElement) {
                ((OObjectLazyMultivalueElement<?>) value).detach(nonProxiedInstance);
                if (nonProxiedInstance)
                    value = ((OObjectLazyMultivalueElement<?>) value).getNonOrientInstance();
            }
            OObjectEntitySerializer.setFieldValue(OObjectEntitySerializer.getField(fieldName, selfClass), self, value);
        }
        OObjectEntitySerializer.setIdField(selfClass, self, doc.getIdentity());
        OObjectEntitySerializer.setVersionField(selfClass, self, doc.getVersion());
    }

    /**
     * Method that detaches all fields contained in the document to the given object
     *
     * @param self :- The object containing this handler instance
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     */
    public void detachAll(final Object self, final boolean nonProxiedInstance,
            final Map<Object, Object> alreadyDetached, final Map<Object, Object> lazyObjects)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        final Class<?> selfClass = self.getClass();

        for (String fieldName : doc.fieldNames()) {
            final Field field = OObjectEntitySerializer.getField(fieldName, selfClass);
            if (field != null) {
                Object value = getValue(self, fieldName, false, null, true);
                if (value instanceof OObjectLazyMultivalueElement) {
                    ((OObjectLazyMultivalueElement<?>) value).detachAll(nonProxiedInstance, alreadyDetached,
                            lazyObjects);
                    if (nonProxiedInstance)
                        value = ((OObjectLazyMultivalueElement<?>) value).getNonOrientInstance();
                } else if (value instanceof Proxy) {
                    OObjectProxyMethodHandler handler = (OObjectProxyMethodHandler) ((ProxyObject) value).getHandler();
                    if (nonProxiedInstance) {
                        value = OObjectEntitySerializer.getNonProxiedInstance(value);
                    }

                    if (OObjectEntitySerializer.isFetchLazyField(self.getClass(), fieldName)) {
                        // just make a placeholder with only the id, so it can be fetched later (but not by orient
                        // internally)
                        // do not use the already detached map for this, that might mix up lazy and non-lazy objects
                        Object lazyValue = lazyObjects.get(handler.doc.getIdentity());
                        if (lazyValue != null) {
                            value = lazyValue;
                        } else {
                            OObjectEntitySerializer.setIdField(field.getType(), value, handler.doc.getIdentity());
                            lazyObjects.put(handler.doc.getIdentity(), value);
                        }
                    } else {
                        Object detachedValue = alreadyDetached.get(handler.doc.getIdentity());
                        if (detachedValue != null) {
                            value = detachedValue;
                        } else {
                            ORID identity = handler.doc.getIdentity();
                            if (identity.isValid())
                                alreadyDetached.put(identity, value);
                            handler.detachAll(value, nonProxiedInstance, alreadyDetached, lazyObjects);
                        }
                    }
                }
                OObjectEntitySerializer.setFieldValue(field, self, value);
            }
        }
        OObjectEntitySerializer.setIdField(selfClass, self, doc.getIdentity());
        OObjectEntitySerializer.setVersionField(selfClass, self, doc.getVersion());
    }

    /**
     * Method that attaches all data contained in the object to the associated document
     *
     *
     * @param self :- The object containing this handler instance
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     */
    public void attach(final Object self)
            throws IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        for (Class<?> currentClass = self.getClass(); currentClass != Object.class;) {
            if (Proxy.class.isAssignableFrom(currentClass)) {
                currentClass = currentClass.getSuperclass();
                continue;
            }
            for (Field f : currentClass.getDeclaredFields()) {
                final String fieldName = f.getName();
                final Class<?> declaringClass = f.getDeclaringClass();

                if (OObjectEntitySerializer.isTransientField(declaringClass, fieldName)
                        || OObjectEntitySerializer.isVersionField(declaringClass, fieldName)
                        || OObjectEntitySerializer.isIdField(declaringClass, fieldName))
                    continue;

                Object value = OObjectEntitySerializer.getFieldValue(f, self);
                value = setValue(self, fieldName, value);
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

    public void updateLoadedFieldMap(final Object proxiedObject, final boolean iReload) {
        final Set<String> fields = new HashSet<String>(loadedFields.keySet());
        for (String fieldName : fields) {
            try {
                if (iReload) {
                    // FORCE POJO FIELD VALUE TO DEFAULT VALUE, WHICH CAN BE null, 0 or false
                    final Field fieldToReset = OObjectEntitySerializer.getField(fieldName, proxiedObject.getClass());
                    OObjectEntitySerializer.setFieldValue(fieldToReset, proxiedObject,
                            getDefaultValueForField(fieldToReset));
                } else {
                    final Object value = getValue(proxiedObject, fieldName, false, null);

                    if (value instanceof OObjectLazyMultivalueElement) {
                        if (((OObjectLazyMultivalueElement<?>) value).getUnderlying() != doc.field(fieldName))
                            loadedFields.remove(fieldName);
                    } else {
                        loadedFields.put(fieldName, doc.getVersion());
                    }
                }
            } catch (IllegalArgumentException e) {
                throw OException.wrapException(new OSerializationException(
                        "Error updating object after save of class " + proxiedObject.getClass()), e);
            } catch (IllegalAccessException e) {
                throw OException.wrapException(new OSerializationException(
                        "Error updating object after save of class " + proxiedObject.getClass()), e);
            } catch (NoSuchMethodException e) {
                throw OException.wrapException(new OSerializationException(
                        "Error updating object after save of class " + proxiedObject.getClass()), e);
            } catch (InvocationTargetException e) {
                throw OException.wrapException(new OSerializationException(
                        "Error updating object after save of class " + proxiedObject.getClass()), e);
            }
        }

        if (iReload) {
            // RESET LOADED FIELDS, SO THE MUST BE RELOADED FROM DATABASE
            loadedFields.clear();
        }
    }

    protected Object manageGetMethod(final Object self, final Method m, final Method proceed, final Object[] args)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException,
            IllegalArgumentException, NoSuchFieldException {
        final String fieldName = OObjectEntityEnhancer.getInstance().getMethodFilter(self.getClass()).getFieldName(m);

        final ORID docRID = doc.getIdentity();

        final boolean idOrVersionField;
        if (OObjectEntitySerializer.isIdField(m.getDeclaringClass(), fieldName)) {
            idOrVersionField = true;
            OObjectEntitySerializer.setIdField(m.getDeclaringClass(), self, docRID);
        } else if (OObjectEntitySerializer.isVersionField(m.getDeclaringClass(), fieldName)) {
            idOrVersionField = true;
            if (docRID.isValid() && !docRID.isTemporary())
                OObjectEntitySerializer.setVersionField(m.getDeclaringClass(), self, doc.getVersion());
        } else
            idOrVersionField = false;

        Object value = proceed.invoke(self, args);

        value = getValue(self, fieldName, idOrVersionField, value);
        if (docRID.isValid() && !docRID.isTemporary())
            loadedFields.put(fieldName, doc.getVersion());
        else
            loadedFields.put(fieldName, 0);

        return value;
    }

    protected Object getValue(final Object self, final String fieldName, final boolean idOrVersionField, Object value)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        return getValue(self, fieldName, idOrVersionField, value, false);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Object getValue(final Object self, final String fieldName, final boolean idOrVersionField, Object value,
            final boolean iIgnoreLoadedFields)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (!idOrVersionField) {
            if (value == null) {
                if (!iIgnoreLoadedFields && loadedFields.containsKey(fieldName)
                        && loadedFields.get(fieldName).compareTo(doc.getVersion()) == 0) {
                    return null;
                } else {
                    final Object docValue = getDocFieldValue(self, fieldName);
                    if (docValue != null) {
                        value = lazyLoadField(self, fieldName, docValue, value);
                    }
                }
            } else {
                if (((value instanceof Collection<?> || value instanceof Map<?, ?>)
                        && !(value instanceof OObjectLazyMultivalueElement)) || value.getClass().isArray()) {
                    final Class<?> genericMultiValueType = OReflectionHelper
                            .getGenericMultivalueType(OObjectEntitySerializer.getField(fieldName, self.getClass()));
                    if (genericMultiValueType == null || !OReflectionHelper.isJavaType(genericMultiValueType)) {
                        final Field f = OObjectEntitySerializer.getField(fieldName, self.getClass());
                        if (OObjectEntitySerializer.isSerializedType(f)
                                && !(value instanceof OObjectLazyCustomSerializer)) {
                            value = manageSerializedCollections(self, fieldName, value);
                        } else if (genericMultiValueType != null && genericMultiValueType.isEnum()
                                && !(value instanceof OObjectLazyEnumSerializer)) {
                            value = manageEnumCollections(self, f.getName(), genericMultiValueType, value);
                        } else {
                            value = manageObjectCollections(self, fieldName, value);
                        }
                    } else {
                        final Object docValue = getDocFieldValue(self, fieldName);
                        if (docValue == null) {
                            if (value.getClass().isArray()) {
                                OClass schemaClass = doc.getSchemaClass();
                                OProperty schemaProperty = null;
                                if (schemaClass != null)
                                    schemaProperty = schemaClass.getProperty(fieldName);

                                doc.field(fieldName, OObjectEntitySerializer.typeToStream(value,
                                        schemaProperty != null ? schemaProperty.getType() : null, getDatabase(), doc));
                            } else
                                doc.field(fieldName, value);

                        } else if (!loadedFields.containsKey(fieldName)) {
                            value = manageArrayFieldObject(OObjectEntitySerializer.getField(fieldName, self.getClass()),
                                    self, docValue);
                            Method setMethod = getSetMethod(self.getClass().getSuperclass(),
                                    getSetterFieldName(fieldName), value);
                            setMethod.invoke(self, value);
                        } else if ((value instanceof Set || value instanceof Map)
                                && loadedFields.get(fieldName).compareTo(doc.getVersion()) < 0
                                && !OReflectionHelper.isJavaType(genericMultiValueType)) {
                            if (value instanceof Set)
                                value = new OObjectLazySet(self, (Set<OIdentifiable>) docValue,
                                        OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), fieldName));
                            else
                                value = new OObjectLazyMap(self, (Map<Object, OIdentifiable>) docValue,
                                        OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), fieldName));
                            final Method setMethod = getSetMethod(self.getClass().getSuperclass(),
                                    getSetterFieldName(fieldName), value);
                            setMethod.invoke(self, value);
                        }
                    }
                } else if (!loadedFields.containsKey(fieldName)
                        || loadedFields.get(fieldName).compareTo(doc.getVersion()) < 0) {
                    final Object docValue = getDocFieldValue(self, fieldName);
                    if (docValue != null && !docValue.equals(value)) {
                        value = lazyLoadField(self, fieldName, docValue, value);
                    }
                }
            }
        }
        return value;
    }

    protected Object getDocFieldValue(final Object self, final String fieldName) {
        final OClass cls = doc.getSchemaClass();
        if (cls != null && cls.existsProperty(fieldName))
            return doc.field(fieldName);
        else {
            OType expected = OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName);
            return ODocumentFieldHandler.getStrategy(this.getDatabase()).load(doc, fieldName, expected);
        }
    }

    protected Object setDocFieldValue(final String fieldName, final Object value, final OType type) {
        return ODocumentFieldHandler.getStrategy(this.getDatabase()).store(doc, fieldName, value, type);
    }

    protected Object manageObjectCollections(final Object self, final String fieldName, Object value)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        boolean customSerialization = false;
        final Field f = OObjectEntitySerializer.getField(fieldName, self.getClass());
        if (OObjectEntitySerializer.isSerializedType(f)) {
            customSerialization = true;
        }
        if (value instanceof Collection<?>) {
            value = manageCollectionSave(self, f, (Collection<?>) value, customSerialization, false);
        } else if (value instanceof Map<?, ?>) {
            value = manageMapSave(self, f, (Map<?, ?>) value, customSerialization);
        } else if (value.getClass().isArray()) {
            value = manageArraySave(fieldName, (Object[]) value);
        }
        OObjectEntitySerializer.setFieldValue(OObjectEntitySerializer.getField(fieldName, self.getClass()), self,
                value);
        return value;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Object manageSerializedCollections(final Object self, final String fieldName, Object value)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (value instanceof Collection<?>) {
            if (value instanceof List) {
                List<Object> docList = doc.field(fieldName, OType.EMBEDDEDLIST);
                if (docList == null) {
                    docList = new ArrayList<Object>();
                    setDocFieldValue(fieldName, docList, OType.EMBEDDEDLIST);
                }
                value = new OObjectCustomSerializerList(
                        OObjectEntitySerializer
                                .getSerializedType(OObjectEntitySerializer.getField(fieldName, self.getClass())),
                        doc, docList, (List<?>) value);
            } else if (value instanceof Set) {
                Set<Object> docSet = doc.field(fieldName, OType.EMBEDDEDSET);
                if (docSet == null) {
                    docSet = new HashSet<Object>();
                    setDocFieldValue(fieldName, docSet, OType.EMBEDDEDSET);
                }
                value = new OObjectCustomSerializerSet(
                        OObjectEntitySerializer
                                .getSerializedType(OObjectEntitySerializer.getField(fieldName, self.getClass())),
                        doc, docSet, (Set<?>) value);
            }
        } else if (value instanceof Map<?, ?>) {
            Map<Object, Object> docMap = doc.field(fieldName, OType.EMBEDDEDMAP);
            if (docMap == null) {
                docMap = new HashMap<Object, Object>();
                setDocFieldValue(fieldName, docMap, OType.EMBEDDEDMAP);
            }
            value = new OObjectCustomSerializerMap(
                    OObjectEntitySerializer
                            .getSerializedType(OObjectEntitySerializer.getField(fieldName, self.getClass())),
                    doc, docMap, (Map<Object, Object>) value);
        } else if (value.getClass().isArray()) {
            value = manageArraySave(fieldName, (Object[]) value);
        }
        OObjectEntitySerializer.setFieldValue(OObjectEntitySerializer.getField(fieldName, self.getClass()), self,
                value);
        return value;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Object manageEnumCollections(final Object self, final String fieldName, final Class<?> enumClass,
            Object value) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (value instanceof Collection<?>) {
            if (value instanceof List) {
                List<Object> docList = doc.field(fieldName, OType.EMBEDDEDLIST);
                if (docList == null) {
                    docList = new ArrayList<Object>();
                    setDocFieldValue(fieldName, docList, OType.EMBEDDEDLIST);
                }
                value = new OObjectEnumLazyList(enumClass, doc, docList, (List<?>) value);
            } else if (value instanceof Set) {
                Set<Object> docSet = doc.field(fieldName, OType.EMBEDDEDSET);
                if (docSet == null) {
                    docSet = new HashSet<Object>();
                    setDocFieldValue(fieldName, docSet, OType.EMBEDDEDSET);
                }
                value = new OObjectEnumLazySet(enumClass, doc, docSet, (Set<?>) value);
            }
        } else if (value instanceof Map<?, ?>) {
            Map<Object, Object> docMap = doc.field(fieldName, OType.EMBEDDEDMAP);
            if (docMap == null) {
                docMap = new HashMap<Object, Object>();
                setDocFieldValue(fieldName, docMap, OType.EMBEDDEDMAP);
            }
            value = new OObjectEnumLazyMap(enumClass, doc, docMap, (Map<?, ?>) value);
        } else if (value.getClass().isArray()) {
            value = manageArraySave(fieldName, (Object[]) value);
        }
        OObjectEntitySerializer.setFieldValue(OObjectEntitySerializer.getField(fieldName, self.getClass()), self,
                value);
        return value;
    }

    protected Object manageArraySave(final String iFieldName, final Object[] value) {
        if (value.length > 0) {
            final Object o = ((Object[]) value)[0];
            if (o instanceof Proxy || o.getClass().isEnum()) {
                Object[] newValue = new Object[value.length];
                convertArray(value, newValue, o.getClass().isEnum());
                doc.field(iFieldName, newValue);
            }
        }
        return value;
    }

    @SuppressWarnings("rawtypes")
    protected void convertArray(final Object[] value, Object[] newValue, boolean isEnum) {
        for (int i = 0; i < value.length; i++) {
            if (isEnum) {
                newValue[i] = value[i] != null ? ((Enum) value[i]).name() : null;
            } else {
                newValue[i] = value[i] != null ? OObjectEntitySerializer.getDocument((Proxy) value[i]) : null;
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Object manageMapSave(final Object self, final Field f, Map<?, ?> value,
            final boolean customSerialization) {
        final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
        if (customSerialization) {
            Map<Object, Object> map = new HashMap<Object, Object>();
            setDocFieldValue(f.getName(), map, OType.EMBEDDEDMAP);
            value = new OObjectCustomSerializerMap<TYPE>(OObjectEntitySerializer.getSerializedType(f), doc, map,
                    (Map<Object, Object>) value);
        } else if (genericType != null && genericType.isEnum()) {
            Map<Object, Object> map = new HashMap<Object, Object>();
            setDocFieldValue(f.getName(), map, OType.EMBEDDEDMAP);
            value = new OObjectEnumLazyMap(genericType, doc, map, (Map<Object, Object>) value);
        } else if (!(value instanceof OObjectLazyMultivalueElement)) {
            OType type = OObjectEntitySerializer.isEmbeddedField(self.getClass(), f.getName()) ? OType.EMBEDDEDMAP
                    : OType.LINKMAP;
            if (doc.fieldType(f.getName()) != type)
                doc.field(f.getName(), doc.field(f.getName()), type);
            Map<Object, OIdentifiable> docMap = doc.field(f.getName(), type);
            if (docMap == null) {
                if (OType.EMBEDDEDMAP == type)
                    docMap = new OTrackedMap<OIdentifiable>(doc);
                else
                    docMap = new ORecordLazyMap(doc);
                setDocFieldValue(f.getName(), docMap, type);
            }
            value = new OObjectLazyMap(self, docMap, value,
                    OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), f.getName()));
        }
        return value;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Object manageCollectionSave(final Object self, final Field f, Collection<?> value,
            final boolean customSerialization, final boolean isFieldUpdate) {
        final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
        if (customSerialization) {
            if (value instanceof List<?>) {
                final List<Object> list = new ArrayList<Object>();
                setDocFieldValue(f.getName(), list, OType.EMBEDDEDLIST);
                value = new OObjectCustomSerializerList(OObjectEntitySerializer.getSerializedType(f), doc,
                        new ArrayList<Object>(), (List<Object>) value);
            } else {
                final Set<Object> set = new HashSet<Object>();
                setDocFieldValue(f.getName(), set, OType.EMBEDDEDSET);
                value = new OObjectCustomSerializerSet(OObjectEntitySerializer.getSerializedType(f), doc, set,
                        (Set<Object>) value);
            }
        } else if (genericType != null && genericType.isEnum()) {
            if (value instanceof List<?>) {
                final List<Object> list = new ArrayList<Object>();
                setDocFieldValue(f.getName(), list, OType.EMBEDDEDLIST);
                value = new OObjectEnumLazyList(genericType, doc, list, (List<Object>) value);
            } else {
                final Set<Object> set = new HashSet<Object>();
                setDocFieldValue(f.getName(), set, OType.EMBEDDEDSET);
                value = new OObjectEnumLazySet(genericType, doc, set, (Set<Object>) value);
            }
        } else if (!(value instanceof OObjectLazyMultivalueElement)) {
            boolean embedded = OObjectEntitySerializer.isEmbeddedField(self.getClass(), f.getName());
            if (value instanceof List) {
                OType type = embedded ? OType.EMBEDDEDLIST : OType.LINKLIST;
                List<OIdentifiable> docList = doc.field(f.getName(), type);
                if (docList == null) {
                    if (embedded)
                        docList = new OTrackedList<OIdentifiable>(doc);
                    else
                        docList = new ORecordLazyList(doc);
                    setDocFieldValue(f.getName(), docList, type);
                } else if (isFieldUpdate) {
                    docList.clear();
                }
                value = new OObjectLazyList(self, docList, value,
                        OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), f.getName()));
            } else if (value instanceof Set) {
                OType type = embedded ? OType.EMBEDDEDSET : OType.LINKSET;
                Set<OIdentifiable> docSet = doc.field(f.getName(), type);
                if (docSet == null) {
                    if (embedded)
                        docSet = new OTrackedSet<OIdentifiable>(doc);
                    else
                        docSet = new ORecordLazySet(doc);
                    setDocFieldValue(f.getName(), docSet, type);
                } else if (isFieldUpdate) {
                    docSet.clear();
                }
                value = new OObjectLazySet(self, docSet, (Set<?>) value,
                        OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), f.getName()));
            }
        }
        if (!((ODatabaseObject) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner()).isLazyLoading())
            ((OObjectLazyMultivalueElement) value).detach(false);
        return value;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Object lazyLoadField(final Object self, final String fieldName, Object docValue,
            final Object currentValue) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        boolean customSerialization = false;
        final Field f = OObjectEntitySerializer.getField(fieldName, self.getClass());
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
            docValue = manageArrayLoad(docValue, f);
        } else if (customSerialization) {
            docValue = OObjectEntitySerializer.deserializeFieldValue(
                    OObjectEntitySerializer.getField(fieldName, self.getClass()).getType(), docValue);
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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Object manageArrayLoad(Object value, Field f) {
        if (((Object[]) value).length > 0) {
            Object o = ((Object[]) value)[0];
            if (o instanceof OIdentifiable) {
                Object[] newValue = new Object[((Object[]) value).length];
                for (int i = 0; i < ((Object[]) value).length; i++) {
                    ODocument doc = ((OIdentifiable) ((Object[]) value)[i]).getRecord();
                    newValue[i] = OObjectEntitySerializer.getDocument((Proxy) doc);
                }
                value = newValue;
            } else {
                final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
                if (genericType != null && genericType.isEnum()) {
                    Object newValue = Array.newInstance(genericType, ((Object[]) value).length);
                    for (int i = 0; i < ((Object[]) value).length; i++) {
                        o = ((Object[]) value)[i];
                        if (o instanceof Number)
                            o = genericType.getEnumConstants()[((Number) o).intValue()];
                        else
                            o = Enum.valueOf(genericType, o.toString());
                        ((Enum[]) newValue)[i] = (Enum) o;
                    }
                    value = newValue;
                }
            }
        }
        return value;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Object manageMapLoad(final Field f, final Object self, Object value, final boolean customSerialization) {
        final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
        if (value instanceof ORecordLazyMap || (value instanceof OTrackedMap<?>
                && (genericType == null || !OReflectionHelper.isJavaType(genericType)) && !customSerialization
                && (genericType == null || !genericType.isEnum()))) {
            value = new OObjectLazyMap(self, (OTrackedMap<?>) value,
                    OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), f.getName()));
        } else if (customSerialization) {
            value = new OObjectCustomSerializerMap<TYPE>(OObjectEntitySerializer.getSerializedType(f), doc,
                    (Map<Object, Object>) value);
        } else if (genericType != null && genericType.isEnum()) {
            value = new OObjectEnumLazyMap(genericType, doc, (Map<Object, Object>) value);
        }
        return value;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Object manageCollectionLoad(final Field f, final Object self, Object value,
            final boolean customSerialization) {
        final Class genericType = OReflectionHelper.getGenericMultivalueType(f);
        if (value instanceof ORecordLazyList || (value instanceof OTrackedList<?>
                && (genericType == null || !OReflectionHelper.isJavaType(genericType)) && !customSerialization
                && (genericType == null || !genericType.isEnum()))) {
            value = new OObjectLazyList(self, (List<OIdentifiable>) value,
                    OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), f.getName()));
        } else if (value instanceof ORecordLazySet || (value instanceof OTrackedSet<?>
                && (genericType == null || !OReflectionHelper.isJavaType(genericType)) && !customSerialization
                && (genericType == null || !genericType.isEnum()))) {
            value = new OObjectLazySet(self, (Set) value,
                    OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), f.getName()));
        } else if (customSerialization) {
            if (value instanceof List<?>) {
                value = new OObjectCustomSerializerList(OObjectEntitySerializer.getSerializedType(f), doc,
                        (List<Object>) value);
            } else {
                value = new OObjectCustomSerializerSet(OObjectEntitySerializer.getSerializedType(f), doc,
                        (Set<Object>) value);
            }
        } else if (genericType != null && genericType.isEnum()) {
            if (value instanceof List<?>) {
                value = new OObjectEnumLazyList(genericType, doc, (List<Object>) value);
            } else {
                value = new OObjectEnumLazySet(genericType, doc, (Set<Object>) value);
            }
        }

        return manageArrayFieldObject(f, self, value);
    }

    protected Object manageArrayFieldObject(final Field field, final Object self, Object value) {
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

    protected Object convertDocumentToObject(final ODocument value, final Object self) {
        if (value == null)
            return null;
        return OObjectEntityEnhancer.getInstance().getProxiedInstance(value.getClassName(),
                getDatabase().getEntityManager(), value, (self instanceof ProxyObject ? (ProxyObject) self : null));
    }

    protected Object manageSetMethod(final Object self, final Method m, final Method proceed, final Object[] args)
            throws IllegalAccessException, InvocationTargetException {
        final String fieldName;
        fieldName = OObjectEntityEnhancer.getInstance().getMethodFilter(self.getClass()).getFieldName(m);
        args[0] = setValue(self, fieldName, args[0]);
        return proceed.invoke(self, args);
    }

    @SuppressWarnings("rawtypes")
    protected Object setValue(final Object self, final String fieldName, Object valueToSet) {
        if (valueToSet == null) {
            Object oldValue = doc.field(fieldName);
            if (OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), fieldName)
                    && oldValue instanceof OIdentifiable)
                orphans.add(((OIdentifiable) oldValue).getIdentity());
            setDocFieldValue(fieldName, valueToSet, OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
        } else if (!valueToSet.getClass().isAnonymousClass()) {
            if (valueToSet instanceof Proxy) {
                ODocument docToSet = OObjectEntitySerializer.getDocument((Proxy) valueToSet);
                if (OObjectEntitySerializer.isEmbeddedField(self.getClass(), fieldName))
                    ODocumentInternal.addOwner(docToSet, doc);
                Object oldValue = doc.field(fieldName);
                if (OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), fieldName)
                        && oldValue instanceof OIdentifiable)
                    orphans.add(((OIdentifiable) oldValue).getIdentity());
                setDocFieldValue(fieldName, docToSet,
                        OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
            } else if (valueToSet instanceof OIdentifiable) {
                if (valueToSet instanceof ODocument
                        && OObjectEntitySerializer.isEmbeddedField(self.getClass(), fieldName))
                    ODocumentInternal.addOwner((ODocument) valueToSet, doc);
                Object oldValue = doc.field(fieldName);
                if (OObjectEntitySerializer.isCascadeDeleteField(self.getClass(), fieldName)
                        && oldValue instanceof OIdentifiable)
                    orphans.add(((OIdentifiable) oldValue).getIdentity());
                setDocFieldValue(fieldName, valueToSet,
                        OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
            } else if (((valueToSet instanceof Collection<?> || valueToSet instanceof Map<?, ?>))
                    || valueToSet.getClass().isArray()) {
                Class<?> genericMultiValueType = OReflectionHelper
                        .getGenericMultivalueType(OObjectEntitySerializer.getField(fieldName, self.getClass()));
                if (genericMultiValueType != null && !OReflectionHelper.isJavaType(genericMultiValueType)) {
                    if (!(valueToSet instanceof OObjectLazyMultivalueElement)) {
                        if (valueToSet instanceof Collection<?>) {
                            boolean customSerialization = false;
                            final Field f = OObjectEntitySerializer.getField(fieldName, self.getClass());
                            if (OObjectEntitySerializer.isSerializedType(f)) {
                                customSerialization = true;
                            }
                            valueToSet = manageCollectionSave(self, f, (Collection<?>) valueToSet, customSerialization,
                                    true);
                        } else if (valueToSet instanceof Map<?, ?>) {
                            boolean customSerialization = false;
                            final Field f = OObjectEntitySerializer.getField(fieldName, self.getClass());
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
                        setDocFieldValue(fieldName, OObjectEntitySerializer.serializeFieldValue(
                                OObjectEntitySerializer.getField(fieldName, self.getClass()).getType(), valueToSet),
                                OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
                    } else {
                        if (valueToSet.getClass().isArray()) {
                            final OClass schemaClass = doc.getSchemaClass();
                            OProperty schemaProperty = null;
                            if (schemaClass != null)
                                schemaProperty = schemaClass.getProperty(fieldName);

                            setDocFieldValue(fieldName,
                                    OObjectEntitySerializer.typeToStream(valueToSet,
                                            schemaProperty != null ? schemaProperty.getType() : null, getDatabase(),
                                            doc),
                                    OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
                        } else
                            setDocFieldValue(fieldName, valueToSet,
                                    OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
                    }
                }
            } else if (valueToSet.getClass().isEnum()) {
                setDocFieldValue(fieldName, ((Enum) valueToSet).name(),
                        OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
            } else {
                if (OObjectEntitySerializer.isToSerialize(valueToSet.getClass())) {
                    setDocFieldValue(fieldName,
                            OObjectEntitySerializer.serializeFieldValue(
                                    OObjectEntitySerializer.getField(fieldName, self.getClass()).getType(), valueToSet),
                            OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
                } else if (getDatabase().getEntityManager()
                        .getEntityClass(valueToSet.getClass().getSimpleName()) != null
                        && !valueToSet.getClass().isEnum()) {
                    valueToSet = OObjectEntitySerializer.serializeObject(valueToSet, getDatabase());
                    final ODocument docToSet = OObjectEntitySerializer.getDocument((Proxy) valueToSet);
                    if (OObjectEntitySerializer.isEmbeddedField(self.getClass(), fieldName))
                        ODocumentInternal.addOwner((ODocument) docToSet, doc);

                    setDocFieldValue(fieldName, docToSet,
                            OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
                } else {
                    setDocFieldValue(fieldName, valueToSet,
                            OObjectEntitySerializer.getTypeByClass(self.getClass(), fieldName));
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

    protected String getSetterFieldName(final String fieldName) {
        final StringBuffer methodName = new StringBuffer("set");
        methodName.append(Character.toUpperCase(fieldName.charAt(0)));
        for (int i = 1; i < fieldName.length(); i++) {
            methodName.append(fieldName.charAt(i));
        }
        return methodName.toString();
    }

    protected Method getSetMethod(final Class<?> iClass, final String fieldName, Object value)
            throws NoSuchMethodException {
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

    private Object getDefaultValueForField(Field field) {
        if (field.getType() == Byte.TYPE)
            return Byte.valueOf("0");

        if (field.getType() == Short.TYPE)
            return Short.valueOf("0");

        if (field.getType() == Integer.TYPE)
            return 0;

        if (field.getType() == Long.TYPE)
            return 0L;

        if (field.getType() == Float.TYPE)
            return 0.0f;

        if (field.getType() == Double.TYPE)
            return 0.0d;

        if (field.getType() == Boolean.TYPE)
            return false;

        return null;
    }
}
