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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author luca.molino
 * 
 */
public class OObjectEntityEnhancer {

	private static final OObjectEntityEnhancer	instance							= new OObjectEntityEnhancer();

	public static final String									ENHANCER_CLASS_PREFIX	= "orientdb_";

	public OObjectEntityEnhancer() {
	}

	@SuppressWarnings("unchecked")
	public <T> T getProxiedInstance(final String iClass, final OEntityManager entityManager, final ODocument doc, Object... iArgs) {
		final Class<T> clazz = (Class<T>) entityManager.getEntityClass(iClass);
		return getProxiedInstance(clazz, doc, iArgs);
	}

	@SuppressWarnings("unchecked")
	public <T> T getProxiedInstance(final String iClass, final Object iEnclosingInstance, final OEntityManager entityManager,
			final ODocument doc, Object... iArgs) {
		final Class<T> clazz = (Class<T>) entityManager.getEntityClass(iClass);
		return getProxiedInstance(clazz, iEnclosingInstance, doc, iArgs);
	}

	public <T> T getProxiedInstance(final Class<T> iClass, final ODocument doc, Object... iArgs) {
		return getProxiedInstance(iClass, null, doc, iArgs);
	}

	@SuppressWarnings("unchecked")
	public <T> T getProxiedInstance(final Class<T> iClass, Object iEnclosingInstance, final ODocument doc, Object... iArgs) {
		if (iClass == null) {
			throw new OSerializationException("Type " + doc.getClassName()
					+ " cannot be serialized because is not part of registered entities. To fix this error register this class");
		}
		final Class<T> c;
		boolean isInnerClass = iClass.getEnclosingClass() != null;
		if (Proxy.class.isAssignableFrom(iClass)) {
			c = iClass;
		} else {
			ProxyFactory f = new ProxyFactory();
			f.setSuperclass(iClass);
			f.setFilter(new MethodFilter() {
				public boolean isHandled(Method m) {
					final String methodName = m.getName();
					try {
						return (isSetterMethod(methodName, m) || isGetterMethod(methodName, m) || methodName.equals("equals") || methodName
								.equals("hashCode"));
					} catch (NoSuchFieldException nsfe) {
						OLogManager.instance().warn(this, "Error handling the method %s in class %s", nsfe, m.getName(), iClass.getName());
						return false;
					} catch (SecurityException se) {
						OLogManager.instance().warn(this, "", se, m.getName(), iClass.getName());
						return false;
					}
				}
			});
			c = f.createClass();
		}
		MethodHandler mi = new OObjectProxyMethodHandler(doc);
		try {
			T newEntity;
			if (iArgs != null && iArgs.length > 0) {
				if (isInnerClass) {
					if (iEnclosingInstance == null) {
						iEnclosingInstance = iClass.getEnclosingClass().newInstance();
					}
					Object[] newArgs = new Object[iArgs.length + 1];
					newArgs[0] = iEnclosingInstance;
					for (int i = 0; i < iArgs.length; i++) {
						newArgs[i + 1] = iArgs[i];
					}
					iArgs = newArgs;
				}
				Constructor<T> constructor = null;
				for (Constructor<?> constr : c.getConstructors()) {
					boolean found = true;
					if (constr.getParameterTypes().length == iArgs.length) {
						for (int i = 0; i < constr.getParameterTypes().length; i++) {
							Class<?> parameterType = constr.getParameterTypes()[i];
							if (parameterType.isPrimitive()) {
								if (!isPrimitiveParameterCorrect(parameterType, iArgs[i])) {
									found = false;
									break;
								}
							} else if (iArgs[i] != null && !parameterType.isAssignableFrom(iArgs[i].getClass())) {
								found = false;
								break;
							}
						}
					} else {
						continue;
					}
					if (found) {
						constructor = (Constructor<T>) constr;
						break;
					}
				}
				if (constructor != null) {
					newEntity = (T) constructor.newInstance(iArgs);
					initDocument(iClass, newEntity, doc, (ODatabaseObject) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner());
				} else {
					if (iEnclosingInstance != null)
						newEntity = createInstanceNoParameters(c, iEnclosingInstance);
					else
						newEntity = createInstanceNoParameters(c, iClass);
				}
			} else {
				if (iEnclosingInstance != null)
					newEntity = createInstanceNoParameters(c, iEnclosingInstance);
				else
					newEntity = createInstanceNoParameters(c, iClass);
			}
			((Proxy) newEntity).setHandler(mi);
			if (OObjectEntitySerializer.hasBoundedDocumentField(iClass))
				OObjectEntitySerializer.setFieldValue(OObjectEntitySerializer.getBoundedDocumentField(iClass), newEntity, doc);
			return newEntity;
		} catch (InstantiationException ie) {
			OLogManager.instance().error(this, "Error creating proxied instance for class " + iClass.getName(), ie);
		} catch (IllegalAccessException iae) {
			OLogManager.instance().error(this, "Error creating proxied instance for class " + iClass.getName(), iae);
		} catch (IllegalArgumentException iae) {
			OLogManager.instance().error(this, "Error creating proxied instance for class " + iClass.getName(), iae);
		} catch (SecurityException se) {
			OLogManager.instance().error(this, "Error creating proxied instance for class " + iClass.getName(), se);
		} catch (InvocationTargetException ite) {
			OLogManager.instance().error(this, "Error creating proxied instance for class " + iClass.getName(), ite);
		} catch (NoSuchMethodException nsme) {
			OLogManager.instance().error(this, "Error creating proxied instance for class " + iClass.getName(), nsme);
		}
		return null;
	}

	public static synchronized OObjectEntityEnhancer getInstance() {
		return instance;
	}

	private boolean isSetterMethod(String fieldName, Method m) throws SecurityException, NoSuchFieldException {
		if (!fieldName.startsWith("set") || !checkIfFirstCharAfterPrefixIsUpperCase(fieldName, "set"))
			return false;
		if (m.getParameterTypes() != null && m.getParameterTypes().length != 1)
			return false;
		return !OObjectEntitySerializer.isTransientField(m.getDeclaringClass(), getFieldName(m));
	}

	private boolean isGetterMethod(String fieldName, Method m) throws SecurityException, NoSuchFieldException {
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
		return !OObjectEntitySerializer.isTransientField(m.getDeclaringClass(), getFieldName(m));
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

	private boolean checkIfFirstCharAfterPrefixIsUpperCase(String methodName, String prefix) {
		return methodName.length() > prefix.length() ? Character.isUpperCase(methodName.charAt(prefix.length())) : false;
	}

	private boolean isPrimitiveParameterCorrect(Class<?> primitiveClass, Object parameterValue) {
		if (parameterValue == null)
			return false;
		final Class<?> parameterClass = parameterValue.getClass();
		if (Integer.TYPE.isAssignableFrom(primitiveClass))
			return Integer.class.isAssignableFrom(parameterClass);
		else if (Double.TYPE.isAssignableFrom(primitiveClass))
			return Double.class.isAssignableFrom(parameterClass);
		else if (Float.TYPE.isAssignableFrom(primitiveClass))
			return Float.class.isAssignableFrom(parameterClass);
		else if (Long.TYPE.isAssignableFrom(primitiveClass))
			return Long.class.isAssignableFrom(parameterClass);
		else if (Short.TYPE.isAssignableFrom(primitiveClass))
			return Short.class.isAssignableFrom(parameterClass);
		else if (Byte.TYPE.isAssignableFrom(primitiveClass))
			return Byte.class.isAssignableFrom(parameterClass);
		return false;
	}

	protected void initDocument(Class<?> iClass, Object iInstance, ODocument iDocument, ODatabaseObject db)
			throws IllegalArgumentException, IllegalAccessException {
		for (Class<?> currentClass = iClass; currentClass != Object.class;) {
			for (Field f : currentClass.getDeclaredFields()) {
				if (f.getName().equals("this$0"))
					continue;
				boolean accessModified = false;
				if (!f.isAccessible()) {
					f.setAccessible(true);
					accessModified = true;
				}
				Object o = f.get(iInstance);
				if (o != null) {
					if (OObjectEntitySerializer.isToSerialize(o.getClass()))
						o = OObjectEntitySerializer.serializeFieldValue(o.getClass(), o);
					iDocument.field(f.getName(), OObjectEntitySerializer.typeToStream(o, OType.getTypeByClass(f.getType()), db, iDocument));
				}
				if (accessModified)
					f.setAccessible(false);
			}
			currentClass = currentClass.getSuperclass();
		}
	}

	protected <T> T createInstanceNoParameters(Class<T> iProxiedClass, Class<?> iOriginalClass) throws SecurityException,
			NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		T instanceToReturn = null;
		final Class<?> enclosingClass = iOriginalClass.getEnclosingClass();

		if (enclosingClass != null) {
			Object instanceOfEnclosingClass = createInstanceNoParameters(enclosingClass, enclosingClass);

			Constructor<T> ctor = iProxiedClass.getConstructor(enclosingClass);

			if (ctor != null) {
				instanceToReturn = ctor.newInstance(instanceOfEnclosingClass);
			}
		} else {
			instanceToReturn = iProxiedClass.newInstance();
		}

		return instanceToReturn;

	}

	protected <T> T createInstanceNoParameters(Class<T> iProxiedClass, Object iEnclosingInstance) throws SecurityException,
			NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		T instanceToReturn = null;
		final Class<?> enclosingClass = iEnclosingInstance.getClass();

		if (enclosingClass != null) {

			Constructor<T> ctor = iProxiedClass.getConstructor(enclosingClass);

			if (ctor != null) {
				instanceToReturn = ctor.newInstance(iEnclosingInstance);
			}
		} else {
			instanceToReturn = iProxiedClass.newInstance();
		}

		return instanceToReturn;

	}
}
