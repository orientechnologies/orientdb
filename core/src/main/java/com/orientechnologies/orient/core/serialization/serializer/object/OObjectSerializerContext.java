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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

import com.orientechnologies.common.reflection.OReflectionHelper;

// We need to suppress the raw types warnings of OObjectSerializer, because otherwise the compiler won't compile
// the parameter iFieldValue (which is of type Object) to the defined type LOCAL_TYPE.
@SuppressWarnings({ "rawtypes", "unchecked" })
public class OObjectSerializerContext implements OObjectSerializer<Object, Object> {
	final private Map<Class<?>, OObjectSerializer>	customSerializers	= new LinkedHashMap<Class<?>, OObjectSerializer>();

	public void bind(final OObjectSerializer serializer) {
		final Type[] actualTypes = OReflectionHelper.getGenericTypes(serializer.getClass());
		if (actualTypes[0] instanceof Class<?>)
			customSerializers.put((Class<?>) actualTypes[0], serializer);
		else if (actualTypes[0] instanceof ParameterizedType)
			customSerializers.put((Class<?>) ((ParameterizedType) actualTypes[0]).getRawType(), serializer);
	}

	public void unbind(final OObjectSerializer serializer) {
		final Type genericType = serializer.getClass().getGenericInterfaces()[0];
		if (genericType != null && genericType instanceof ParameterizedType) {
			final ParameterizedType pt = (ParameterizedType) genericType;
			if (pt.getActualTypeArguments() != null && pt.getActualTypeArguments().length > 1) {
				final Type[] actualTypes = pt.getActualTypeArguments();
				if (actualTypes[0] instanceof Class<?>) {
					customSerializers.remove((Class<?>) actualTypes[0]);
				} else if (actualTypes[0] instanceof ParameterizedType) {
					customSerializers.remove((Class<?>) ((ParameterizedType) actualTypes[0]).getRawType());
				}
			}
		}
	}

	public Object serializeFieldValue(final Class<?> iClass, Object iFieldValue) {
		for (Class<?> type : customSerializers.keySet()) {
			if (type.isInstance(iFieldValue) || (iFieldValue == null && type == Void.class)) {
				iFieldValue = customSerializers.get(type).serializeFieldValue(iClass, iFieldValue);
				break;
			}
		}

		return iFieldValue;
	}

	public Object unserializeFieldValue(final Class<?> iClass, final Object iFieldValue) {
		if (iClass != null)
			for (Class<?> type : customSerializers.keySet()) {
				if (type.isAssignableFrom(iClass) || type == Void.class) {
					return customSerializers.get(type).unserializeFieldValue(iClass, iFieldValue);
				}
			}

		return iFieldValue;
	}
}
