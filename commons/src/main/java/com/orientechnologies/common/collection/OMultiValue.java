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
package com.orientechnologies.common.collection;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import com.orientechnologies.common.log.OLogManager;

/**
 * Handles multi value types such as Collections, Maps and Arrays
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public class OMultiValue {

	public static boolean isMultiValue(final Class<?> iType) {
		return (iType.isArray() || Collection.class.isAssignableFrom(iType) || Map.class.isAssignableFrom(iType));
	}

	public static boolean isMultiValue(final Object iObject) {
		return iObject == null ? false : isMultiValue(iObject.getClass());
	}

	public static int getSize(final Object iObject) {
		if (iObject == null)
			return 0;

		if (!isMultiValue(iObject))
			return 0;

		if (iObject instanceof Collection<?>)
			return ((Collection<Object>) iObject).size();
		if (iObject instanceof Map<?, ?>)
			return ((Map<?, Object>) iObject).size();
		if (iObject.getClass().isArray())
			return Array.getLength(iObject);
		return 0;
	}

	public static Object getFirstValue(final Object iObject) {
		if (iObject == null)
			return null;

		if (!isMultiValue(iObject))
			return null;

		try {
			if (iObject instanceof Collection<?>)
				return ((Collection<Object>) iObject).iterator().next();
			if (iObject instanceof Map<?, ?>)
				return ((Map<?, Object>) iObject).values().iterator().next();
			if (iObject.getClass().isArray())
				return Array.get(iObject, 0);
		} catch (Exception e) {
			// IGNORE IT
			OLogManager.instance().debug(iObject, "Error on reading the first item of the multi value field '%s'", iObject);
		}

		return null;
	}

	public static Iterable<Object> getMultiValueIterable(final Object iObject) {
		if (iObject == null)
			return null;

		if (!isMultiValue(iObject))
			return null;

		if (iObject instanceof Collection<?>)
			return ((Collection<Object>) iObject);
		if (iObject instanceof Map<?, ?>)
			return ((Map<?, Object>) iObject).values();
		if (iObject.getClass().isArray())
			return Arrays.asList(iObject);
		return null;
	}

	public static String toString(final Object iObject) {
		final StringBuilder sb = new StringBuilder();

		if (iObject instanceof Collection<?>) {
			final Collection<Object> coll = (Collection<Object>) iObject;

			sb.append('[');
			for (final Iterator<Object> it = coll.iterator(); it.hasNext();) {
				try {
					Object e = it.next();
					sb.append(e == iObject ? "(this Collection)" : e);
					if (it.hasNext())
						sb.append(", ");
				} catch (NoSuchElementException ex) {
					// IGNORE THIS
				}
			}
			return sb.append(']').toString();
		} else if (iObject instanceof Map<?, ?>) {
			final Map<String, Object> map = (Map<String, Object>) iObject;

			Entry<String, Object> e;

			sb.append('{');
			for (final Iterator<Entry<String, Object>> it = map.entrySet().iterator(); it.hasNext();) {
				try {
					e = it.next();

					sb.append(e.getKey());
					sb.append(":");
					sb.append(e.getValue() == iObject ? "(this Map)" : e.getValue());
					if (it.hasNext())
						sb.append(", ");
				} catch (NoSuchElementException ex) {
					// IGNORE THIS
				}
			}
			return sb.append('}').toString();
		}

		return iObject.toString();
	}
}
