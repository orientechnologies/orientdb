/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.common.comparator;

import java.util.Comparator;

/**
 * Creates comparators for classes that does not implement {@link Comparable}
 * but logically can be compared.
 *
 * @author Andrey Lomakin
 * @since 03.07.12
 */
public class OComparatorFactory {
	public static final OComparatorFactory INSTANCE = new OComparatorFactory();

	/**
	 * Returns {@link Comparator} instance if applicable one exist or <code>null</code> otherwise.
	 *
	 * @param clazz Class of object that is going to be compared.
	 * @param <T>  Class of object that is going to be compared.
	 * @return {@link Comparator} instance if applicable one exist or <code>null</code> otherwise.
	 */
	public <T> Comparator<T> getComparator(Class<T> clazz) {
		if(clazz.equals(byte[].class))
			return (Comparator<T>)OByteArrayComparator.INSTANCE;

		return null;
	}
}