/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.common.util;

/**
 * Keeps in memory the information about a hole in data segment.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 * @param <K>
 *          Hole size
 * @param <V>
 *          Value
 * @param <SV>
 *          Sub value
 * @see OPair
 */
public class OTriple<K extends Comparable<K>, V, SV> implements Comparable<OTriple<K, V, SV>> {
	public K	key;
	public V	value;
	public SV	subValue;

	public OTriple() {
	}

	public OTriple(final K iKey, final V iValue, final SV iSubValue) {
		init(iKey, iValue, iSubValue);
	}

	public void init(final K iKey, final V iValue, final SV iSubValue) {
		key = iKey;
		value = iValue;
		subValue = iSubValue;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	public SV getSubValue() {
		return subValue;
	}

	public V setValue(final V iValue) {
		V oldValue = value;
		value = iValue;
		return oldValue;
	}

	public SV setSubValue(final SV iSubValue) {
		SV oldSubValue = subValue;
		subValue = iSubValue;
		return oldSubValue;
	}

	@Override
	public String toString() {
		return key + ":" + value + "/" + value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OTriple<?, ?, ?> other = (OTriple<?, ?, ?>) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	public int compareTo(final OTriple<K, V, SV> o) {
		return key.compareTo(o.key);
	}
}
