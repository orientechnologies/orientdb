/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.common.collection;

import java.util.Map;
import java.util.Map.Entry;

public class OSimpleImmutableEntry<K, V> implements Entry<K, V>, java.io.Serializable {
	private static final long	serialVersionUID	= 7138329143949025153L;

	private final K						key;
	private final V						value;

	/**
	 * Creates an entry representing a mapping from the specified key to the specified value.
	 * 
	 * @param key
	 *          the key represented by this entry
	 * @param value
	 *          the value represented by this entry
	 */
	public OSimpleImmutableEntry(final K key, final V value) {
		this.key = key;
		this.value = value;
	}

	/**
	 * Creates an entry representing the same mapping as the specified entry.
	 * 
	 * @param entry
	 *          the entry to copy
	 */
	public OSimpleImmutableEntry(final Entry<? extends K, ? extends V> entry) {
		this.key = entry.getKey();
		this.value = entry.getValue();
	}

	/**
	 * Returns the key corresponding to this entry.
	 * 
	 * @return the key corresponding to this entry
	 */
	public K getKey() {
		return key;
	}

	/**
	 * Returns the value corresponding to this entry.
	 * 
	 * @return the value corresponding to this entry
	 */
	public V getValue() {
		return value;
	}

	/**
	 * Replaces the value corresponding to this entry with the specified value (optional operation). This implementation simply throws
	 * <tt>UnsupportedOperationException</tt>, as this class implements an <i>immutable</i> map entry.
	 * 
	 * @param value
	 *          new value to be stored in this entry
	 * @return (Does not return)
	 * @throws UnsupportedOperationException
	 *           always
	 */
	public V setValue(V value) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Compares the specified object with this entry for equality. Returns {@code true} if the given object is also a map entry and
	 * the two entries represent the same mapping. More formally, two entries {@code e1} and {@code e2} represent the same mapping if
	 * 
	 * <pre>
	 * (e1.getKey() == null ? e2.getKey() == null : e1.getKey().equals(e2.getKey()))
	 * 		&amp;&amp; (e1.getValue() == null ? e2.getValue() == null : e1.getValue().equals(e2.getValue()))
	 * </pre>
	 * 
	 * This ensures that the {@code equals} method works properly across different implementations of the {@code Map.Entry} interface.
	 * 
	 * @param o
	 *          object to be compared for equality with this map entry
	 * @return {@code true} if the specified object is equal to this map entry
	 * @see #hashCode
	 */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Map.Entry)) {
                    return false;
                }
		Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
		return eq(key, e.getKey()) && eq(value, e.getValue());
	}

	private boolean eq(final Object o1, final Object o2) {
		return o1 == null ? o2 == null : o1.equals(o2);
	}

	/**
	 * Returns the hash code value for this map entry. The hash code of a map entry {@code e} is defined to be:
	 * 
	 * <pre>
	 * (e.getKey() == null ? 0 : e.getKey().hashCode()) &circ; (e.getValue() == null ? 0 : e.getValue().hashCode())
	 * </pre>
	 * 
	 * This ensures that {@code e1.equals(e2)} implies that {@code e1.hashCode()==e2.hashCode()} for any two Entries {@code e1} and
	 * {@code e2}, as required by the general contract of {@link Object#hashCode}.
	 * 
	 * @return the hash code value for this map entry
	 * @see #equals
	 */
	@Override
	public int hashCode() {
		return (key == null ? 0 : key.hashCode()) ^ (value == null ? 0 : value.hashCode());
	}

	/**
	 * Returns a String representation of this map entry. This implementation returns the string representation of this entry's key
	 * followed by the equals character ("<tt>=</tt>") followed by the string representation of this entry's value.
	 * 
	 * @return a String representation of this map entry
	 */
	@Override
	public String toString() {
		return key + "=" + value;
	}

}
