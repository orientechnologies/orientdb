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
package com.orientechnologies.orient.core.db.record;

/**
 * Event that contains information about operation that is performed on tracked collection.
 *
 * @param <K> Value that indicates position of item inside collection.
 * @param <V> Item value.
 */
public class OMultiValueChangeEvent<K, V> {
	/**
	 * Operation that is performed on collection.
	 */
	public static enum OChangeType {
		ADD, UPDATE, REMOVE
	}

	/**
	 * Operation that is performed on collection.
	 */
	private final OChangeType changeType;

	/**
	 * Value that indicates position of item inside collection.
	 */
	private final K key;

	/**
	 * New item value.
	 */
	private V value;

	/**
	 * Previous item value.
	 */
	private V oldValue;

	public OMultiValueChangeEvent(final OChangeType changeType, final K key, final V value) {
		this.changeType = changeType;
		this.key = key;
		this.value = value;
	}

	public OMultiValueChangeEvent(final OChangeType changeType, final K key, final V value, final V oldValue) {
		this.changeType = changeType;
		this.key = key;
		this.value = value;
		this.oldValue = oldValue;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	public OChangeType getChangeType() {
		return changeType;
	}

	public V getOldValue() {
		return oldValue;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		final OMultiValueChangeEvent that = (OMultiValueChangeEvent) o;

		if (changeType != that.changeType) {
			return false;
		}
		if (!key.equals(that.key)) {
			return false;
		}
		if (oldValue != null ? !oldValue.equals(that.oldValue) : that.oldValue != null) {
			return false;
		}
		if (value != null ? !value.equals(that.value) : that.value != null) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int result = changeType.hashCode();
		result = 31 * result + key.hashCode();
		result = 31 * result + (value != null ? value.hashCode() : 0);
		result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "OMultiValueChangeEvent{" +
						"changeType=" + changeType +
						", key=" + key +
						", value=" + value +
						", oldValue=" + oldValue +
						'}';
	}
}
