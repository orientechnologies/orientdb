package com.orientechnologies.common.util;

import java.util.Map.Entry;

public class OPair<K, V> implements Entry<K, V> {
	public K	key;
	public V	value;

	public OPair(final K iKey, final V iValue) {
		key = iKey;
		value = iValue;
	}

	public OPair(final Entry<K, V> iSource) {
		key = iSource.getKey();
		value = iSource.getValue();
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	public V setValue(final V iValue) {
		V oldValue = value;
		value = iValue;
		return oldValue;
	}

	@Override
	public String toString() {
		return key + ":" + value;
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
		OPair<?, ?> other = (OPair<?, ?>) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}
}
