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
}
