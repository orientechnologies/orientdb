package com.orientechnologies.common.factory;

import java.util.HashMap;
import java.util.Map;

public class ODynamicFactory<K, V> {
	protected Map<K, V>	registry	= new HashMap<K, V>();

	public V get(K iKey) {
		return registry.get(iKey);
	}

	public void register(K iKey, V iValue) {
		registry.put(iKey, iValue);
	}

	public void unregister(K iKey) {
		registry.remove(iKey);
	}
}
