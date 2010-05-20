package com.orientechnologies.common.factory;

import java.util.HashMap;
import java.util.Map;

public class ODynamicFactoryInverse<K, V> extends ODynamicFactory<K, V> {
	protected Map<V, K>	inverseRegistry	= new HashMap<V, K>();

	public K getInverse(V iValue) {
		return inverseRegistry.get(iValue);
	}

	@Override
	public void register(K iKey, V iValue) {
		super.register(iKey, iValue);
		inverseRegistry.put(iValue, iKey);
	}

	@Override
	public void unregister(K iKey) {
		V value = get(iKey);
		if (value == null)
			return;
		super.unregister(iKey);
		inverseRegistry.remove(value);
	}
}
