package com.orientechnologies.common.concur.resource;

public interface OResourcePoolListener<K, V> {

	public V createNewResource(K iKey, String... iAdditionalArgs);

	public V reuseResource(K iKey, V iValue);
}
