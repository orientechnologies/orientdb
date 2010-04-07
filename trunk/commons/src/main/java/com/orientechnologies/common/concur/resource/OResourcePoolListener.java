package com.orientechnologies.common.concur.resource;

public interface OResourcePoolListener<K, V> {

	public V createNewResource(K iURL);

}
