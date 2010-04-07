package com.orientechnologies.common.collection;

public interface OTreeMapEventListener<K, V> {
	public void signalTreeChanged(OTreeMap<K, V> iTree);

	public void signalNodeChanged(OTreeMapEntry<K, V> iNode);
}
