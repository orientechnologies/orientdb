package com.orientechnologies.common.collection;

/**
 * Key that is used in {@link OMVRBTree} for partial composite key search.
 * It always greater than any passed in key.
 * 
 * @since 20.03.12
 */
public final class OAlwaysGreaterKey implements Comparable<Comparable>{
	public int compareTo(Comparable o) {
		return 1;
	}
}