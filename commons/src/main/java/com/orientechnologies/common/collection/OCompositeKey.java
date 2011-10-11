/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.common.collection;

import java.io.Serializable;
import java.util.*;

/**
 * Container for the list of heterogeneous values that are going to be stored in {@link OMVRBTree} as
 * composite keys.
 *
 * Such keys is allowed to use only in {@link OMVRBTree#subMap(Object, boolean, Object, boolean)},
 * {@link OMVRBTree#tailMap(Object, boolean)} and {@link OMVRBTree#headMap(Object, boolean)} methods.
 *
 * @see OMVRBTree.PartialSearchMode
 * @author Andrey lomakin, Artem Orobets
 */
public class OCompositeKey implements Comparable<OCompositeKey>, Serializable {
    /**
     * List of heterogeneous values that are going to be stored in {@link OMVRBTree}.
     */
    private final List<Comparable> keys;

    public OCompositeKey(final List<? extends Comparable> keys) {
        this();
        for(final Comparable key : keys)
            addKey(key);
    }

    public OCompositeKey() {
        this.keys = new LinkedList<Comparable>();
    }

    /**
     * @return List of heterogeneous values that are going to be stored in {@link OMVRBTree}.
     */
    public List<Comparable> getKeys() {
        return Collections.unmodifiableList(keys);
    }

    /**
     * Add new key value to the list of already registered values.
     *
     * If passed in value is {@link OCompositeKey} itself then its values will be copied in current index.
     * But key itself will not be added.
     *
     * @param key Key to add.
     */
    public void addKey(final Comparable key) {
        if (key instanceof OCompositeKey) {
            final OCompositeKey compositeKey = (OCompositeKey) key;
            for (final Comparable inKey : compositeKey.keys) {
                addKey(inKey);
            }
        } else {
            keys.add(key);
        }
    }

    /**
     * Performs partial comparison of two composite keys.
     *
     * Two objects will be equal if the common subset of their keys is equal.
     * For example if first object contains two keys and second contains four keys then only
     * first two keys will be compared.
     *
     * @param otherKey Key to compare.
     *
     * @return a negative integer, zero, or a positive integer as this object
     *		is less than, equal to, or greater than the specified object.
     */
    public int compareTo(final OCompositeKey otherKey) {
        final Iterator<Comparable> inIter = keys.iterator();
        final Iterator<Comparable> outIter = otherKey.keys.iterator();

        while (inIter.hasNext() && outIter.hasNext()) {
            final Comparable inKey = inIter.next();
            final Comparable outKey = outIter.next();

            @SuppressWarnings("unchecked")
            final int result = inKey.compareTo(outKey);
            if (result != 0)
                return result;
        }

        return 0;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final OCompositeKey that = (OCompositeKey) o;

        return keys.equals(that.keys);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public int hashCode() {
        return keys.hashCode();
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public String toString() {
        return "OCompositeKey{" +
                "keys=" + keys +
                '}';
    }
}
