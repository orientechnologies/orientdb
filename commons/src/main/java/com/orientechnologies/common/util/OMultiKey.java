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
package com.orientechnologies.common.util;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * Multiple key container that is used as key for {@link java.util.Map}.
 * Despite of the {@link java.util.List} order of keys does not matter, but unlike {@link java.util.Set} can contain
 * duplicate values.
 *
 */
public class OMultiKey {
    private final Collection<?> keys;
    private final int hash;

    public OMultiKey(final Collection<?> keys) {
        this.keys = new ArrayList<Object>(keys);
        hash = generateHashCode(keys);
    }

    private int generateHashCode(final Collection<?> objects) {
        int total = 0;
        for (final Object object : objects) {
            total ^= object.hashCode();
        }
        return total;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return hash;
    }

    /**
     * Objects are equals if they contain the same amount of keys and these keys are equals.
     * Order of keys does not matter.
     *
     * @param o obj the reference object with which to compare.
     * @return  <code>true</code> if this object is the same as the obj
     *          argument; <code>false</code> otherwise.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final OMultiKey oMultiKey = (OMultiKey) o;

        if(keys.size() != oMultiKey.keys.size())
            return false;

        for (final Object inKey : keys) {
            if (!oMultiKey.keys.contains(inKey))
                return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "OMultiKey " + keys + "";
    }
}
