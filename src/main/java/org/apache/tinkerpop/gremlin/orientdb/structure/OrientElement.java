/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.orientdb.structure;

import org.apache.tinkerpop.gremlin.structure.*;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.commons.lang.NotImplementedException;
import java.util.Set;

/**
 * @author Michael Pollmeier (http://michaelpollmeier.com)
 */
public class OrientElement implements Element {
    protected OIdentifiable rawElement;
    protected OrientGraph graph;

    public OrientElement(final OrientGraph graph, final OIdentifiable rawElement) {
        this.graph = graph;
        this.rawElement = rawElement;
    }

    public Object id() {
        return rawElement.getIdentity();
    }

    public String label() {
        throw new NotImplementedException();
    }

    public Graph graph() {
        return graph;
    }

    public <V> Property<V> property(final String key, final V value) {
        throw new NotImplementedException();
    }

    public void remove() {
        throw new NotImplementedException();
    }

    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        throw new NotImplementedException();
    }
}
