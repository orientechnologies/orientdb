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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;

/**
 * @author Michael Pollmeier (http://michaelpollmeier.com)
 */
public final class OrientGraphFactory {
    public OrientGraph open(String url, String user, String password) {
        return new OrientGraph(getDatabase(url, user, password));
    }

    protected ODatabaseDocumentTx getDatabase(String url, String user, String password) {
        final ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
        db.open(user, password);

        return db;
    }
}
