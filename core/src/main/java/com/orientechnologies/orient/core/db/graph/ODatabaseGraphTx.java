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
package com.orientechnologies.orient.core.db.graph;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ONode;

/**
 * GraphDB implementation on top of Document Database.
 * 
 * @author Luca Garulli
 * 
 */
public class ODatabaseGraphTx extends ODatabaseDocumentTx {

	public ODatabaseGraphTx(String iURL) {
		super(iURL);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <THISDB extends ODatabase> THISDB open(String iUserName, String iUserPassword) {
		super.open(iUserName, iUserPassword);

		if (!getMetadata().getSchema().existsClass("ONode")) {
			final OClass node = getMetadata().getSchema().createClass("ONode", addPhysicalCluster("ONode"));
			final OClass arc = getMetadata().getSchema().createClass("OArc", addPhysicalCluster("OArc"));

			arc.createProperty("from", OType.LINK, node);
			arc.createProperty("to", OType.LINK, node);
			node.createProperty("arcs", OType.EMBEDDEDLIST, arc);

			getMetadata().getSchema().save();
		}

		return (THISDB) this;
	}

	public ONode createNode() {
		return new ONode(this);
	}

	public ONode getRoot(final String iName) {
		return (ONode) getDictionary().get(iName);
	}

	public ODatabaseGraphTx setRoot(final String iName, final ONode iNode) {
		getDictionary().put(iName, iNode);
		return this;
	}
}
