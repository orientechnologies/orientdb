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

		if (!getMetadata().getSchema().existsClass("OGraphNode")) {
			final OClass node = getMetadata().getSchema().createClass("OGraphNode", addPhysicalCluster("OGraphNode"));
			final OClass arc = getMetadata().getSchema().createClass("OGraphArc", addPhysicalCluster("OGraphArc"));

			arc.createProperty("from", OType.LINK, node);
			arc.createProperty("to", OType.LINK, node);
			node.createProperty("arcs", OType.EMBEDDEDLIST, arc);

			getMetadata().getSchema().save();
		}

		return (THISDB) this;
	}

	public OGraphNode createNode() {
		return new OGraphNode(this);
	}

	public OGraphNode getRoot(final String iName) {
		return new OGraphNode(getDictionary().get(iName));
	}

	public OGraphNode getRoot(final String iName, final String iFetchPlan) {
		return new OGraphNode(getDictionary().get(iName), iFetchPlan);
	}

	public ODatabaseGraphTx setRoot(final String iName, final OGraphNode iNode) {
		getDictionary().put(iName, iNode.getDocument());
		return this;
	}
}
