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
import com.orientechnologies.orient.core.db.ODatabaseBridgeWrapperAbstract;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * GraphDB implementation on top of Document underlying.
 * 
 * @author Luca Garulli
 * 
 */
public class ODatabaseGraphTx extends ODatabaseBridgeWrapperAbstract<ODatabaseDocumentTx, ODocument, OGraphElement> {

	public ODatabaseGraphTx(final String iURL) {
		super(new ODatabaseDocumentTx(iURL));
	}

	@SuppressWarnings("unchecked")
	public <THISDB extends ODatabase> THISDB open(String iUserName, String iUserPassword) {
		underlying.open(iUserName, iUserPassword);

		if (!underlying.getMetadata().getSchema().existsClass("OGraphNode")) {
			final OClass node = underlying.getMetadata().getSchema()
					.createClass("OGraphNode", underlying.addPhysicalCluster("OGraphNode"));
			final OClass arc = underlying.getMetadata().getSchema().createClass("OGraphArc", underlying.addPhysicalCluster("OGraphArc"));

			arc.createProperty("from", OType.LINK, node);
			arc.createProperty("to", OType.LINK, node);
			node.createProperty("arcs", OType.EMBEDDEDLIST, arc);

			underlying.getMetadata().getSchema().save();
		}

		return (THISDB) this;
	}

	public OGraphNode createNode() {
		return new OGraphNode(this);
	}

	public OGraphNode getRoot(final String iName) {
		return new OGraphNode(underlying.getDictionary().get(iName));
	}

	public OGraphNode getRoot(final String iName, final String iFetchPlan) {
		return new OGraphNode(underlying.getDictionary().get(iName), iFetchPlan);
	}

	public ODatabaseGraphTx setRoot(final String iName, final OGraphNode iNode) {
		underlying.getDictionary().put(iName, iNode.getDocument());
		return this;
	}

	public OGraphElement newInstance() {
		return new OGraphNode(this);
	}

	public OGraphElement load(final OGraphElement iObject) {
		if (iObject != null)
			iObject.getDocument().load();
		return iObject;
	}

	public OGraphElement load(final ORID iRecordId) {
		if (iRecordId == null)
			return null;

		final ODocument doc = underlying.load(iRecordId);
		if (doc == null)
			return null;

		if (doc.getClassName().equals(OGraphNode.class.getSimpleName()))
			return new OGraphNode(doc);
		else if (doc.getClassName().equals(OGraphArc.class.getSimpleName()))
			return new OGraphArc(doc);
		else
			throw new IllegalArgumentException("RecordID is not of supported type. Class=" + doc.getClassName());
	}

	public ODatabaseComplex<OGraphElement> save(final OGraphElement iObject) {
		iObject.getDocument().save();
		return this;
	}

	public ODatabaseComplex<OGraphElement> save(final OGraphElement iObject, final String iClusterName) {
		iObject.getDocument().save(iClusterName);
		return this;
	}

	public ODatabaseComplex<OGraphElement> delete(final OGraphElement iObject) {
		iObject.getDocument().delete();
		return this;
	}

	public ODictionary<OGraphElement> getDictionary() {
		return null;
	}

}
