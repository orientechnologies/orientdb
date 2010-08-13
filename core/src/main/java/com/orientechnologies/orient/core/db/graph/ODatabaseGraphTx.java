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
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OGraphException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.OGraphVertexIterator;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * GraphDB implementation on top of Document underlying.
 * 
 * @author Luca Garulli
 * 
 */
public class ODatabaseGraphTx extends ODatabasePojoAbstract<ODocument, OGraphElement> {

	public ODatabaseGraphTx(final String iURL) {
		super(new ODatabaseDocumentTx(iURL));
	}

	@SuppressWarnings("unchecked")
	public <THISDB extends ODatabase> THISDB open(final String iUserName, final String iUserPassword) {
		underlying.open(iUserName, iUserPassword);

		checkForGraphSchema();

		return (THISDB) this;
	}

	@SuppressWarnings("unchecked")
	public <THISDB extends ODatabase> THISDB create() {
		underlying.create();

		checkForGraphSchema();

		return (THISDB) this;
	}

	public OGraphVertex createVertex() {
		return registerPojo(new OGraphVertex(this));
	}

	public OGraphVertex getRoot(final String iName) {
		return registerPojo(new OGraphVertex(underlying.getDictionary().get(iName)));
	}

	public OGraphVertex getRoot(final String iName, final String iFetchPlan) {
		return registerPojo(new OGraphVertex(underlying.getDictionary().get(iName), iFetchPlan));
	}

	public ODatabaseGraphTx setRoot(final String iName, final OGraphVertex iNode) {
		underlying.getDictionary().put(iName, iNode.getDocument());
		return this;
	}

	public OGraphElement newInstance() {
		return registerPojo(new OGraphVertex(this));
	}

	public OGraphElement load(final OGraphElement iObject) {
		if (iObject != null)
			iObject.getDocument().load();
		return iObject;
	}

	public OGraphElement load(final ORID iRecordId) {
		if (iRecordId == null)
			return null;

		// TRY IN LOCAL CACHE
		ODocument doc = getRecordById(iRecordId);
		if (doc == null) {
			// TRY TO LOAD IT
			doc = underlying.load(iRecordId);
			if (doc == null)
				// NOT FOUND
				return null;
		}

		if (doc.getClassName() == null)
			throw new OGraphException(
					"The document loaded has no class, while it should be a OGraphVertex, OGraphEdge or any subclass of its");

		if (doc.getClassName().equals(OGraphVertex.class.getSimpleName()))
			return new OGraphVertex(doc);
		else if (doc.getClassName().equals(OGraphEdge.class.getSimpleName()))
			return new OGraphEdge(doc);
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

	public OGraphVertexIterator browseVertexes() {
		return new OGraphVertexIterator(this);
	}

	private void checkForGraphSchema() {
		if (!underlying.getMetadata().getSchema().existsClass(OGraphVertex.CLASS_NAME)) {
			// CREATE THE META MODEL USING THE ORIENT SCHEMA
			final OClass vertex = underlying.getMetadata().getSchema()
					.createClass(OGraphVertex.CLASS_NAME, underlying.addPhysicalCluster(OGraphVertex.CLASS_NAME));
			final OClass edge = underlying.getMetadata().getSchema().createClass(OGraphEdge.CLASS_NAME);

			edge.createProperty(OGraphEdge.IN, OType.LINK, vertex);
			edge.createProperty(OGraphEdge.OUT, OType.LINK, vertex);

			vertex.createProperty(OGraphVertex.FIELD_IN_EDGES, OType.EMBEDDEDLIST, edge);
			vertex.createProperty(OGraphVertex.FIELD_OUT_EDGES, OType.EMBEDDEDLIST, edge);

			underlying.getMetadata().getSchema().save();
		}
	}

	@Override
	protected ODocument pojo2Stream(final OGraphElement iPojo, ODocument record) {
		return record;
	}

	@Override
	protected Object stream2pojo(ODocument record, final OGraphElement iPojo, String iFetchPlan) {
		return iPojo;
	}

	@Override
	public OGraphElement newInstance(String iClassName) {
		if (iClassName.equals(OGraphVertex.class.getSimpleName()))
			return new OGraphVertex(this);
		else if (iClassName.equals(OGraphEdge.class.getSimpleName()))
			return new OGraphEdge(this);

		throw new OGraphException("Unrecognized class: " + iClassName);
	}

	private OGraphVertex registerPojo(final OGraphVertex iVertex) {
		registerPojo(iVertex, iVertex.getDocument());
		return iVertex;
	}
}
