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

import java.util.Collection;
import java.util.IdentityHashMap;

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
 * GraphDB implementation on top of underlying Document. Graph Element are Object itself. If you want a lighter version see
 * {@link OGraphDatabase}.
 * 
 * @author Luca Garulli
 * 
 */
public class ODatabaseGraphTx extends ODatabasePojoAbstract<OGraphElement> {

	public ODatabaseGraphTx(final String iURL) {
		super(new ODatabaseDocumentTx(iURL));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <THISDB extends ODatabase> THISDB open(final String iUserName, final String iUserPassword) {
		underlying.open(iUserName, iUserPassword);

		checkForGraphSchema();

		return (THISDB) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <THISDB extends ODatabase> THISDB create() {
		underlying.create();

		checkForGraphSchema();

		return (THISDB) this;
	}

	public OGraphVertex createVertex() {
		return new OGraphVertex(this);
	}

	public OGraphVertex createVertex(final String iClassName) {
		return new OGraphVertex(this, iClassName);
	}

	public OGraphVertex getRoot(final String iName) {
		return registerPojo(new OGraphVertex(this, (ODocument) underlying.getDictionary().get(iName)));
	}

	public OGraphVertex getRoot(final String iName, final String iFetchPlan) {
		return registerPojo(new OGraphVertex(this, (ODocument) underlying.getDictionary().get(iName), iFetchPlan));
	}

	public ODatabaseGraphTx setRoot(final String iName, final OGraphVertex iNode) {
		underlying.getDictionary().put(iName, iNode.getDocument());
		return this;
	}

	@SuppressWarnings("unchecked")
	public OGraphElement newInstance() {
		return new OGraphVertex(this);
	}

	@SuppressWarnings("unchecked")
	public OGraphElement load(final OGraphElement iObject) {
		if (iObject != null)
			iObject.getDocument().load();
		return iObject;
	}

	@SuppressWarnings("unchecked")
	public OGraphElement load(final ORID iRecordId) {
		ODocument doc = loadAsDocument(iRecordId);

		if (doc == null)
			return null;

		return newInstance(doc.getClassName()).setDocument(doc);
	}

	public ODocument loadAsDocument(final ORID iRecordId) {
		if (iRecordId == null)
			return null;

		// TRY IN LOCAL CACHE
		ODocument doc = getRecordById(iRecordId);
		if (doc == null) {
			// TRY TO LOAD IT
			doc = (ODocument) underlying.load(iRecordId);
			if (doc == null)
				// NOT FOUND
				return null;
		}
		if (doc.getClassName() == null)
			throw new OGraphException("The document loaded has no class, while it should be a OGraphVertex, OGraphEdge or any subclass of its");

		return doc;
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
		unregisterPojo(iObject, iObject.getDocument());
		return this;
	}

	public ODictionary<OGraphElement> getDictionary() {
		return null;
	}

	public OGraphVertexIterator browseVertexes() {
		return new OGraphVertexIterator(this, true);
	}

	public OGraphVertexIterator browseVertexes(final boolean iPolymorphic) {
		return new OGraphVertexIterator(this, iPolymorphic);
	}

	@SuppressWarnings("unchecked")
	public OGraphElement newInstance(final String iClassName) {
		if (iClassName.equals(OGraphVertex.class.getSimpleName()))
			return new OGraphVertex(this);
		else if (iClassName.equals(OGraphEdge.class.getSimpleName()))
			return new OGraphEdge(this);

		OClass cls = getMetadata().getSchema().getClass(iClassName);
		if (cls != null) {
			cls = cls.getSuperClass();
			while (cls != null) {
				if (cls.getName().equals(OGraphVertex.class.getSimpleName()))
					return new OGraphVertex(this, iClassName);
				else if (cls.getName().equals(OGraphEdge.class.getSimpleName()))
					return new OGraphEdge(this, iClassName);

				cls = cls.getSuperClass();
			}
		}

		throw new OGraphException("Unrecognized class: " + iClassName);
	}

	public IdentityHashMap<ODocument, OGraphElement> getRecords2Objects() {
		return records2Objects;
	}

	public void removeCachedElements(final Collection<OGraphElement> iElements) {
		for (OGraphElement e : iElements) {
			records2Objects.remove(e.getDocument());
			rid2Records.remove(e.getDocument().getIdentity());
			objects2Records.remove(System.identityHashCode(e));
		}
	}

	private OGraphVertex registerPojo(final OGraphVertex iVertex) {
		registerPojo(iVertex, iVertex.getDocument());
		return iVertex;
	}

	private void checkForGraphSchema() {
		if (!underlying.getMetadata().getSchema().existsClass(OGraphVertex.CLASS_NAME)) {
			// CREATE THE META MODEL USING THE ORIENT SCHEMA
			final OClass vertex = underlying.getMetadata().getSchema()
					.createClass(OGraphVertex.CLASS_NAME, underlying.addPhysicalCluster(OGraphVertex.CLASS_NAME));
			final OClass edge = underlying.getMetadata().getSchema()
					.createClass(OGraphEdge.CLASS_NAME, underlying.addPhysicalCluster(OGraphEdge.CLASS_NAME));

			edge.createProperty(OGraphEdge.IN, OType.LINK, vertex);
			edge.createProperty(OGraphEdge.OUT, OType.LINK, vertex);

			vertex.createProperty(OGraphVertex.FIELD_IN_EDGES, OType.LINKLIST, edge);
			vertex.createProperty(OGraphVertex.FIELD_OUT_EDGES, OType.LINKLIST, edge);

			underlying.getMetadata().getSchema().save();
		}
	}

	@Override
	protected ODocument pojo2Stream(final OGraphElement iPojo, ODocument record) {
		return record;
	}

	@Override
	protected Object stream2pojo(ODocument record, final Object iPojo, String iFetchPlan) {
		((OGraphElement) iPojo).setDocument(record);
		return iPojo;
	}

	@Override
	public void delete() {
		underlying.delete();
	}

	public long countClass(final String iClassName) {
		return underlying.countClass(iClassName);
	}
}
