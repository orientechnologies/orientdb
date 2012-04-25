/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.enterprise.command;

import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Database wrapper class to use from scripts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OScriptGraphDatabaseWrapper extends OScriptDocumentDatabaseWrapper {

	public OScriptGraphDatabaseWrapper(final OGraphDatabase database) {
		super(database);
	}

	public OScriptGraphDatabaseWrapper(final ODatabaseDocumentTx database) {
		super(new OGraphDatabase((ODatabaseRecordTx) ((OGraphDatabase) database).getUnderlying()));
	}

	public OScriptGraphDatabaseWrapper(final ODatabaseRecordTx database) {
		super(new OGraphDatabase(database));
	}

	public OScriptGraphDatabaseWrapper(final String iURL) {
		super(new OGraphDatabase(iURL));
	}

	public long countVertexes() {
		return ((OGraphDatabase) database).countVertexes();
	}

	public long countEdges() {
		return ((OGraphDatabase) database).countEdges();
	}

	public Iterable<ODocument> browseVertices() {
		return ((OGraphDatabase) database).browseVertices();
	}

	public Iterable<ODocument> browseVertices(boolean iPolymorphic) {
		return ((OGraphDatabase) database).browseVertices(iPolymorphic);
	}

	public Iterable<ODocument> browseEdges() {
		return ((OGraphDatabase) database).browseEdges();
	}

	public Iterable<ODocument> browseEdges(boolean iPolymorphic) {
		return ((OGraphDatabase) database).browseEdges(iPolymorphic);
	}

	public Iterable<ODocument> browseElements(String iClass, boolean iPolymorphic) {
		return ((OGraphDatabase) database).browseElements(iClass, iPolymorphic);
	}

	public ODocument createVertex() {
		return ((OGraphDatabase) database).createVertex();
	}

	public ODocument createVertex(String iClassName) {
		return ((OGraphDatabase) database).createVertex(iClassName);
	}

	public ODocument createEdge(ORID iSourceVertexRid, ORID iDestVertexRid) {
		return ((OGraphDatabase) database).createEdge(iSourceVertexRid, iDestVertexRid);
	}

	public ODocument createEdge(ORID iSourceVertexRid, ORID iDestVertexRid, String iClassName) {
		return ((OGraphDatabase) database).createEdge(iSourceVertexRid, iDestVertexRid, iClassName);
	}

	public void removeVertex(ODocument iVertex) {
		((OGraphDatabase) database).removeVertex(iVertex);
	}

	public void removeEdge(ODocument iEdge) {
		((OGraphDatabase) database).removeEdge(iEdge);
	}

	public ODocument createEdge(ODocument iSourceVertex, ODocument iDestVertex) {
		return ((OGraphDatabase) database).createEdge(iSourceVertex, iDestVertex);
	}

	public ODocument createEdge(ODocument iOutVertex, ODocument iInVertex, String iClassName) {
		return ((OGraphDatabase) database).createEdge(iOutVertex, iInVertex, iClassName);
	}

	public Set<ODocument> getEdgesBetweenVertexes(ODocument iVertex1, ODocument iVertex2) {
		return ((OGraphDatabase) database).getEdgesBetweenVertexes(iVertex1, iVertex2);
	}

	public Set<ODocument> getEdgesBetweenVertexes(ODocument iVertex1, ODocument iVertex2, String[] iLabels) {
		return ((OGraphDatabase) database).getEdgesBetweenVertexes(iVertex1, iVertex2, iLabels);
	}

	public Set<ODocument> getEdgesBetweenVertexes(ODocument iVertex1, ODocument iVertex2, String[] iLabels, String[] iClassNames) {
		return ((OGraphDatabase) database).getEdgesBetweenVertexes(iVertex1, iVertex2, iLabels, iClassNames);
	}

	public Set<OIdentifiable> getOutEdges(OIdentifiable iVertex) {
		return ((OGraphDatabase) database).getOutEdges(iVertex);
	}

	public Set<OIdentifiable> getOutEdges(OIdentifiable iVertex, String iLabel) {
		return ((OGraphDatabase) database).getOutEdges(iVertex, iLabel);
	}

	public Set<OIdentifiable> getOutEdgesHavingProperties(OIdentifiable iVertex, Map<String, Object> iProperties) {
		return ((OGraphDatabase) database).getOutEdgesHavingProperties(iVertex, iProperties);
	}

	public Set<OIdentifiable> getOutEdgesHavingProperties(OIdentifiable iVertex, Iterable<String> iProperties) {
		return ((OGraphDatabase) database).getOutEdgesHavingProperties(iVertex, iProperties);
	}

	public Set<OIdentifiable> getInEdges(OIdentifiable iVertex) {
		return ((OGraphDatabase) database).getInEdges(iVertex);
	}

	public Set<OIdentifiable> getInEdges(OIdentifiable iVertex, String iLabel) {
		return ((OGraphDatabase) database).getInEdges(iVertex, iLabel);
	}

	public Set<OIdentifiable> getInEdgesHavingProperties(OIdentifiable iVertex, Iterable<String> iProperties) {
		return ((OGraphDatabase) database).getInEdgesHavingProperties(iVertex, iProperties);
	}

	public Set<OIdentifiable> getInEdgesHavingProperties(ODocument iVertex, Map<String, Object> iProperties) {
		return ((OGraphDatabase) database).getInEdgesHavingProperties(iVertex, iProperties);
	}

	public ODocument getInVertex(OIdentifiable iEdge) {
		return ((OGraphDatabase) database).getInVertex(iEdge);
	}

	public ODocument getOutVertex(OIdentifiable iEdge) {
		return ((OGraphDatabase) database).getOutVertex(iEdge);
	}

	public ODocument getRoot(String iName) {
		return ((OGraphDatabase) database).getRoot(iName);
	}

	public ODocument getRoot(String iName, String iFetchPlan) {
		return ((OGraphDatabase) database).getRoot(iName, iFetchPlan);
	}

	public OGraphDatabase setRoot(String iName, ODocument iNode) {
		return ((OGraphDatabase) database).setRoot(iName, iNode);
	}

	public OClass createVertexType(String iClassName) {
		return ((OGraphDatabase) database).createVertexType(iClassName);
	}

	public OClass createVertexType(String iClassName, String iSuperClassName) {
		return ((OGraphDatabase) database).createVertexType(iClassName, iSuperClassName);
	}

	public OClass createVertexType(String iClassName, OClass iSuperClass) {
		return ((OGraphDatabase) database).createVertexType(iClassName, iSuperClass);
	}

	public OClass getVertexType(String iClassName) {
		return ((OGraphDatabase) database).getVertexType(iClassName);
	}

	public OClass createEdgeType(String iClassName) {
		return ((OGraphDatabase) database).createEdgeType(iClassName);
	}

	public OClass createEdgeType(String iClassName, String iSuperClassName) {
		return ((OGraphDatabase) database).createEdgeType(iClassName, iSuperClassName);
	}

	public OClass createEdgeType(String iClassName, OClass iSuperClass) {
		return ((OGraphDatabase) database).createEdgeType(iClassName, iSuperClass);
	}

	public OClass getEdgeType(String iClassName) {
		return ((OGraphDatabase) database).getEdgeType(iClassName);
	}

	public boolean isSafeMode() {
		return ((OGraphDatabase) database).isSafeMode();
	}

	public void setSafeMode(boolean safeMode) {
		((OGraphDatabase) database).setSafeMode(safeMode);
	}

	public OClass getVertexBaseClass() {
		return ((OGraphDatabase) database).getVertexBaseClass();
	}

	public OClass getEdgeBaseClass() {
		return ((OGraphDatabase) database).getEdgeBaseClass();
	}

	public Set<OIdentifiable> filterEdgesByProperties(OMVRBTreeRIDSet iEdges, Iterable<String> iPropertyNames) {
		return ((OGraphDatabase) database).filterEdgesByProperties(iEdges, iPropertyNames);
	}

	public Set<OIdentifiable> filterEdgesByProperties(OMVRBTreeRIDSet iEdges, Map<String, Object> iProperties) {
		return ((OGraphDatabase) database).filterEdgesByProperties(iEdges, iProperties);
	}

	public boolean isUseCustomTypes() {
		return ((OGraphDatabase) database).isUseCustomTypes();
	}

	public void setUseCustomTypes(boolean useCustomTypes) {
		((OGraphDatabase) database).setUseCustomTypes(useCustomTypes);
	}

	public boolean isVertex(ODocument iRecord) {
		return ((OGraphDatabase) database).isVertex(iRecord);
	}

	public boolean isEdge(ODocument iRecord) {
		return ((OGraphDatabase) database).isEdge(iRecord);
	}

}
