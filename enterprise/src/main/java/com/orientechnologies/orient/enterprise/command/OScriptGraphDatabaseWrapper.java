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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.db.ODataSegmentStrategy;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseComplex.OPERATION_MODE;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Database wrapper class to use from scripts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OScriptGraphDatabaseWrapper {
	private OGraphDatabase	database;

	public OScriptGraphDatabaseWrapper(final OGraphDatabase database) {
		this.database = database;
	}

	public OScriptGraphDatabaseWrapper(final ODatabaseDocumentTx database) {
		this.database = new OGraphDatabase((ODatabaseRecordTx) database.getUnderlying());
	}

	public OScriptGraphDatabaseWrapper(final ODatabaseRecordTx database) {
		this.database = new OGraphDatabase(database);
	}

	public OScriptGraphDatabaseWrapper(final String iURL) {
		this.database = new OGraphDatabase(iURL);
	}

	public List<OIdentifiable> query(final String iText) {
		return database.query(new OSQLSynchQuery<Object>(iText));
	}

	public Object command(final String iText) {
		return database.command(new OCommandSQL(iText));
	}

	public boolean exists() {
		return database.exists();
	}

	public ODocument newInstance() {
		return database.newInstance();
	}

	public void reload() {
		database.reload();
	}

	public ODocument newInstance(String iClassName) {
		return database.newInstance(iClassName);
	}

	public ORecordIteratorClass<ODocument> browseClass(String iClassName) {
		return database.browseClass(iClassName);
	}

	public STATUS getStatus() {
		return database.getStatus();
	}

	public ORecordIteratorClass<ODocument> browseClass(String iClassName, boolean iPolymorphic) {
		return database.browseClass(iClassName, iPolymorphic);
	}

	public <THISDB extends ODatabase> THISDB setStatus(STATUS iStatus) {
		return database.setStatus(iStatus);
	}

	public void drop() {
		database.drop();
	}

	public String getName() {
		return database.getName();
	}

	public int addCluster(String iType, String iClusterName, String iLocation, String iDataSegmentName, Object... iParameters) {
		return database.addCluster(iType, iClusterName, iLocation, iDataSegmentName, iParameters);
	}

	public String getURL() {
		return database.getURL();
	}

	public ORecordIteratorCluster<ODocument> browseCluster(String iClusterName) {
		return database.browseCluster(iClusterName);
	}

	public boolean isClosed() {
		return database.isClosed();
	}

	public <THISDB extends ODatabase> THISDB open(String iUserName, String iUserPassword) {
		return database.open(iUserName, iUserPassword);
	}

	public ODatabaseDocumentTx save(ORecordInternal<?> iRecord) {
		return database.save(iRecord);
	}

	public boolean dropCluster(String iClusterName) {
		return database.dropCluster(iClusterName);
	}

	public <THISDB extends ODatabase> THISDB create() {
		return database.create();
	}

	public boolean dropCluster(int iClusterId) {
		return database.dropCluster(iClusterId);
	}

	public void close() {
		database.close();
	}

	public int getClusters() {
		return database.getClusters();
	}

	public long countVertexes() {
		return database.countVertexes();
	}

	public Collection<String> getClusterNames() {
		return database.getClusterNames();
	}

	public int addDataSegment(String iName, String iLocation) {
		return database.addDataSegment(iName, iLocation);
	}

	public long countEdges() {
		return database.countEdges();
	}

	public Iterable<ODocument> browseVertices() {
		return database.browseVertices();
	}

	public String getClusterType(String iClusterName) {
		return database.getClusterType(iClusterName);
	}

	public Iterable<ODocument> browseVertices(boolean iPolymorphic) {
		return database.browseVertices(iPolymorphic);
	}

	public OTransaction getTransaction() {
		return database.getTransaction();
	}

	public int getDataSegmentIdByName(String iDataSegmentName) {
		return database.getDataSegmentIdByName(iDataSegmentName);
	}

	public ODatabaseComplex<ORecordInternal<?>> begin() {
		return database.begin();
	}

	public Iterable<ODocument> browseEdges() {
		return database.browseEdges();
	}

	public String getDataSegmentNameById(int iDataSegmentId) {
		return database.getDataSegmentNameById(iDataSegmentId);
	}

	public Iterable<ODocument> browseEdges(boolean iPolymorphic) {
		return database.browseEdges(iPolymorphic);
	}

	public int getClusterIdByName(String iClusterName) {
		return database.getClusterIdByName(iClusterName);
	}

	public Iterable<ODocument> browseElements(String iClass, boolean iPolymorphic) {
		return database.browseElements(iClass, iPolymorphic);
	}

	public boolean isMVCC() {
		return database.isMVCC();
	}

	public String getClusterNameById(int iClusterId) {
		return database.getClusterNameById(iClusterId);
	}

	public <RET extends ODatabaseComplex<?>> RET setMVCC(boolean iValue) {
		return database.setMVCC(iValue);
	}

	public ODocument createVertex() {
		return database.createVertex();
	}

	public long getClusterRecordSizeById(int iClusterId) {
		return database.getClusterRecordSizeById(iClusterId);
	}

	public ODocument createVertex(String iClassName) {
		return database.createVertex(iClassName);
	}

	public boolean isValidationEnabled() {
		return database.isValidationEnabled();
	}

	public long getClusterRecordSizeByName(String iClusterName) {
		return database.getClusterRecordSizeByName(iClusterName);
	}

	public <RET extends ODatabaseRecord> RET setValidationEnabled(boolean iValue) {
		return database.setValidationEnabled(iValue);
	}

	public ODocument createEdge(ORID iSourceVertexRid, ORID iDestVertexRid) {
		return database.createEdge(iSourceVertexRid, iDestVertexRid);
	}

	public OUser getUser() {
		return database.getUser();
	}

	public ODatabaseDocumentTx save(ORecordInternal<?> iRecord, OPERATION_MODE iMode) {
		return database.save(iRecord, iMode);
	}

	public OMetadata getMetadata() {
		return database.getMetadata();
	}

	public ODocument createEdge(ORID iSourceVertexRid, ORID iDestVertexRid, String iClassName) {
		return database.createEdge(iSourceVertexRid, iDestVertexRid, iClassName);
	}

	public ODictionary<ORecordInternal<?>> getDictionary() {
		return database.getDictionary();
	}

	public byte getRecordType() {
		return database.getRecordType();
	}

	public void removeVertex(ODocument iVertex) {
		database.removeVertex(iVertex);
	}

	public ODatabaseComplex<ORecordInternal<?>> delete(ORID iRid) {
		return database.delete(iRid);
	}

	public boolean dropDataSegment(String name) {
		return database.dropDataSegment(name);
	}

	public <RET extends ORecordInternal<?>> RET load(ORID iRecordId) {
		return database.load(iRecordId);
	}

	public <RET extends ORecordInternal<?>> RET load(ORID iRecordId, String iFetchPlan) {
		return database.load(iRecordId, iFetchPlan);
	}

	public <RET extends ORecordInternal<?>> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache) {
		return database.load(iRecordId, iFetchPlan, iIgnoreCache);
	}

	public <RET extends ORecordInternal<?>> RET getRecord(OIdentifiable iIdentifiable) {
		return database.getRecord(iIdentifiable);
	}

	public int getDefaultClusterId() {
		return database.getDefaultClusterId();
	}

	public <RET extends ORecordInternal<?>> RET load(ORecordInternal<?> iRecord) {
		return database.load(iRecord);
	}

	public void removeEdge(ODocument iEdge) {
		database.removeEdge(iEdge);
	}

	public boolean declareIntent(OIntent iIntent) {
		return database.declareIntent(iIntent);
	}

	public <RET extends ORecordInternal<?>> RET load(ORecordInternal<?> iRecord, String iFetchPlan) {
		return database.load(iRecord, iFetchPlan);
	}

	public <RET extends ORecordInternal<?>> RET load(ORecordInternal<?> iRecord, String iFetchPlan, boolean iIgnoreCache) {
		return database.load(iRecord, iFetchPlan, iIgnoreCache);
	}

	public ODatabaseComplex<?> setDatabaseOwner(ODatabaseComplex<?> iOwner) {
		return database.setDatabaseOwner(iOwner);
	}

	public void reload(ORecordInternal<?> iRecord) {
		database.reload(iRecord);
	}

	public void reload(ORecordInternal<?> iRecord, String iFetchPlan, boolean iIgnoreCache) {
		database.reload(iRecord, iFetchPlan, iIgnoreCache);
	}

	public Object setProperty(String iName, Object iValue) {
		return database.setProperty(iName, iValue);
	}

	public ODocument createEdge(ODocument iSourceVertex, ODocument iDestVertex) {
		return database.createEdge(iSourceVertex, iDestVertex);
	}

	public ODatabaseDocumentTx save(ORecordInternal<?> iRecord, String iClusterName) {
		return database.save(iRecord, iClusterName);
	}

	public Object getProperty(String iName) {
		return database.getProperty(iName);
	}

	public ODocument createEdge(ODocument iOutVertex, ODocument iInVertex, String iClassName) {
		return database.createEdge(iOutVertex, iInVertex, iClassName);
	}

	public Iterator<Entry<String, Object>> getProperties() {
		return database.getProperties();
	}

	public Object get(ATTRIBUTES iAttribute) {
		return database.get(iAttribute);
	}

	public <THISDB extends ODatabase> THISDB set(ATTRIBUTES attribute, Object iValue) {
		return database.set(attribute, iValue);
	}

	public void setInternal(ATTRIBUTES attribute, Object iValue) {
		database.setInternal(attribute, iValue);
	}

	public boolean isRetainRecords() {
		return database.isRetainRecords();
	}

	public ODatabaseRecord setRetainRecords(boolean iValue) {
		return database.setRetainRecords(iValue);
	}

	public long getSize() {
		return database.getSize();
	}

	public ORecordInternal<?> getRecordByUserObject(Object iUserObject, boolean iCreateIfNotAvailable) {
		return database.getRecordByUserObject(iUserObject, iCreateIfNotAvailable);
	}

	public Set<ODocument> getEdgesBetweenVertexes(ODocument iVertex1, ODocument iVertex2) {
		return database.getEdgesBetweenVertexes(iVertex1, iVertex2);
	}

	public Set<ODocument> getEdgesBetweenVertexes(ODocument iVertex1, ODocument iVertex2, String[] iLabels) {
		return database.getEdgesBetweenVertexes(iVertex1, iVertex2, iLabels);
	}

	public ODatabaseDocumentTx save(ORecordInternal<?> iRecord, String iClusterName, OPERATION_MODE iMode) {
		return database.save(iRecord, iClusterName, iMode);
	}

	public Set<ODocument> getEdgesBetweenVertexes(ODocument iVertex1, ODocument iVertex2, String[] iLabels, String[] iClassNames) {
		return database.getEdgesBetweenVertexes(iVertex1, iVertex2, iLabels, iClassNames);
	}

	public ODataSegmentStrategy getDataSegmentStrategy() {
		return database.getDataSegmentStrategy();
	}

	public void setDataSegmentStrategy(ODataSegmentStrategy dataSegmentStrategy) {
		database.setDataSegmentStrategy(dataSegmentStrategy);
	}

	public Set<OIdentifiable> getOutEdges(OIdentifiable iVertex) {
		return database.getOutEdges(iVertex);
	}

	public Set<OIdentifiable> getOutEdges(OIdentifiable iVertex, String iLabel) {
		return database.getOutEdges(iVertex, iLabel);
	}

	public Set<OIdentifiable> getOutEdgesHavingProperties(OIdentifiable iVertex, Map<String, Object> iProperties) {
		return database.getOutEdgesHavingProperties(iVertex, iProperties);
	}

	public ODatabaseDocumentTx delete(ODocument iRecord) {
		return database.delete(iRecord);
	}

	public Set<OIdentifiable> getOutEdgesHavingProperties(OIdentifiable iVertex, Iterable<String> iProperties) {
		return database.getOutEdgesHavingProperties(iVertex, iProperties);
	}

	public Set<OIdentifiable> getInEdges(OIdentifiable iVertex) {
		return database.getInEdges(iVertex);
	}

	public Set<OIdentifiable> getInEdges(OIdentifiable iVertex, String iLabel) {
		return database.getInEdges(iVertex, iLabel);
	}

	public Set<OIdentifiable> getInEdgesHavingProperties(OIdentifiable iVertex, Iterable<String> iProperties) {
		return database.getInEdgesHavingProperties(iVertex, iProperties);
	}

	public long countClass(String iClassName) {
		return database.countClass(iClassName);
	}

	public ODatabaseComplex<ORecordInternal<?>> commit() {
		return database.commit();
	}

	public ODatabaseComplex<ORecordInternal<?>> rollback() {
		return database.rollback();
	}

	public Set<OIdentifiable> getInEdgesHavingProperties(ODocument iVertex, Map<String, Object> iProperties) {
		return database.getInEdgesHavingProperties(iVertex, iProperties);
	}

	public ODocument getInVertex(OIdentifiable iEdge) {
		return database.getInVertex(iEdge);
	}

	public ODocument getOutVertex(OIdentifiable iEdge) {
		return database.getOutVertex(iEdge);
	}

	public ODocument getRoot(String iName) {
		return database.getRoot(iName);
	}

	public ODocument getRoot(String iName, String iFetchPlan) {
		return database.getRoot(iName, iFetchPlan);
	}

	public OGraphDatabase setRoot(String iName, ODocument iNode) {
		return database.setRoot(iName, iNode);
	}

	public OClass createVertexType(String iClassName) {
		return database.createVertexType(iClassName);
	}

	public OClass createVertexType(String iClassName, String iSuperClassName) {
		return database.createVertexType(iClassName, iSuperClassName);
	}

	public OClass createVertexType(String iClassName, OClass iSuperClass) {
		return database.createVertexType(iClassName, iSuperClass);
	}

	public OClass getVertexType(String iClassName) {
		return database.getVertexType(iClassName);
	}

	public OClass createEdgeType(String iClassName) {
		return database.createEdgeType(iClassName);
	}

	public OClass createEdgeType(String iClassName, String iSuperClassName) {
		return database.createEdgeType(iClassName, iSuperClassName);
	}

	public OClass createEdgeType(String iClassName, OClass iSuperClass) {
		return database.createEdgeType(iClassName, iSuperClass);
	}

	public OClass getEdgeType(String iClassName) {
		return database.getEdgeType(iClassName);
	}

	public boolean isSafeMode() {
		return database.isSafeMode();
	}

	public void setSafeMode(boolean safeMode) {
		database.setSafeMode(safeMode);
	}

	public OClass getVertexBaseClass() {
		return database.getVertexBaseClass();
	}

	public OClass getEdgeBaseClass() {
		return database.getEdgeBaseClass();
	}

	public Set<OIdentifiable> filterEdgesByProperties(OMVRBTreeRIDSet iEdges, Iterable<String> iPropertyNames) {
		return database.filterEdgesByProperties(iEdges, iPropertyNames);
	}

	public Set<OIdentifiable> filterEdgesByProperties(OMVRBTreeRIDSet iEdges, Map<String, Object> iProperties) {
		return database.filterEdgesByProperties(iEdges, iProperties);
	}

	public boolean isUseCustomTypes() {
		return database.isUseCustomTypes();
	}

	public void setUseCustomTypes(boolean useCustomTypes) {
		database.setUseCustomTypes(useCustomTypes);
	}

	public boolean isVertex(ODocument iRecord) {
		return database.isVertex(iRecord);
	}

	public boolean isEdge(ODocument iRecord) {
		return database.isEdge(iRecord);
	}

	public String getType() {
		return database.getType();
	}
}
