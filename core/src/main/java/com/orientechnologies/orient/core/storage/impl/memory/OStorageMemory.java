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
package com.orientechnologies.orient.core.storage.impl.memory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.query.OQueryExecutor;
import com.orientechnologies.orient.core.query.sql.OSQLAsynchQuery;
import com.orientechnologies.orient.core.query.sql.OSQLAsynchQueryLocalExecutor;
import com.orientechnologies.orient.core.query.sql.OSQLSynchQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.logical.OClusterLogical;
import com.orientechnologies.orient.core.tx.OTransaction;

/**
 * Memory implementation of storage. This storage works only in memory and has the following features:
 * <ul>
 * <li>The name is "Memory"</li>
 * <li>Has a unique Data Segment</li>
 * </ul>
 * 
 * @author Luca Garulli
 * 
 */
public class OStorageMemory extends OStorageAbstract {
	private final List<OClusterMemory>	clusters				= new ArrayList<OClusterMemory>();
	private final List<OClusterLogical>	logicalClusters	= new ArrayList<OClusterLogical>();
	private final ODataSegmentMemory		data						= new ODataSegmentMemory();

	public OStorageMemory() {
		super("Memory", "Memory", "rw");
	}

	public void open(final int iRequesterId, final String iUserName, final String iUserPassword) {
		configuration = new OStorageConfiguration(this);
		addCluster("index");

		open = true;
	}

	public void create(final String iStorageMode) {
		configuration = new OStorageConfiguration(this);
		addCluster("index");

		open = true;
	}

	public void close() {
		// CLOSE ALL THE CLUSTERS
		for (OClusterMemory c : clusters)
			c.close();
		clusters.clear();

		// CLOSE THE DATA SEGMENT
		data.close();

		open = false;
	}

	public int addLogicalCluster(final OClusterLogical iClusterLogical) {
		iClusterLogical.setId(getLogicalClusterIndex(logicalClusters.size()));
		return registerLogicalCluster(iClusterLogical);
	}

	public int registerLogicalCluster(final OClusterLogical iClusterLogical) {
		logicalClusters.add(iClusterLogical);
		return iClusterLogical.getId();
	}

	public int addClusterSegment(final String iClusterName, final String iClusterFileName, final int iStartSize) {
		clusters.add(new OClusterMemory(clusters.size(), iClusterName));
		return clusters.size() - 1;
	}

	public int addDataSegment(final String iDataSegmentName) {
		// UNIQUE DATASEGMENT
		return 0;
	}

	public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
		return addDataSegment(iSegmentName);
	}

	@Override
	public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
		if (ppos.dataSegment > 0)
			return false;

		if (ppos.dataPosition >= data.size())
			return false;

		return true;
	}

	public long createRecord(final int iClusterId, final byte[] iContent, final byte iRecordType) {
		long offset = data.createRecord(iContent);
		OClusterMemory cluster = clusters.get(iClusterId);
		return cluster.addPhysicalPosition(0, offset, iRecordType);
	}

	public ORawBuffer readRecord(final int iRequesterId, final int iClusterId, final long iPosition) {
		OClusterMemory cluster = clusters.get(iClusterId);
		OPhysicalPosition ppos = cluster.getPhysicalPosition(iPosition, new OPhysicalPosition());

		return new ORawBuffer(data.readRecord(ppos.dataPosition), ppos.version, ppos.type);
	}

	public int updateRecord(final int iRequesterId, final int iClusterId, final long iPosition, final byte[] iContent,
			final int iVersion, final byte iRecordType) {
		OClusterMemory cluster = clusters.get(iClusterId);
		OPhysicalPosition ppos = cluster.getPhysicalPosition(iPosition, new OPhysicalPosition());

		// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
		if (iVersion > -1 && ppos.version != iVersion)
			throw new OConcurrentModificationException(
					"Can't update record #"
							+ ORecordId.generateString(iClusterId, iPosition)
							+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

		data.updateRecord(ppos.dataPosition, iContent);

		return ++ppos.version;
	}

	public void deleteRecord(final int iRequesterId, final int iClusterId, final long iPosition, final int iVersion) {
		OClusterMemory cluster = clusters.get(iClusterId);
		OPhysicalPosition ppos = cluster.getPhysicalPosition(iPosition, new OPhysicalPosition());

		// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
		if (iVersion > -1 && ppos.version != iVersion)
			throw new OConcurrentModificationException(
					"Can't update record #"
							+ ORecordId.generateString(iClusterId, iPosition)
							+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

		data.deleteRecord(ppos.dataPosition);
		cluster.removePhysicalPosition(iClusterId, null);
	}

	public long count(final int iClusterId) {
		OClusterMemory cluster = clusters.get(iClusterId);
		return cluster.getElements();
	}

	public long count(final int[] iClusterIds) {
		long tot = 0;
		for (int i = 0; i < iClusterIds.length; ++i)
			tot += clusters.get(iClusterIds[i]).getElements();
		return tot;
	}

	public OCluster getClusterByName(final String iClusterName) {
		for (int i = 0; i < clusters.size(); ++i)
			if (clusters.get(i).getName().equals(iClusterName))
				return clusters.get(i);
		return null;
	}

	public int getClusterIdByName(final String iClusterName) {
		for (int i = 0; i < clusters.size(); ++i)
			if (clusters.get(i).getName().equals(iClusterName))
				return clusters.get(i).getId();
		return -1;
	}

	public String getPhysicalClusterNameById(final int iClusterId) {
		for (int i = 0; i < clusters.size(); ++i)
			if (clusters.get(i).getId() == iClusterId)
				return clusters.get(i).getName();
		return null;
	}

	public Set<String> getClusterNames() {
		Set<String> result = new HashSet<String>();
		for (int i = 0; i < clusters.size(); ++i)
			result.add(clusters.get(i).getName());
		return result;
	}

	public long count(final String iClassName) {
		throw new UnsupportedOperationException("count");
	}

	public List<ORecordSchemaAware<?>> query(final int iRequesterId, final OQuery<?> iQuery, final int iLimit) {
		throw new UnsupportedOperationException("count");
	}

	public ORecordSchemaAware<?> queryFirst(final int iRequesterId, final OQuery<?> iQuery) {
		throw new UnsupportedOperationException("count");
	}

	public void commit(final int iRequesterId, final OTransaction<?> iTx) {
	}

	public void synch() {
	}

	@SuppressWarnings("unchecked")
	public ODictionary<?> createDictionary(final ODatabaseRecord<?> iDatabase) throws Exception {
		return new ODictionaryMemory();
	}

	public void browse(int iRequesterId, int[] iClusterId, ORecordBrowsingListener iListener, ORecord<?> iRecord) {
	}

	public OQueryExecutor getQueryExecutor(OQuery<?> iQuery) {
		if (iQuery instanceof OSQLAsynchQuery<?>)
			return OSQLAsynchQueryLocalExecutor.INSTANCE;

		else if (iQuery instanceof OSQLSynchQuery<?>)
			return OSQLAsynchQueryLocalExecutor.INSTANCE;

		throw new OConfigurationException("Query executor not configured for query type: " + iQuery.getClass());
	}

	public boolean exists() {
		return true;
	}
}
