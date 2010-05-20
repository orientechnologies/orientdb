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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
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
	private final ODataSegmentMemory		data							= new ODataSegmentMemory();
	private final List<OClusterMemory>	clusters					= new ArrayList<OClusterMemory>();
	private int													defaultClusterId	= 0;

	public OStorageMemory(final String iURL) {
		super(iURL, iURL, "rw");
	}

	public void create() {
		open(-1, "", "");
	}

	public void open(final int iRequesterId, final String iUserName, final String iUserPassword) {
		addUser();
		configuration = new OStorageConfiguration(this);

		addDataSegment(OStorage.CLUSTER_DEFAULT_NAME);

		// ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
		addCluster(OStorage.CLUSTER_METADATA_NAME, null);

		// ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF INDEXING
		addCluster("index", null);

		// ADD THE DEFAULT CLUSTER
		defaultClusterId = addCluster(OStorage.CLUSTER_DEFAULT_NAME, null);

		try {
			configuration = new OStorageConfiguration(this);
			configuration.create();
		} catch (IOException e) {
		}

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

	public int addCluster(final String iClusterName, final String iClusterType, final Object... iParameters) {
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
		OCluster cluster = getClusterById(iClusterId);
		try {
			return cluster.addPhysicalPosition(0, offset, iRecordType);
		} catch (IOException e) {
			throw new OStorageException("Error on create record in cluster: " + iClusterId, e);
		}
	}

	public ORawBuffer readRecord(final int iRequesterId, final int iClusterId, final long iPosition) {
		OCluster cluster = getClusterById(iClusterId);
		try {
			OPhysicalPosition ppos = cluster.getPhysicalPosition(iPosition, new OPhysicalPosition());

			return new ORawBuffer(data.readRecord(ppos.dataPosition), ppos.version, ppos.type);
		} catch (IOException e) {
			throw new OStorageException("Error on read record in cluster: " + iClusterId, e);
		}
	}

	public int updateRecord(final int iRequesterId, final int iClusterId, final long iPosition, final byte[] iContent,
			final int iVersion, final byte iRecordType) {
		OCluster cluster = getClusterById(iClusterId);
		try {
			OPhysicalPosition ppos = cluster.getPhysicalPosition(iPosition, new OPhysicalPosition());
			if (ppos == null)
				return -1;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't update record #"
								+ ORecordId.generateString(iClusterId, iPosition)
								+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

			data.updateRecord(ppos.dataPosition, iContent);

			return ++ppos.version;

		} catch (IOException e) {
			throw new OStorageException("Error on update record in cluster: " + iClusterId, e);
		}
	}

	public boolean deleteRecord(final int iRequesterId, final int iClusterId, final long iPosition, final int iVersion) {
		OCluster cluster = getClusterById(iClusterId);

		try {
			OPhysicalPosition ppos = cluster.getPhysicalPosition(iPosition, new OPhysicalPosition());

			if (ppos == null)
				return false;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't update record #"
								+ ORecordId.generateString(iClusterId, iPosition)
								+ " because it was modified by another user in the meanwhile of current transaction. Use pessimistic locking instead of optimistic or simply re-execute the transaction");

			cluster.removePhysicalPosition(iPosition, null);
			data.deleteRecord(ppos.dataPosition);

			return true;

		} catch (IOException e) {
			throw new OStorageException("Error on delete record in cluster: " + iClusterId, e);
		}
	}

	public long count(final int iClusterId) {
		OCluster cluster = getClusterById(iClusterId);
		try {
			return cluster.getElements();
		} catch (IOException e) {
			throw new OStorageException("Error on count record in cluster: " + iClusterId, e);
		}
	}

	public long count(final int[] iClusterIds) {
		long tot = 0;
		for (int i = 0; i < iClusterIds.length; ++i)
			tot += clusters.get(iClusterIds[i]).getElements();
		return tot;
	}

	public OCluster getClusterByName(final String iClusterName) {
		for (int i = 0; i < clusters.size(); ++i)
			if (getClusterById(i).getName().equals(iClusterName))
				return getClusterById(i);
		return null;
	}

	public int getClusterIdByName(final String iClusterName) {
		for (int i = 0; i < clusters.size(); ++i)
			if (getClusterById(i).getName().equals(iClusterName))
				return getClusterById(i).getId();
		return -1;
	}

	public String getPhysicalClusterNameById(final int iClusterId) {
		for (int i = 0; i < clusters.size(); ++i)
			if (getClusterById(i).getId() == iClusterId)
				return getClusterById(i).getName();
		return null;
	}

	public Set<String> getClusterNames() {
		Set<String> result = new HashSet<String>();
		for (int i = 0; i < clusters.size(); ++i)
			result.add(getClusterById(i).getName());
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
		return new ODictionaryMemory(iDatabase);
	}

	public void browse(int iRequesterId, int[] iClusterId, ORecordBrowsingListener iListener, ORecord<?> iRecord) {
	}

	public boolean exists() {
		return true;
	}

	public OCluster getClusterById(final int iClusterId) {
		return clusters.get(iClusterId);
	}

	public Collection<? extends OCluster> getClusters() {
		return Collections.unmodifiableCollection(clusters);
	}

	public int getDefaultClusterId() {
		return defaultClusterId;
	}

	public Object command(OCommandRequestInternal iCommand) {
		return null;
	}
}
