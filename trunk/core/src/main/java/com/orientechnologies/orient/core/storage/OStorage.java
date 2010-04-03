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
package com.orientechnologies.orient.core.storage;

import java.io.IOException;
import java.util.Set;

import com.orientechnologies.orient.core.cache.OCacheRecord;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.query.OQueryExecutor;
import com.orientechnologies.orient.core.storage.impl.logical.OClusterLogical;
import com.orientechnologies.orient.core.tx.OTransaction;

public interface OStorage {
	public static final String	DEFAULT_SEGMENT	= "default";

	public void open(int iRequesterId, String iUserName, String iUserPassword);

	public void create(String iStorageMode);

	public boolean exists();

	public void close();

	public void close(boolean iForce);

	public boolean isClosed();

	// CRUD OPERATIONS
	public long createRecord(int iClusterId, byte[] iContent, final byte iRecordType);

	public ORawBuffer readRecord(int iRequesterId, ORID iRecordId);

	public ORawBuffer readRecord(int iRequesterId, int iClusterId, long iPosition);

	public int updateRecord(int iRequesterId, int iClusterId, long iPosition, byte[] iContent, final int iVersion,
			final byte iRecordType);

	public int updateRecord(int iRequesterId, ORID iRecordId, byte[] iContent, final int iVersion, final byte iRecordType);

	public void deleteRecord(int iRequesterId, ORID iRecordId, final int iVersion);

	public void deleteRecord(int iRequesterId, int iClusterId, long iPosition, final int iVersion);

	// TX OPERATIONS
	public void commit(int iRequesterId, OTransaction<?> iTx);

	public OStorageConfiguration getConfiguration();

	public Set<String> getClusterNames();

	/**
	 * Add a new cluster in the default segment directory and with filename equals to the cluster name.
	 */
	public int addCluster(String iClusterName);

	public int registerLogicalCluster(OClusterLogical iClusterLogical);

	public int addLogicalCluster(OClusterLogical iClusterLogical);

	/**
	 * Add a new cluster into the storage.
	 * 
	 * @throws IOException
	 */
	public int addClusterSegment(String iClusterName, String iClusterFileName, int iStartSize);

	/**
	 * Add a new data segment in the default segment directory and with filename equals to the cluster name.
	 */
	public int addDataSegment(String iDataSegmentName);

	public int addDataSegment(String iSegmentName, String iSegmentFileName);

	public long count(int iClusterId);

	public long count(int[] iClusterIds);

	public int getClusterIdByName(String iClusterName);

	public String getPhysicalClusterNameById(int iClusterId);

	public boolean checkForRecordValidity(OPhysicalPosition ppos);

	public String getName();

	public void synch();

	public int getUsers();

	public int addUser();

	public int removeUser();

	public ODictionary<?> createDictionary(ODatabaseRecord<?> iDatabase) throws Exception;

	public OCacheRecord getCache();

	public OQueryExecutor getQueryExecutor(OQuery<?> iQuery);
}
