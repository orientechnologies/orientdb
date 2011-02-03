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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.cache.OCacheRecord;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.tx.OTransaction;

/**
 * This is the gateway interface between the Database side and the storage. Provided implementations are: Local, Remote and Memory.
 * 
 * @see OStorageLocal, OStorageMemory
 * @author Luca Garulli
 * 
 */
public interface OStorage {
	public static final String	CLUSTER_INTERNAL_NAME	= "internal";
	public static final String	CLUSTER_INDEX_NAME		= "index";
	public static final String	CLUSTER_DEFAULT_NAME	= "default";
	public static final String	DATA_DEFAULT_NAME			= "default";

	public enum CLUSTER_TYPE {
		PHYSICAL, LOGICAL, MEMORY
	}

	public enum SIZE {
		TINY, MEDIUM, LARGE, HUGE
	}

	public void open(int iRequesterId, String iUserName, String iUserPassword, final Map<String, Object> iProperties);

	public void create(Map<String, Object> iProperties);

	public void delete();

	public boolean exists();

	public void close();

	public void close(boolean iForce);

	public boolean isClosed();

	// CRUD OPERATIONS
	public long createRecord(int iClusterId, byte[] iContent, final byte iRecordType);

	public ORawBuffer readRecord(ODatabaseRecord iDatabase, int iRequesterId, int iClusterId, long iPosition, String iFetchPlan);

	public int updateRecord(int iRequesterId, int iClusterId, long iPosition, byte[] iContent, final int iVersion,
			final byte iRecordType);

	public int updateRecord(int iRequesterId, ORID iRecordId, byte[] iContent, final int iVersion, final byte iRecordType);

	public boolean deleteRecord(int iRequesterId, ORID iRecordId, final int iVersion);

	public boolean deleteRecord(int iRequesterId, int iClusterId, long iPosition, final int iVersion);

	// TX OPERATIONS
	public void commit(int iRequesterId, OTransaction iTx);

	public OStorageConfiguration getConfiguration();

	public Set<String> getClusterNames();

	public OCluster getClusterById(int iId);

	public Collection<? extends OCluster> getClusters();

	/**
	 * Add a new cluster into the storage.
	 * 
	 * @param iClusterName
	 *          name of the cluster
	 * @param iClusterType
	 *          Cluster type. Type depends by the implementation.
	 * @param iParameters
	 *          Additional parameters to configure the cluster
	 * @throws IOException
	 */
	public int addCluster(String iClusterName, OStorage.CLUSTER_TYPE iClusterType, Object... iParameters);

	public boolean removeCluster(String iClusterName);

	public boolean removeCluster(int iId);

	/**
	 * Add a new data segment in the default segment directory and with filename equals to the cluster name.
	 */
	public int addDataSegment(String iDataSegmentName);

	public int addDataSegment(String iSegmentName, String iSegmentFileName);

	public long count(int iClusterId);

	public long count(int[] iClusterIds);

	public int getDefaultClusterId();

	public int getClusterIdByName(String iClusterName);

	public String getClusterTypeByName(String iClusterName);

	public String getPhysicalClusterNameById(int iClusterId);

	public boolean checkForRecordValidity(OPhysicalPosition ppos);

	public String getName();

	public String getURL();

	public long getVersion();

	public void synch();

	public int getUsers();

	public int addUser();

	public int removeUser();

	public ODictionary<?> createDictionary(ODatabaseRecord iDatabase) throws Exception;

	/**
	 * Return the configured local Level-2 cache component. Cache component is always created even if not used.
	 * 
	 * @return
	 */
	public OCacheRecord getCache();

	/**
	 * Execute the command request and return the result back.
	 */
	public Object command(OCommandRequestText iCommand);

	/**
	 * Returns a pair of long values telling the begin and end positions of data in the requested cluster. Useful to know the range of
	 * the records.
	 * 
	 * @param iCurrentClusterId
	 *          Cluster id
	 */
	public long[] getClusterDataRange(int currentClusterId);
}
