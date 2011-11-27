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
import java.util.concurrent.Callable;

import com.orientechnologies.common.concur.resource.OSharedContainer;
import com.orientechnologies.orient.core.cache.OLevel2RecordCache;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.tx.OTransaction;

/**
 * This is the gateway interface between the Database side and the storage. Provided implementations are: Local, Remote and Memory.
 * 
 * @see OStorageLocal, OStorageMemory
 * @author Luca Garulli
 * 
 */
public interface OStorage extends OSharedContainer {
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

	public void open(String iUserName, String iUserPassword, final Map<String, Object> iProperties);

	public void create(Map<String, Object> iProperties);

	public boolean exists();

	public void reload();

	public void delete();

	public void close();

	public void close(boolean iForce);

	public boolean isClosed();

	/**
	 * Returns the level1 cache. Cannot be null.
	 * 
	 * @return Current cache.
	 */
	public OLevel2RecordCache getLevel2Cache();

	// CRUD OPERATIONS
	public long createRecord(ORecordId iRecordId, byte[] iContent, byte iRecordType, ORecordCallback<Long> iCallback);

	public ORawBuffer readRecord(ODatabaseRecord iDatabase, ORecordId iRid, String iFetchPlan, ORecordCallback<ORawBuffer> iCallback);

	public int updateRecord(ORecordId iRecordId, byte[] iContent, int iVersion, byte iRecordType, ORecordCallback<Integer> iCallback);

	public boolean deleteRecord(ORecordId iRecordId, int iVersion, ORecordCallback<Boolean> iCallback);

	// TX OPERATIONS
	public void commit(OTransaction iTx);

	// TX OPERATIONS
	public void rollback(OTransaction iTx);

	// MISC
	public OStorageConfiguration getConfiguration();

	public int getClusters();

	public Set<String> getClusterNames();

	public OCluster getClusterById(int iId);

	public Collection<? extends OCluster> getClusterInstances();

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

	public boolean dropCluster(String iClusterName);

	/**
	 * Drops a cluster.
	 * 
	 * @param iId
	 * @return true if has been removed, otherwise false
	 */
	public boolean dropCluster(int iId);

	/**
	 * Add a new data segment in the default segment directory and with filename equals to the cluster name.
	 */
	public int addDataSegment(String iDataSegmentName);

	public int addDataSegment(String iSegmentName, String iSegmentFileName);

	public long count(int iClusterId);

	public long count(int[] iClusterIds);

	/**
	 * Returns the size of the database.
	 */
	public long getSize();

	/**
	 * Returns the total number of records.
	 */
	public long countRecords();

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

	public void renameCluster(String iOldName, String iNewName);

	public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock);
}
