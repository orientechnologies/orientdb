/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.orientechnologies.common.concur.lock.OLockManager.LOCK;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ODataSegment;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTxListener;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

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
public class OStorageMemory extends OStorageEmbedded {
  private final List<ODataSegmentMemory>    dataSegments      = new ArrayList<ODataSegmentMemory>();
  private final List<OClusterMemory>        clusters          = new ArrayList<OClusterMemory>();
  private final Map<String, OClusterMemory> clusterMap        = new HashMap<String, OClusterMemory>();
  private int                               defaultClusterId  = 0;
  private long                              positionGenerator = 0;

  public OStorageMemory(final String iURL) {
    super(iURL, iURL, "rw");
    configuration = new OStorageConfiguration(this);
  }

  public void create(final Map<String, Object> iOptions) {
    addUser();

    lock.acquireExclusiveLock();
    try {

      addDataSegment(OStorage.DATA_DEFAULT_NAME);
      addDataSegment(OMetadataDefault.DATASEGMENT_INDEX_NAME);

      // ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
      addCluster(CLUSTER_TYPE.PHYSICAL.toString(), OMetadataDefault.CLUSTER_INTERNAL_NAME, null, null, true);

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF INDEXING IN THE INDEX DATA SEGMENT
      addCluster(CLUSTER_TYPE.PHYSICAL.toString(), OMetadataDefault.CLUSTER_INDEX_NAME, null,
          OMetadataDefault.DATASEGMENT_INDEX_NAME, true);

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF INDEXING
      addCluster(CLUSTER_TYPE.PHYSICAL.toString(), OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null, null, true);

      // ADD THE DEFAULT CLUSTER
      defaultClusterId = addCluster(CLUSTER_TYPE.PHYSICAL.toString(), CLUSTER_DEFAULT_NAME, null, null, false);

      configuration.create();

      status = STATUS.OPEN;

    } catch (OStorageException e) {
      close();
      throw e;

    } catch (IOException e) {
      close();
      throw new OStorageException("Error on creation of storage: " + name, e);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iOptions) {
    addUser();

    if (status == STATUS.OPEN)
      // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
      // REUSED
      return;

    lock.acquireExclusiveLock();
    try {

      if (!exists())
        throw new OStorageException("Cannot open the storage '" + name + "' because it does not exist in path: " + url);

      status = STATUS.OPEN;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void close(final boolean iForce) {
    lock.acquireExclusiveLock();
    try {

      if (!checkForClose(iForce))
        return;

      status = STATUS.CLOSING;

      // CLOSE ALL THE CLUSTERS
      for (OClusterMemory c : clusters)
        if (c != null)
          c.close();
      clusters.clear();
      clusterMap.clear();

      // CLOSE THE DATA SEGMENTS
      for (ODataSegmentMemory d : dataSegments)
        if (d != null)
          d.close();
      dataSegments.clear();

      level2Cache.shutdown();

      super.close(iForce);

      Orient.instance().unregisterStorage(this);
      status = STATUS.CLOSED;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void delete() {
    close(true);
  }

  @Override
  public void backup(OutputStream out, Map<String, Object> options, Callable<Object> callable) throws IOException {
    throw new UnsupportedOperationException("backup");
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable) throws IOException {
    throw new UnsupportedOperationException("restore");
  }

  public void reload() {
  }

  public int addCluster(final String iClusterType, String iClusterName, final String iLocation, final String iDataSegmentName,
      boolean forceListBased, final Object... iParameters) {
    iClusterName = iClusterName.toLowerCase();
    lock.acquireExclusiveLock();
    try {
      int clusterId = clusters.size();
      for (int i = 0; i < clusters.size(); ++i) {
        if (clusters.get(i) == null) {
          clusterId = i;
          break;
        }
      }

      final OClusterMemory cluster = (OClusterMemory) Orient.instance().getClusterFactory().createCluster(OClusterMemory.TYPE);
      cluster.configure(this, clusterId, iClusterName, iLocation, getDataSegmentIdByName(iDataSegmentName), iParameters);

      if (clusterId == clusters.size())
        // APPEND IT
        clusters.add(cluster);
      else
        // RECYCLE THE FREE POSITION
        clusters.set(clusterId, cluster);
      clusterMap.put(iClusterName, cluster);

      return clusterId;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public int addCluster(String iClusterType, String iClusterName, int iRequestedId, String iLocation, String iDataSegmentName,
      boolean forceListBased, Object... iParameters) {
    throw new UnsupportedOperationException("This operation is unsupported for " + getType()
        + " storage. If you are doing import please use parameter -preserveClusterIDs=false .");
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    lock.acquireExclusiveLock();
    try {

      final OCluster c = clusters.get(iClusterId);
      if (c != null) {
        if (iTruncate)
          c.truncate();
        c.delete();
        clusters.set(iClusterId, null);
        getLevel2Cache().freeCluster(iClusterId);
        clusterMap.remove(c.getName());
      }

    } catch (IOException e) {
    } finally {

      lock.releaseExclusiveLock();
    }

    return false;
  }

  public boolean dropDataSegment(final String iName) {
    lock.acquireExclusiveLock();
    try {

      final int id = getDataSegmentIdByName(iName);
      final ODataSegment data = dataSegments.get(id);
      if (data == null)
        return false;

      data.drop();

      dataSegments.set(id, null);

      // UPDATE CONFIGURATION
      configuration.dropCluster(id);

      return true;
    } catch (Exception e) {
      OLogManager.instance().exception("Error while removing data segment '" + iName + '\'', e, OStorageException.class);

    } finally {
      lock.releaseExclusiveLock();
    }

    return false;
  }

  public int addDataSegment(final String iDataSegmentName) {
    lock.acquireExclusiveLock();
    try {
      int pos = -1;
      for (int i = 0; i < dataSegments.size(); ++i) {
        if (dataSegments.get(i) == null) {
          pos = i;
          break;
        }
      }

      if (pos == -1)
        pos = dataSegments.size();

      final ODataSegmentMemory dataSegment = new ODataSegmentMemory(iDataSegmentName, pos);

      if (pos == dataSegments.size())
        dataSegments.add(dataSegment);
      else
        dataSegments.set(pos, dataSegment);

      return pos;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public int addDataSegment(final String iSegmentName, final String iLocation) {
    return addDataSegment(iSegmentName);
  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(final int iDataSegmentId, final ORecordId iRid,
      final byte[] iContent, ORecordVersion iRecordVersion, final byte iRecordType, final int iMode,
      ORecordCallback<OClusterPosition> iCallback) {
    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireSharedLock();
    try {
      final ODataSegmentMemory data = getDataSegmentById(iDataSegmentId);

      final long offset = data.createRecord(iContent);
      final OCluster cluster = getClusterById(iRid.clusterId);

      // ASSIGN THE POSITION IN THE CLUSTER
      final OPhysicalPosition ppos = new OPhysicalPosition(iDataSegmentId, offset, iRecordType);
      if (cluster.isHashBased()) {
        if (iRid.isNew()) {
          if (OGlobalConfiguration.USE_NODE_ID_CLUSTER_POSITION.getValueAsBoolean()) {
            ppos.clusterPosition = OClusterPositionFactory.INSTANCE.generateUniqueClusterPosition();
          } else {
            ppos.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(positionGenerator++);
          }
        } else {
          ppos.clusterPosition = iRid.clusterPosition;
        }
      }

      if (!cluster.addPhysicalPosition(ppos)) {
        data.readRecord(ppos.dataSegmentPos);
        throw new OStorageException("Record with given id " + iRid + " has already exists.");
      }

      iRid.clusterPosition = ppos.clusterPosition;

      if (iCallback != null)
        iCallback.call(iRid, iRid.clusterPosition);

      if (iRecordVersion.getCounter() > 0 && iRecordVersion.compareTo(ppos.recordVersion) != 0) {
        // OVERWRITE THE VERSION
        cluster.updateVersion(iRid.clusterPosition, iRecordVersion);
        ppos.recordVersion = iRecordVersion;
      }

      return new OStorageOperationResult<OPhysicalPosition>(ppos);
    } catch (IOException e) {
      throw new OStorageException("Error on create record in cluster: " + iRid.clusterId, e);

    } finally {
      lock.releaseSharedLock();
      Orient.instance().getProfiler()
          .stopChrono(PROFILER_CREATE_RECORD, "Create a record in database", timer, "db.*.data.updateHole");
    }
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, String iFetchPlan, boolean iIgnoreCache,
      ORecordCallback<ORawBuffer> iCallback, boolean loadTombstones) {
    return new OStorageOperationResult<ORawBuffer>(readRecord(getClusterById(iRid.clusterId), iRid, true, loadTombstones));
  }

  @Override
  protected ORawBuffer readRecord(final OCluster iClusterSegment, final ORecordId iRid, final boolean iAtomicLock,
      boolean loadTombstones) {
    final long timer = Orient.instance().getProfiler().startChrono();

    lock.acquireSharedLock();
    try {
      lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.SHARED);
      try {
        final OClusterPosition lastPos = iClusterSegment.getLastPosition();

        if (!iClusterSegment.isHashBased()) {
          if (iRid.clusterPosition.compareTo(lastPos) > 0)
            return null;
        }

        final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(new OPhysicalPosition(iRid.clusterPosition));
        if (ppos != null && loadTombstones && ppos.recordVersion.isTombstone())
          return new ORawBuffer(null, ppos.recordVersion, ppos.recordType);

        if (ppos == null || ppos.recordVersion.isTombstone())
          return null;

        final ODataSegmentMemory dataSegment = getDataSegmentById(ppos.dataSegmentId);

        return new ORawBuffer(dataSegment.readRecord(ppos.dataSegmentPos), ppos.recordVersion, ppos.recordType);

      } finally {
        lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.SHARED);
      }
    } catch (IOException e) {
      throw new OStorageException("Error on read record in cluster: " + iClusterSegment.getId(), e);

    } finally {
      lock.releaseSharedLock();

      Orient.instance().getProfiler().stopChrono(PROFILER_READ_RECORD, "Read a record from database", timer, "db.*.readRecord");
    }
  }

  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId iRid, final byte[] iContent,
      final ORecordVersion iVersion, final byte iRecordType, final int iMode, ORecordCallback<ORecordVersion> iCallback) {
    final long timer = Orient.instance().getProfiler().startChrono();

    final OCluster cluster = getClusterById(iRid.clusterId);

    lock.acquireSharedLock();
    try {
      lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
      try {

        final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(iRid.clusterPosition));
        if (ppos == null || ppos.recordVersion.isTombstone()) {
          final ORecordVersion v = OVersionFactory.instance().createUntrackedVersion();
          if (iCallback != null) {
            iCallback.call(iRid, v);
          }
          return new OStorageOperationResult<ORecordVersion>(v);
        }

        // VERSION CONTROL CHECK
        switch (iVersion.getCounter()) {
        // DOCUMENT UPDATE, NO VERSION CONTROL
        case -1:
          ppos.recordVersion.increment();
          cluster.updateVersion(iRid.clusterPosition, ppos.recordVersion);
          break;

        // DOCUMENT UPDATE, NO VERSION CONTROL, NO VERSION UPDATE
        case -2:
          break;

        default:
          // MVCC CONTROL AND RECORD UPDATE OR WRONG VERSION VALUE
          if (iVersion.getCounter() > -1) {
            // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
            if (!iVersion.equals(ppos.recordVersion))
              if (OFastConcurrentModificationException.enabled())
                throw OFastConcurrentModificationException.instance();
              else
                throw new OConcurrentModificationException(iRid, ppos.recordVersion, iVersion, ORecordOperation.UPDATED);
            ppos.recordVersion.increment();
            cluster.updateVersion(iRid.clusterPosition, ppos.recordVersion);
          } else {
            // DOCUMENT ROLLBACKED
            iVersion.clearRollbackMode();
            ppos.recordVersion.copyFrom(iVersion);
            cluster.updateVersion(iRid.clusterPosition, ppos.recordVersion);
          }
        }

        if (ppos.recordType != iRecordType)
          cluster.updateRecordType(iRid.clusterPosition, iRecordType);

        final ODataSegmentMemory dataSegment = getDataSegmentById(ppos.dataSegmentId);
        dataSegment.updateRecord(ppos.dataSegmentPos, iContent);

        if (iCallback != null)
          iCallback.call(null, ppos.recordVersion);

        return new OStorageOperationResult<ORecordVersion>(ppos.recordVersion);

      } finally {
        lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
      }
    } catch (IOException e) {
      throw new OStorageException("Error on update record " + iRid, e);

    } finally {
      lock.releaseSharedLock();

      Orient.instance().getProfiler().stopChrono(PROFILER_UPDATE_RECORD, "Update a record to database", timer, "db.*.updateRecord");
    }
  }

  @Override
  public boolean updateReplica(int dataSegmentId, ORecordId rid, byte[] content, ORecordVersion recordVersion, byte recordType)
      throws IOException {
    if (rid.isNew())
      throw new OStorageException("Passed record with id " + rid + " is new and can not be treated as replica.");

    checkOpeness();

    final OCluster cluster = getClusterById(rid.clusterId);
    final ODataSegmentMemory data = getDataSegmentById(dataSegmentId);

    lock.acquireSharedLock();
    try {
      lockManager.acquireLock(Thread.currentThread(), rid, LOCK.EXCLUSIVE);
      try {
        OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(rid.clusterPosition));
        if (ppos == null) {
          if (!cluster.isHashBased())
            throw new OStorageException("Cluster with LH support is required.");

          ppos = new OPhysicalPosition(rid.clusterPosition, recordVersion);

          ppos.recordType = recordType;
          ppos.dataSegmentId = data.getId();

          if (!recordVersion.isTombstone()) {
            ppos.dataSegmentPos = data.createRecord(content);
          }

          cluster.addPhysicalPosition(ppos);

          return true;
        } else {
          if (ppos.recordType != recordType)
            throw new OStorageException("Record types of provided and stored replicas are different " + recordType + ":"
                + ppos.recordType + ".");

          if (ppos.recordVersion.compareTo(recordVersion) < 0) {
            if (!recordVersion.isTombstone() && !ppos.recordVersion.isTombstone()) {
              data.updateRecord(ppos.dataSegmentPos, content);
            } else if (recordVersion.isTombstone() && !ppos.recordVersion.isTombstone()) {
              data.deleteRecord(ppos.dataSegmentPos);
            } else if (!recordVersion.isTombstone() && ppos.recordVersion.isTombstone()) {
              ppos.dataSegmentPos = data.createRecord(content);
              cluster.updateDataSegmentPosition(ppos.clusterPosition, dataSegmentId, ppos.dataSegmentPos);
            }

            cluster.updateVersion(ppos.clusterPosition, recordVersion);
            return true;
          }
        }

      } finally {
        lockManager.releaseLock(Thread.currentThread(), rid, LOCK.EXCLUSIVE);
      }
    } finally {
      lock.releaseSharedLock();
    }

    return false;
  }

  @Override
  public <V> V callInRecordLock(Callable<V> callable, ORID rid, boolean exclusiveLock) {
    lock.acquireSharedLock();

    try {
      lockManager.acquireLock(Thread.currentThread(), rid, exclusiveLock ? LOCK.EXCLUSIVE : LOCK.SHARED);
      try {
        return callable.call();
      } finally {
        lockManager.releaseLock(Thread.currentThread(), rid, exclusiveLock ? LOCK.EXCLUSIVE : LOCK.SHARED);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new OException("Error on nested call in lock", e);
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRid, final ORecordVersion iVersion, final int iMode,
      ORecordCallback<Boolean> iCallback) {
    return new OStorageOperationResult<Boolean>(deleteRecord(iRid, iVersion,
        OGlobalConfiguration.STORAGE_USE_TOMBSTONES.getValueAsBoolean(), iCallback));
  }

  @Override
  public boolean cleanOutRecord(ORecordId recordId, ORecordVersion recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    return deleteRecord(recordId, recordVersion, false, callback);
  }

  private boolean deleteRecord(ORecordId iRid, ORecordVersion iVersion, boolean useTombstones, ORecordCallback<Boolean> iCallback) {
    final long timer = Orient.instance().getProfiler().startChrono();

    final OCluster cluster = getClusterById(iRid.clusterId);

    lock.acquireSharedLock();
    try {
      lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
      try {

        final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(iRid.clusterPosition));

        if (ppos == null || (ppos.recordVersion.isTombstone() && useTombstones)) {
          if (iCallback != null)
            iCallback.call(iRid, false);
          return false;
        }

        // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
        if (iVersion.getCounter() > -1 && !ppos.recordVersion.equals(iVersion))
          if (OFastConcurrentModificationException.enabled())
            throw OFastConcurrentModificationException.instance();
          else
            throw new OConcurrentModificationException(iRid, ppos.recordVersion, iVersion, ORecordOperation.DELETED);

        if (!ppos.recordVersion.isTombstone()) {
          final ODataSegmentMemory dataSegment = getDataSegmentById(ppos.dataSegmentId);
          dataSegment.deleteRecord(ppos.dataSegmentPos);
          ppos.dataSegmentPos = -1;
        }

        if (useTombstones && cluster.hasTombstonesSupport())
          cluster.convertToTombstone(iRid.clusterPosition);
        else
          cluster.removePhysicalPosition(iRid.clusterPosition);

        if (iCallback != null)
          iCallback.call(null, true);

        return true;

      } finally {
        lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
      }
    } catch (IOException e) {
      throw new OStorageException("Error on delete record " + iRid, e);

    } finally {
      lock.releaseSharedLock();

      Orient.instance().getProfiler()
          .stopChrono(PROFILER_DELETE_RECORD, "Delete a record from database", timer, "db.*.deleteRecord");
    }
  }

  public long count(final int iClusterId) {
    return count(iClusterId, false);
  }

  @Override
  public long count(int iClusterId, boolean countTombstones) {
    final OCluster cluster = getClusterById(iClusterId);

    lock.acquireSharedLock();
    try {
      return cluster.getEntries() - (countTombstones ? 0L : cluster.getTombstonesCount());
    } finally {
      lock.releaseSharedLock();
    }
  }

  public OClusterPosition[] getClusterDataRange(final int iClusterId) {
    final OCluster cluster = getClusterById(iClusterId);
    lock.acquireSharedLock();
    try {

      return new OClusterPosition[] { cluster.getFirstPosition(), cluster.getLastPosition() };

    } catch (IOException ioe) {
      throw new OStorageException("Can not retrieve information about data range", ioe);
    } finally {
      lock.releaseSharedLock();
    }
  }

  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    lock.acquireSharedLock();
    try {

      long tot = 0;
      for (int iClusterId : iClusterIds) {
        if (iClusterId > -1) {
          final OCluster cluster = clusters.get(iClusterId);

          if (cluster != null)
            tot += cluster.getEntries() - (countTombstones ? 0L : cluster.getTombstonesCount());
        }
      }
      return tot;

    } finally {
      lock.releaseSharedLock();
    }
  }

  public OCluster getClusterByName(final String iClusterName) {
    lock.acquireSharedLock();
    try {

      return clusterMap.get(iClusterName.toLowerCase());

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getClusterIdByName(String iClusterName) {
    iClusterName = iClusterName.toLowerCase();

    lock.acquireSharedLock();
    try {

      final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());
      if (cluster == null)
        return -1;
      return cluster.getId();

    } finally {
      lock.releaseSharedLock();
    }
  }

  public String getClusterTypeByName(final String iClusterName) {
    return OClusterMemory.TYPE;
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    lock.acquireSharedLock();
    try {

      for (OClusterMemory cluster : clusters) {
        if (cluster != null && cluster.getId() == iClusterId)
          return cluster.getName();
      }
      return null;

    } finally {
      lock.releaseSharedLock();
    }
  }

  public Set<String> getClusterNames() {
    lock.acquireSharedLock();
    try {

      return new HashSet<String>(clusterMap.keySet());

    } finally {
      lock.releaseSharedLock();
    }
  }

  public void commit(final OTransaction iTx, Runnable callback) {
    lock.acquireExclusiveLock();
    try {

      final List<ORecordOperation> tmpEntries = new ArrayList<ORecordOperation>();

      while (iTx.getCurrentRecordEntries().iterator().hasNext()) {
        for (ORecordOperation txEntry : iTx.getCurrentRecordEntries())
          tmpEntries.add(txEntry);

        iTx.clearRecordEntries();

        for (ORecordOperation txEntry : tmpEntries)
          // COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
          commitEntry(iTx, txEntry);

        tmpEntries.clear();
      }

      // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT
      OTransactionAbstract.updateCacheFromEntries(iTx, iTx.getAllRecordEntries(), true);
    } catch (IOException e) {
      rollback(iTx);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void rollback(final OTransaction iTx) {
  }

  public void synch() {
  }

  public boolean exists() {
    lock.acquireSharedLock();
    try {

      return !clusters.isEmpty();

    } finally {
      lock.releaseSharedLock();
    }
  }

  public ODataSegmentMemory getDataSegmentById(int iDataId) {
    lock.acquireSharedLock();
    try {

      if (iDataId < 0 || iDataId > dataSegments.size() - 1)
        throw new IllegalArgumentException("Invalid data segment id " + iDataId + ". Range is 0-" + (dataSegments.size() - 1));

      return dataSegments.get(iDataId);

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getDataSegmentIdByName(final String iDataSegmentName) {
    if (iDataSegmentName == null)
      return 0;

    lock.acquireSharedLock();
    try {

      for (ODataSegmentMemory d : dataSegments)
        if (d != null && d.getName().equalsIgnoreCase(iDataSegmentName))
          return d.getId();

      throw new IllegalArgumentException("Data segment '" + iDataSegmentName + "' does not exist in storage '" + name + "'");

    } finally {
      lock.releaseSharedLock();
    }
  }

  public OCluster getClusterById(int iClusterId) {
    lock.acquireSharedLock();
    try {

      if (iClusterId == ORID.CLUSTER_ID_INVALID)
        // GET THE DEFAULT CLUSTER
        iClusterId = defaultClusterId;

      checkClusterSegmentIndexRange(iClusterId);

      return clusters.get(iClusterId);

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getClusters() {
    lock.acquireSharedLock();
    try {

      return clusterMap.size();

    } finally {
      lock.releaseSharedLock();
    }
  }

  public Collection<? extends OCluster> getClusterInstances() {
    lock.acquireSharedLock();
    try {

      return Collections.unmodifiableCollection(clusters);

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public long getSize() {
    long size = 0;

    lock.acquireSharedLock();
    try {
      for (ODataSegmentMemory d : dataSegments)
        if (d != null)
          size += d.getSize();

    } finally {
      lock.releaseSharedLock();
    }
    return size;
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    if (ppos.dataSegmentId > 0)
      return false;

    lock.acquireSharedLock();
    try {
      final ODataSegmentMemory dataSegment = getDataSegmentById(ppos.dataSegmentId);
      if (ppos.dataSegmentPos >= dataSegment.count())
        return false;

    } finally {
      lock.releaseSharedLock();
    }
    return true;
  }

  private void commitEntry(final OTransaction iTx, final ORecordOperation txEntry) throws IOException {

    final ORecordId rid = (ORecordId) txEntry.getRecord().getIdentity();

    final OCluster cluster = getClusterById(rid.clusterId);
    rid.clusterId = cluster.getId();

    if (txEntry.getRecord() instanceof OTxListener)
      ((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.BEFORE_COMMIT);

    switch (txEntry.type) {
    case ORecordOperation.LOADED:
      break;

    case ORecordOperation.CREATED:
      if (rid.isNew()) {
        // CHECK 2 TIMES TO ASSURE THAT IT'S A CREATE OR AN UPDATE BASED ON RECURSIVE TO-STREAM METHOD
        final byte[] stream = txEntry.getRecord().toStream();

        if (stream == null) {
          OLogManager.instance().warn(this, "Null serialization on committing new record %s in transaction", rid);
          break;
        }

        if (rid.isNew()) {
          final ORecordId oldRID = rid.copy();

          final OPhysicalPosition ppos = createRecord(txEntry.dataSegmentId, rid, stream,
              OVersionFactory.instance().createVersion(), txEntry.getRecord().getRecordType(), 0, null).getResult();

          txEntry.getRecord().getRecordVersion().copyFrom(ppos.recordVersion);

          iTx.updateIdentityAfterCommit(oldRID, rid);
        } else {
          txEntry
              .getRecord()
              .getRecordVersion()
              .copyFrom(
                  updateRecord(rid, stream, txEntry.getRecord().getRecordVersion(), txEntry.getRecord().getRecordType(), 0, null)
                      .getResult());
        }
      }
      break;

    case ORecordOperation.UPDATED:
      final byte[] stream = txEntry.getRecord().toStream();

      if (stream == null) {
        OLogManager.instance().warn(this, "Null serialization on committing updated record %s in transaction", rid);
        break;
      }

      txEntry
          .getRecord()
          .getRecordVersion()
          .copyFrom(
              updateRecord(rid, stream, txEntry.getRecord().getRecordVersion(), txEntry.getRecord().getRecordType(), 0, null)
                  .getResult());
      break;

    case ORecordOperation.DELETED:
      deleteRecord(rid, txEntry.getRecord().getRecordVersion(), 0, null);
      break;
    }

    txEntry.getRecord().unsetDirty();

    if (txEntry.getRecord() instanceof OTxListener)
      ((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.AFTER_COMMIT);
  }

  @Override
  public String getURL() {
    return OEngineMemory.NAME + ":" + url;
  }

  public OStorageConfigurationSegment getConfigurationSegment() {
    return null;
  }

  public void renameCluster(final String iOldName, final String iNewName) {
    final OClusterMemory cluster = (OClusterMemory) getClusterByName(iOldName);
    if (cluster != null)
      try {
        cluster.set(com.orientechnologies.orient.core.storage.OCluster.ATTRIBUTES.NAME, iNewName);
      } catch (IOException e) {
      }
  }

  public void setDefaultClusterId(int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  @Override
  public String getType() {
    return OEngineMemory.NAME;
  }

  private void checkClusterSegmentIndexRange(final int iClusterId) {
    if (iClusterId > clusters.size() - 1)
      throw new IllegalArgumentException("Cluster segment #" + iClusterId + " does not exist in database '" + name + "'");
  }

}
