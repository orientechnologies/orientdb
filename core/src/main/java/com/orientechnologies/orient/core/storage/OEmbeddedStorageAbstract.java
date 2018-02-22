/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.*;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OOfflineCluster;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public abstract class OEmbeddedStorageAbstract extends OStorageAbstract implements OStorageIndex {
  protected static final Comparator<ORecordOperation> COMMIT_RECORD_OPERATION_COMPARATOR = Comparator
      .comparing(o -> o.getRecord().getIdentity());

  protected final Map<String, OCluster>     clusterMap         = new HashMap<>();
  protected final List<OCluster>            clusters           = new ArrayList<>();
  protected final Map<String, OIndexEngine> indexEngineNameMap = new HashMap<>();
  protected final List<OIndexEngine>        indexEngines       = new ArrayList<>();

  /**
   * Error which happened inside of storage or during data processing related to this storage.
   */
  protected final AtomicReference<Error> jvmError = new AtomicReference<>();
  protected int defaultClusterId;

  protected volatile ORecordConflictStrategy recordConflictStrategy = Orient.instance().getRecordConflictStrategy()
      .getDefaultImplementation();

  public OEmbeddedStorageAbstract(final String name, final String iURL, final String mode) {
    super(name, iURL, mode);
  }

  protected abstract OCluster createCluster(final String clusterName);

  @Override
  public ORecordConflictStrategy getConflictStrategy() {
    return recordConflictStrategy;
  }

  @Override
  public void setConflictStrategy(final ORecordConflictStrategy recordConflictStrategy) {
    this.recordConflictStrategy = recordConflictStrategy;
  }

  @Override
  public int loadExternalIndexEngine(String engineName, String algorithm, String indexType, OIndexDefinition indexDefinition,
      OBinarySerializer valueSerializer, boolean isAutomatic, Boolean durableInNonTxMode, int version,
      Map<String, String> engineProperties) {
    try {
      checkOpenness();

      stateLock.acquireWriteLock();
      try {
        checkOpenness();

        checkLowDiskSpaceRequestsAndReadOnlyConditions();

        // this method introduced for binary compatibility only
        if (getConfiguration().getBinaryFormatVersion() > 15)
          return -1;

        if (indexEngineNameMap.containsKey(engineName))
          throw new OIndexException("Index with name " + engineName + " already exists");

        makeStorageDirty();

        final OBinarySerializer keySerializer = determineKeySerializer(indexDefinition);
        final int keySize = determineKeySize(indexDefinition);
        final OType[] keyTypes = indexDefinition != null ? indexDefinition.getTypes() : null;
        final boolean nullValuesSupport = indexDefinition != null && !indexDefinition.isNullValuesIgnored();

        final OStorageConfigurationImpl.IndexEngineData engineData = new OStorageConfigurationImpl.IndexEngineData(engineName,
            algorithm, indexType, durableInNonTxMode, version, valueSerializer.getId(), keySerializer.getId(), isAutomatic,
            keyTypes, nullValuesSupport, keySize, engineProperties);

        final OIndexEngine engine = OIndexes
            .createIndexEngine(engineName, algorithm, indexType, durableInNonTxMode, this, version, engineProperties, null);
        engine.load(engineName, valueSerializer, isAutomatic, keySerializer, keyTypes, nullValuesSupport, keySize,
            engineData.getEngineProperties());

        indexEngineNameMap.put(engineName, engine);
        indexEngines.add(engine);
        getConfiguration().addIndexEngine(engineName, engineData);

        return indexEngines.size() - 1;

      } catch (IOException e) {
        throw OException
            .wrapException(new OStorageException("Cannot add index engine '" + engineName + "' in storage '" + name + "'"), e);
      } finally {
        stateLock.releaseWriteLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public int loadIndexEngine(final String name) {
    try {
      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OIndexEngine engine = indexEngineNameMap.get(name);
        if (engine == null)
          return -1;

        final int indexId = indexEngines.indexOf(engine);
        assert indexId >= 0;

        return indexId;

      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    try {
      if (currentClusterId == -1)
        return new OPhysicalPosition[0];

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = getClusterById(currentClusterId);
        return cluster.higherPositions(physicalPosition);
      } catch (IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    try {
      if (clusterId == -1)
        return new OPhysicalPosition[0];

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = getClusterById(clusterId);
        return cluster.ceilingPositions(physicalPosition);
      } catch (IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    try {
      if (currentClusterId == -1)
        return new OPhysicalPosition[0];

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = getClusterById(currentClusterId);

        return cluster.lowerPositions(physicalPosition);
      } catch (IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + currentClusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    try {
      if (clusterId == -1)
        return new OPhysicalPosition[0];

      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = getClusterById(clusterId);

        return cluster.floorPositions(physicalPosition);
      } catch (IOException ioe) {
        throw OException
            .wrapException(new OStorageException("Cluster Id " + clusterId + " is invalid in storage '" + name + '\''), ioe);
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
    try {
      stateLock.acquireReadLock();
      try {
        if (iExclusiveLock) {
          return super.callInLock(iCallable, true);
        } else {
          return super.callInLock(iCallable, false);
        }
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public Set<String> getClusterNames() {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        return new HashSet<>(clusterMap.keySet());
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public int getClusterIdByName(final String clusterName) {
    try {
      checkOpenness();

      if (clusterName == null)
        throw new IllegalArgumentException("Cluster name is null");

      if (clusterName.length() == 0)
        throw new IllegalArgumentException("Cluster name is empty");

      // if (Character.isDigit(clusterName.charAt(0)))
      // return Integer.parseInt(clusterName);

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        // SEARCH IT BETWEEN PHYSICAL CLUSTERS

        final OCluster segment = clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));
        if (segment != null)
          return segment.getId();

        return -1;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * That is internal method which is called once we encounter any error inside of JVM. In such case we need to restart JVM to avoid
   * any data corruption. Till JVM is not restarted storage will be put in read-only state.
   */
  public void handleJVMError(Error e) {
    jvmError.compareAndSet(null, e);
  }

  @Override
  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  @Override
  public void setDefaultClusterId(final int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  @Override
  public OStorageConfigurationModifiable getConfiguration() {
    return (OStorageConfigurationModifiable) configuration;
  }

  @Override
  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  @Override
  public long count(final int iClusterId) {
    return count(iClusterId, false);
  }

  @Override
  public long count(int clusterId, boolean countTombstones) {
    try {
      if (clusterId == -1)
        throw new OStorageException("Cluster Id " + clusterId + " is invalid in database '" + name + "'");

      // COUNT PHYSICAL CLUSTER IF ANY
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        final OCluster cluster = clusters.get(clusterId);
        if (cluster == null)
          return 0;

        if (countTombstones)
          return cluster.getEntries();

        return cluster.getEntries() - cluster.getTombstonesCount();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    try {
      checkOpenness();

      long tot = 0;

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        for (int iClusterId : iClusterIds) {
          if (iClusterId >= clusters.size())
            throw new OConfigurationException("Cluster id " + iClusterId + " was not found in database '" + name + "'");

          if (iClusterId > -1) {
            final OCluster c = clusters.get(iClusterId);
            if (c != null)
              tot += c.getEntries() - (countTombstones ? 0L : c.getTombstonesCount());
          }
        }

        return tot;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public String getPhysicalClusterNameById(final int iClusterId) {
    try {
      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();

        if (iClusterId < 0 || iClusterId >= clusters.size())
          return null;

        return clusters.get(iClusterId) != null ? clusters.get(iClusterId).getName() : null;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OCluster getClusterById(int iClusterId) {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();

        if (iClusterId == ORID.CLUSTER_ID_INVALID)
          // GET THE DEFAULT CLUSTER
          iClusterId = defaultClusterId;

        final OCluster cluster = doGetAndCheckCluster(iClusterId);

        return cluster;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public OCluster getClusterByName(final String clusterName) {
    try {
      checkOpenness();

      stateLock.acquireReadLock();
      try {
        checkOpenness();
        final OCluster cluster = clusterMap.get(clusterName.toLowerCase(configuration.getLocaleInstance()));

        if (cluster == null)
          throw new OStorageException("Cluster " + clusterName + " does not exist in database '" + name + "'");
        return cluster;
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public long getSize() {
    try {
      try {
        long size = 0;

        stateLock.acquireReadLock();
        try {
          for (OCluster c : clusters)
            if (c != null)
              size += c.getRecordsSize();
        } finally {
          stateLock.releaseReadLock();
        }

        return size;
      } catch (IOException ioe) {
        throw OException.wrapException(new OStorageException("Cannot calculate records size"), ioe);
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public int getClusters() {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();
        return clusterMap.size();
      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public Set<OCluster> getClusterInstances() {
    try {
      checkOpenness();
      stateLock.acquireReadLock();
      try {
        checkOpenness();
        final Set<OCluster> result = new HashSet<>();

        // ADD ALL THE CLUSTERS
        for (OCluster c : clusters)
          if (c != null)
            result.add(c);

        return result;

      } finally {
        stateLock.releaseReadLock();
      }
    } catch (RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  protected Error logAndPrepareForRethrow(final Error error) {
    return logAndPrepareForRethrow(error, true);
  }

  protected Error logAndPrepareForRethrow(final Error error, final boolean putInReadOnlyMode) {
    if (!(error instanceof OHighLevelException))
      OLogManager.instance()
          .errorStorage(this, "Exception `%08X` in storage `%s`: %s", error, System.identityHashCode(error), getURL(),
              OConstants.getVersion());

    if (putInReadOnlyMode) {
      handleJVMError(error);
    }

    return error;
  }

  protected RuntimeException logAndPrepareForRethrow(RuntimeException runtimeException) {
    if (!(runtimeException instanceof OHighLevelException || runtimeException instanceof ONeedRetryException))
      OLogManager.instance()
          .errorStorage(this, "Exception `%08X` in storage `%s`: %s", runtimeException, System.identityHashCode(runtimeException),
              getURL(), OConstants.getVersion());
    return runtimeException;
  }

  protected RuntimeException logAndPrepareForRethrow(final Throwable throwable) {
    if (!(throwable instanceof OHighLevelException || throwable instanceof ONeedRetryException))
      OLogManager.instance()
          .errorStorage(this, "Exception `%08X` in storage `%s`: %s", throwable, System.identityHashCode(throwable), getURL(),
              OConstants.getVersion());
    return new RuntimeException(throwable);
  }

  protected OInvalidIndexEngineIdException logAndPrepareForRethrow(final OInvalidIndexEngineIdException exception) {
    OLogManager.instance()
        .errorStorage(this, "Exception `%08X` in storage `%s` : %s", exception, System.identityHashCode(exception), getURL(),
            OConstants.getVersion());
    return exception;
  }

  /**
   * Checks if the storage is open. If it's closed an exception is raised.
   */
  @SuppressWarnings("WeakerAccess")
  protected void checkOpenness() {
    if (status != STATUS.OPEN)
      throw new OStorageException("Storage " + name + " is not opened.");
  }

  protected void commitIndexes(final Map<String, OTransactionIndexChanges> indexesToCommit) {
    final Map<OIndex, OIndexAbstract.IndexTxSnapshot> snapshots = new IdentityHashMap<>();

    for (OTransactionIndexChanges changes : indexesToCommit.values()) {
      final OIndexInternal<?> index = changes.getAssociatedIndex();
      final OIndexAbstract.IndexTxSnapshot snapshot = new OIndexAbstract.IndexTxSnapshot();
      snapshots.put(index, snapshot);

      index.preCommit(snapshot);
    }

    for (OTransactionIndexChanges changes : indexesToCommit.values()) {
      final OIndexInternal<?> index = changes.getAssociatedIndex();
      final OIndexAbstract.IndexTxSnapshot snapshot = snapshots.get(index);

      index.addTxOperation(snapshot, changes);
    }

    try {
      for (OTransactionIndexChanges changes : indexesToCommit.values()) {
        final OIndexInternal<?> index = changes.getAssociatedIndex();
        final OIndexAbstract.IndexTxSnapshot snapshot = snapshots.get(index);

        index.commit(snapshot);
      }
    } finally {
      for (OTransactionIndexChanges changes : indexesToCommit.values()) {
        final OIndexInternal<?> index = changes.getAssociatedIndex();
        final OIndexAbstract.IndexTxSnapshot snapshot = snapshots.get(index);

        index.postCommit(snapshot);
      }
    }
  }

  /**
   * Register the cluster internally.
   *
   * @param cluster OCluster implementation
   *
   * @return The id (physical position into the array) of the new cluster just created. First is 0.
   */
  protected int registerCluster(final OCluster cluster) {
    final int id;

    if (cluster != null) {
      // CHECK FOR DUPLICATION OF NAMES
      if (clusterMap.containsKey(cluster.getName().toLowerCase(configuration.getLocaleInstance())))
        throw new OConfigurationException(
            "Cannot add cluster '" + cluster.getName() + "' because it is already registered in database '" + name + "'");
      // CREATE AND ADD THE NEW REF SEGMENT
      clusterMap.put(cluster.getName().toLowerCase(configuration.getLocaleInstance()), cluster);
      id = cluster.getId();
    } else {
      id = clusters.size();
    }

    setCluster(id, cluster);

    return id;
  }

  protected void setCluster(final int id, final OCluster cluster) {
    if (clusters.size() <= id) {
      while (clusters.size() < id)
        clusters.add(null);

      clusters.add(cluster);
    } else
      clusters.set(id, cluster);
  }

  protected int doAddCluster(final String clusterName, final Object[] parameters) throws IOException {
    // FIND THE FIRST AVAILABLE CLUSTER ID
    int clusterPos = clusters.size();
    for (int i = 0; i < clusters.size(); ++i) {
      if (clusters.get(i) == null) {
        clusterPos = i;
        break;
      }
    }

    return addClusterInternal(clusterName, clusterPos, parameters);
  }

  protected void addDefaultClusters() throws IOException {
    final String storageCompression = getConfiguration().getContextConfiguration()
        .getValueAsString(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD);

    final String storageEncryption = getConfiguration().getContextConfiguration()
        .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD);

    final String encryptionKey = getConfiguration().getContextConfiguration()
        .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

    final String stgConflictStrategy = getConflictStrategy().getName();

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(getConfiguration(), clusters.size(), OMetadataDefault.CLUSTER_INTERNAL_NAME, null,
            true, 20, 4, storageCompression, storageEncryption, encryptionKey, stgConflictStrategy,
            OStorageClusterConfiguration.STATUS.ONLINE));

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(getConfiguration(), clusters.size(), OMetadataDefault.CLUSTER_INDEX_NAME, null,
            false, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
            OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, storageCompression, storageEncryption, encryptionKey,
            stgConflictStrategy, OStorageClusterConfiguration.STATUS.ONLINE));

    createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(getConfiguration(), clusters.size(), OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME,
            null, false, 1, 1, storageCompression, storageEncryption, encryptionKey, stgConflictStrategy,
            OStorageClusterConfiguration.STATUS.ONLINE));

    defaultClusterId = createClusterFromConfig(
        new OStoragePaginatedClusterConfiguration(getConfiguration(), clusters.size(), CLUSTER_DEFAULT_NAME, null, true,
            OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
            storageCompression, storageEncryption, encryptionKey, stgConflictStrategy, OStorageClusterConfiguration.STATUS.ONLINE));
  }

  protected int createClusterFromConfig(final OStorageClusterConfiguration config) throws IOException {
    OCluster cluster = clusterMap.get(config.getName().toLowerCase(configuration.getLocaleInstance()));

    if (cluster != null) {
      cluster.configure(this, config);
      return -1;
    }

    if (config.getStatus() == OStorageClusterConfiguration.STATUS.ONLINE)
      cluster = createCluster(config.getName());
    else
      cluster = new OOfflineCluster(this, config.getId(), config.getName());
    cluster.configure(this, config);

    return registerCluster(cluster);
  }

  protected int addClusterInternal(String clusterName, final int clusterPos, final Object... parameters) throws IOException {

    final OCluster cluster;
    if (clusterName != null) {
      clusterName = clusterName.toLowerCase(configuration.getLocaleInstance());

      cluster = createCluster(clusterName);
      cluster.configure(this, clusterPos, clusterName, parameters);
    } else {
      cluster = null;
    }

    final int createdClusterId = registerCluster(cluster);

    if (cluster != null) {
      if (!cluster.exists()) {
        cluster.create(-1);
      } else {
        cluster.open();
      }

      ((OStorageConfigurationModifiable) configuration).update();
    }

    if (OLogManager.instance().isDebugEnabled())
      OLogManager.instance()
          .debug(this, "Created cluster '%s' in database '%s' with id %d. Clusters: %s", clusterName, url, createdClusterId,
              clusters);

    return createdClusterId;
  }

  protected OBinarySerializer determineKeySerializer(final OIndexDefinition indexDefinition) {
    final OBinarySerializer keySerializer;
    if (indexDefinition != null) {
      if (indexDefinition instanceof ORuntimeKeyIndexDefinition) {
        keySerializer = ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer();
      } else {
        if (indexDefinition.getTypes().length > 1) {
          keySerializer = OCompositeKeySerializer.INSTANCE;
        } else {
          OCurrentStorageComponentsFactory currentStorageComponentsFactory = componentsFactory;
          if (currentStorageComponentsFactory != null)
            keySerializer = currentStorageComponentsFactory.binarySerializerFactory
                .getObjectSerializer(indexDefinition.getTypes()[0]);
          else
            throw new IllegalStateException("Cannot load binary serializer, storage is not properly initialized");
        }
      }
    } else {
      keySerializer = new OSimpleKeySerializer();
    }
    return keySerializer;
  }

  protected int determineKeySize(final OIndexDefinition indexDefinition) {
    if (indexDefinition == null || indexDefinition instanceof ORuntimeKeyIndexDefinition)
      return 1;
    else
      return indexDefinition.getTypes().length;
  }

  protected void checkClusterSegmentIndexRange(final int iClusterId) {
    if (iClusterId < 0 || iClusterId > clusters.size() - 1)
      throw new IllegalArgumentException("Cluster segment #" + iClusterId + " does not exist in database '" + name + "'");
  }

  protected OCluster doGetAndCheckCluster(final int clusterId) {
    checkClusterSegmentIndexRange(clusterId);

    final OCluster cluster = clusters.get(clusterId);
    if (cluster == null)
      throw new IllegalArgumentException("Cluster " + clusterId + " is null");
    return cluster;
  }

  protected void makeStorageDirty() throws IOException {
  }

  protected void checkLowDiskSpaceRequestsAndReadOnlyConditions() {
  }
}
