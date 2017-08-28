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

package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.ORidBagDeleter;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.*;
import com.orientechnologies.orient.core.record.impl.*;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSaveThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorageComponent;
import com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OOfflineClusterException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Document API entrypoint.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public abstract class ODatabaseDocumentAbstract extends OListenerManger<ODatabaseListener> implements ODatabaseDocumentInternal {

  protected final Map<String, Object> properties = new HashMap<String, Object>();
  protected Map<ORecordHook, ORecordHook.HOOK_POSITION> unmodifiableHooks;
  protected final Set<OIdentifiable> inHook = new HashSet<OIdentifiable>();
  protected ORecordSerializer    serializer;
  protected String               url;
  protected STATUS               status;
  protected OIntent              currentIntent;
  protected ODatabaseInternal<?> databaseOwner;
  protected OMetadataDefault     metadata;
  protected OImmutableUser       user;
  protected final byte                                        recordType    = ODocument.RECORD_TYPE;
  protected final Map<ORecordHook, ORecordHook.HOOK_POSITION> hooks         = new LinkedHashMap<ORecordHook, ORecordHook.HOOK_POSITION>();
  protected       boolean                                     retainRecords = true;
  protected OLocalRecordCache                localCache;
  protected OCurrentStorageComponentsFactory componentsFactory;
  protected boolean initialized = false;
  protected OTransaction currentTx;

  protected final ORecordHook[][] hooksByScope = new ORecordHook[ORecordHook.SCOPE.values().length][];
  protected OSharedContext sharedContext;

  private boolean prefetchRecords;

  protected OMicroTransaction microTransaction = null;

  protected Map<String, OResultSet> activeQueries = new HashMap<>();

  protected ODatabaseDocumentAbstract() {
    // DO NOTHING IS FOR EXTENDED OBJECTS
    super(false);
  }

  /**
   * @return default serializer which is used to serialize documents. Default serializer is common for all database instances.
   */
  public static ORecordSerializer getDefaultSerializer() {
    return ORecordSerializerFactory.instance().getDefaultRecordSerializer();
  }

  /**
   * Sets default serializer. The default serializer is common for all database instances.
   *
   * @param iDefaultSerializer new default serializer value
   */
  public static void setDefaultSerializer(ORecordSerializer iDefaultSerializer) {
    ORecordSerializerFactory.instance().setDefaultRecordSerializer(iDefaultSerializer);
  }

  public void callOnOpenListeners() {
    // WAKE UP DB LIFECYCLE LISTENER
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
      it.next().onOpen(getDatabaseOwner());

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : getListenersCopy())
      try {
        listener.onOpen(getDatabaseOwner());
      } catch (Throwable t) {
        t.printStackTrace();
      }
  }

  protected abstract void loadMetadata();

  public void callOnCloseListeners() {
    // WAKE UP DB LIFECYCLE LISTENER
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
      it.next().onClose(getDatabaseOwner());

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : getListenersCopy())
      try {
        listener.onClose(getDatabaseOwner());
      } catch (Throwable t) {
        t.printStackTrace();
      }
  }

  public void callOnDropListeners() {

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : getListenersCopy())
      try {
        activateOnCurrentThread();
        listener.onDelete(getDatabaseOwner());
      } catch (Throwable t) {
        t.printStackTrace();
      }
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends ORecord> RET getRecord(final OIdentifiable iIdentifiable) {
    if (iIdentifiable instanceof ORecord)
      return (RET) iIdentifiable;
    return (RET) load(iIdentifiable.getIdentity());
  }

  @Override
  public void reload() {
    checkIfActive();

    if (this.isClosed())
      throw new ODatabaseException("Cannot reload a closed db");

    metadata.reload();
    getStorage().reload();
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends ORecord> RET load(final ORID iRecordId, final String iFetchPlan, final boolean iIgnoreCache) {
    return (RET) executeReadRecord((ORecordId) iRecordId, null, -1, iFetchPlan, iIgnoreCache, !iIgnoreCache, false,
        OStorage.LOCKING_STRATEGY.DEFAULT, new SimpleRecordReader(prefetchRecords));
  }

  /**
   * Deletes the record checking the version.
   */
  public ODatabase<ORecord> delete(final ORID iRecord, final int iVersion) {
    ORecord record = load(iRecord);
    ORecordInternal.setVersion(record, iVersion);
    delete(record);
    return this;
  }

  public ODatabaseDocumentInternal cleanOutRecord(final ORID iRecord, final int iVersion) {
    executeDeleteRecord(iRecord, iVersion, true, OPERATION_MODE.SYNCHRONOUS, true);
    return this;
  }

  public String getType() {
    return TYPE;
  }

  /**
   * Deletes the record without checking the version.
   */
  public ODatabaseDocument delete(final ORID iRecord, final OPERATION_MODE iMode) {
    ORecord record = load(iRecord);
    if (record == null)
      return this;

    delete(record, iMode);
    return this;
  }

  public ODatabaseDocument delete(final ORecord iRecord, final OPERATION_MODE iMode) {
    checkIfActive();
    currentTx.deleteRecord(iRecord, iMode);
    return this;
  }

  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(final String iClusterName, final Class<REC> iClass) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    checkIfActive();

    final int clusterId = getClusterIdByName(iClusterName);

    return new ORecordIteratorCluster<REC>(this, this, clusterId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Deprecated
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(final String iClusterName, final Class<REC> iRecordClass,
      final long startClusterPosition, final long endClusterPosition, final boolean loadTombstones) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    checkIfActive();

    final int clusterId = getClusterIdByName(iClusterName);

    return new ORecordIteratorCluster<REC>(this, this, clusterId, startClusterPosition, endClusterPosition, loadTombstones,
        OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @Override
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName, Class<REC> iRecordClass,
      long startClusterPosition, long endClusterPosition) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    checkIfActive();

    final int clusterId = getClusterIdByName(iClusterName);

    return new ORecordIteratorCluster<REC>(this, this, clusterId, startClusterPosition, endClusterPosition);
  }

  /**
   * {@inheritDoc}
   */
  public OCommandRequest command(final OCommandRequest iCommand) {
    checkSecurity(ORule.ResourceGeneric.COMMAND, ORole.PERMISSION_READ);
    checkIfActive();

    final OCommandRequestInternal command = (OCommandRequestInternal) iCommand;

    try {
      command.reset();
      return command;

    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Error on command execution"), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends List<?>> RET query(final OQuery<?> iCommand, final Object... iArgs) {
    checkIfActive();
    iCommand.reset();
    return (RET) iCommand.execute(iArgs);
  }

  /**
   * {@inheritDoc}
   */
  public byte getRecordType() {
    return recordType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(final int[] iClusterIds) {
    return countClusterElements(iClusterIds, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(final int iClusterId) {
    return countClusterElements(iClusterId, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void truncateCluster(String clusterName) {
    command("truncate cluster " + clusterName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(int iClusterId, boolean countTombstones) {
    final String name = getClusterNameById(iClusterId);
    if (name == null)
      return 0;

    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, name);
    checkIfActive();

    return getStorage().count(iClusterId, countTombstones);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(int[] iClusterIds, boolean countTombstones) {
    checkIfActive();
    String name;
    for (int iClusterId : iClusterIds) {
      name = getClusterNameById(iClusterId);
      checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, name);
    }

    return getStorage().count(iClusterIds, countTombstones);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(final String iClusterName) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    checkIfActive();

    final int clusterId = getClusterIdByName(iClusterName);
    if (clusterId < 0)
      throw new IllegalArgumentException("Cluster '" + iClusterName + "' was not found");
    return getStorage().count(clusterId);
  }

  /**
   * {@inheritDoc}
   */
  public OMetadataDefault getMetadata() {
    checkOpenness();
    return metadata;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabaseDocument> DB checkSecurity(final ORule.ResourceGeneric resourceGeneric, final String resourceSpecific,
      final int iOperation) {
    if (user != null) {
      try {
        user.allow(resourceGeneric, resourceSpecific, iOperation);
      } catch (OSecurityAccessException e) {

        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance()
              .debug(this, "User '%s' tried to access the reserved resource '%s.%s', operation '%s'", getUser(), resourceGeneric,
                  resourceSpecific, iOperation);

        throw e;
      }
    }
    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabaseDocument> DB checkSecurity(final ORule.ResourceGeneric iResourceGeneric, final int iOperation,
      final Object... iResourcesSpecific) {

    if (user != null) {
      try {
        if (iResourcesSpecific.length != 0) {
          for (Object target : iResourcesSpecific) {
            if (target != null) {
              user.allow(iResourceGeneric, target.toString(), iOperation);
            } else
              user.allow(iResourceGeneric, null, iOperation);
          }
        } else
          user.allow(iResourceGeneric, null, iOperation);
      } catch (OSecurityAccessException e) {
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance()
              .debug(this, "[checkSecurity] User '%s' tried to access the reserved resource '%s', target(s) '%s', operation '%s'",
                  getUser(), iResourceGeneric, Arrays.toString(iResourcesSpecific), iOperation);

        throw e;
      }
    }
    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabaseDocument> DB checkSecurity(final ORule.ResourceGeneric iResourceGeneric, final int iOperation,
      final Object iResourceSpecific) {
    checkOpenness();
    if (user != null) {
      try {
        if (iResourceSpecific != null)
          user.allow(iResourceGeneric, iResourceSpecific.toString(), iOperation);
        else
          user.allow(iResourceGeneric, null, iOperation);
      } catch (OSecurityAccessException e) {
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance()
              .debug(this, "[checkSecurity] User '%s' tried to access the reserved resource '%s', target '%s', operation '%s'",
                  getUser(), iResourceGeneric, iResourceSpecific, iOperation);

        throw e;
      }
    }
    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabaseInternal<?> getDatabaseOwner() {
    ODatabaseInternal<?> current = databaseOwner;

    while (current != null && current != this && current.getDatabaseOwner() != current)
      current = current.getDatabaseOwner();

    return current;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabaseInternal<ORecord> setDatabaseOwner(ODatabaseInternal<?> iOwner) {
    databaseOwner = iOwner;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isRetainRecords() {
    return retainRecords;
  }

  /**
   * {@inheritDoc}
   */
  public ODatabaseDocument setRetainRecords(boolean retainRecords) {
    this.retainRecords = retainRecords;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase> DB setStatus(final STATUS status) {
    checkIfActive();
    setStatusInternal(status);
    return (DB) this;
  }

  public void setStatusInternal(final STATUS status) {
    this.status = status;
  }

  /**
   * Deprecated since v2.2
   */
  @Deprecated
  public void setDefaultClusterIdInternal(final int iDefClusterId) {
    checkIfActive();
    getStorage().setDefaultClusterId(iDefClusterId);
  }

  /**
   * {@inheritDoc}
   */
  public void setInternal(final ATTRIBUTES iAttribute, final Object iValue) {
    set(iAttribute, iValue);
  }

  /**
   * {@inheritDoc}
   */
  public OSecurityUser getUser() {
    return user;
  }

  /**
   * {@inheritDoc}
   */
  public void setUser(final OSecurityUser user) {
    checkIfActive();
    if (user instanceof OUser) {
      OMetadata metadata = getMetadata();
      if (metadata != null) {
        final OSecurity security = metadata.getSecurity();
        this.user = new OImmutableUser(security.getVersion(), (OUser) user);
      } else
        this.user = new OImmutableUser(-1, (OUser) user);
    } else
      this.user = (OImmutableUser) user;
  }

  public void reloadUser() {
    if (user != null) {
      activateOnCurrentThread();

      OMetadata metadata = getMetadata();

      if (metadata != null) {
        final OSecurity security = metadata.getSecurity();
        OUser secGetUser = security.getUser(user.getName());

        if (secGetUser != null)
          user = new OImmutableUser(security.getVersion(), secGetUser);
        else
          user = new OImmutableUser(-1, new OUser());
      } else
        user = new OImmutableUser(-1, new OUser());
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean isMVCC() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase<?>> DB setMVCC(boolean mvcc) {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  public ODictionary<ORecord> getDictionary() {
    checkOpenness();
    return metadata.getIndexManager().getDictionary();
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase<?>> DB registerHook(final ORecordHook iHookImpl, final ORecordHook.HOOK_POSITION iPosition) {
    checkOpenness();
    checkIfActive();

    final Map<ORecordHook, ORecordHook.HOOK_POSITION> tmp = new LinkedHashMap<ORecordHook, ORecordHook.HOOK_POSITION>(hooks);
    tmp.put(iHookImpl, iPosition);
    hooks.clear();
    for (ORecordHook.HOOK_POSITION p : ORecordHook.HOOK_POSITION.values()) {
      for (Map.Entry<ORecordHook, ORecordHook.HOOK_POSITION> e : tmp.entrySet()) {
        if (e.getValue() == p)
          hooks.put(e.getKey(), e.getValue());
      }
    }

    compileHooks();

    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase<?>> DB registerHook(final ORecordHook iHookImpl) {
    return (DB) registerHook(iHookImpl, ORecordHook.HOOK_POSITION.REGULAR);
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase<?>> DB unregisterHook(final ORecordHook iHookImpl) {
    checkIfActive();
    if (iHookImpl != null) {
      iHookImpl.onUnregister();
      hooks.remove(iHookImpl);
      compileHooks();
    }

    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OLocalRecordCache getLocalCache() {
    return localCache;
  }

  /**
   * {@inheritDoc}
   */
  public Map<ORecordHook, ORecordHook.HOOK_POSITION> getHooks() {
    return unmodifiableHooks;
  }

  /**
   * Callback the registered hooks if any.
   *
   * @param type Hook type. Define when hook is called.
   * @param id   Record received in the callback
   *
   * @return True if the input record is changed, otherwise false
   */
  public ORecordHook.RESULT callbackHooks(final ORecordHook.TYPE type, final OIdentifiable id) {
    if (id == null || hooks.isEmpty() || id.getIdentity().getClusterId() == 0)
      return ORecordHook.RESULT.RECORD_NOT_CHANGED;

    final ORecordHook.SCOPE scope = ORecordHook.SCOPE.typeToScope(type);
    final int scopeOrdinal = scope.ordinal();

    final ORID identity = id.getIdentity().copy();
    if (!pushInHook(identity))
      return ORecordHook.RESULT.RECORD_NOT_CHANGED;

    try {
      final ORecord rec = id.getRecord();
      if (rec == null)
        return ORecordHook.RESULT.RECORD_NOT_CHANGED;

      final OScenarioThreadLocal.RUN_MODE runMode = OScenarioThreadLocal.INSTANCE.getRunMode();

      boolean recordChanged = false;
      for (ORecordHook hook : hooksByScope[scopeOrdinal]) {
        switch (runMode) {
        case DEFAULT: // NON_DISTRIBUTED OR PROXIED DB
          if (getStorage().isDistributed()
              && hook.getDistributedExecutionMode() == ORecordHook.DISTRIBUTED_EXECUTION_MODE.TARGET_NODE)
            // SKIP
            continue;
          break; // TARGET NODE
        case RUNNING_DISTRIBUTED:
          if (hook.getDistributedExecutionMode() == ORecordHook.DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE)
            continue;
        }

        final ORecordHook.RESULT res = hook.onTrigger(type, rec);

        if (res == ORecordHook.RESULT.RECORD_CHANGED)
          recordChanged = true;
        else if (res == ORecordHook.RESULT.SKIP_IO)
          // SKIP IO OPERATION
          return res;
        else if (res == ORecordHook.RESULT.SKIP)
          // SKIP NEXT HOOKS AND RETURN IT
          return res;
        else if (res == ORecordHook.RESULT.RECORD_REPLACED)
          return res;
      }

      return recordChanged ? ORecordHook.RESULT.RECORD_CHANGED : ORecordHook.RESULT.RECORD_NOT_CHANGED;

    } finally {
      popInHook(identity);
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean isValidationEnabled() {
    return (Boolean) get(ATTRIBUTES.VALIDATION);
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabaseDocument> DB setValidationEnabled(final boolean iEnabled) {
    set(ATTRIBUTES.VALIDATION, iEnabled);
    return (DB) this;
  }

  public ORecordConflictStrategy getConflictStrategy() {
    checkIfActive();
    return getStorage().getConflictStrategy();
  }

  public ODatabaseDocumentAbstract setConflictStrategy(final String iStrategyName) {
    checkIfActive();
    getStorage().setConflictStrategy(Orient.instance().getRecordConflictStrategy().getStrategy(iStrategyName));
    return this;
  }

  public ODatabaseDocumentAbstract setConflictStrategy(final ORecordConflictStrategy iResolver) {
    checkIfActive();
    getStorage().setConflictStrategy(iResolver);
    return this;
  }

  @Override
  public OContextConfiguration getConfiguration() {
    checkIfActive();
    if (getStorage() != null)
      return getStorage().getConfiguration().getContextConfiguration();
    return null;
  }

  @Override
  public boolean declareIntent(final OIntent iIntent) {
    checkIfActive();

    if (currentIntent != null) {
      if (iIntent != null && iIntent.getClass().equals(currentIntent.getClass()))
        // SAME INTENT: JUMP IT
        return false;

      // END CURRENT INTENT
      currentIntent.end(this);
    }

    currentIntent = iIntent;

    if (iIntent != null)
      iIntent.begin(this);

    return true;
  }

  @Override
  public OIntent getActiveIntent() {
    return currentIntent;
  }

  @Override
  public void close() {
    checkIfActive();

    try {
      closeActiveQueries();

      localCache.shutdown();

      if (isClosed()) {
        status = STATUS.CLOSED;
        return;
      }

      try {
        rollback(true);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Exception during commit of active transaction", e);
      }

      if (status != STATUS.OPEN)
        return;

      callOnCloseListeners();

      if (currentIntent != null) {
        currentIntent.end(this);
        currentIntent = null;
      }
      sharedContext = null;
      status = STATUS.CLOSED;

      localCache.clear();

      if (getStorage() != null)
        getStorage().close();

    } finally {
      // ALWAYS RESET TL
      ODatabaseRecordThreadLocal.INSTANCE.remove();
    }
  }

  @Override
  public STATUS getStatus() {
    return status;
  }

  @Override
  public long getSize() {
    checkIfActive();
    return getStorage().getSize();
  }

  @Override
  public String getName() {
    return getStorage() != null ? getStorage().getName() : url;
  }

  @Override
  public String getURL() {
    return url != null ? url : getStorage().getURL();
  }

  @Override
  public int getDefaultClusterId() {
    checkIfActive();
    return getStorage().getDefaultClusterId();
  }

  @Override
  public int getClusters() {
    checkIfActive();
    return getStorage().getClusters();
  }

  @Override
  public boolean existsCluster(final String iClusterName) {
    checkIfActive();
    return getStorage().getClusterNames().contains(iClusterName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Collection<String> getClusterNames() {
    checkIfActive();
    return getStorage().getClusterNames();
  }

  @Override
  public int getClusterIdByName(final String iClusterName) {
    if (iClusterName == null)
      return -1;

    checkIfActive();
    return getStorage().getClusterIdByName(iClusterName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public String getClusterNameById(final int iClusterId) {
    if (iClusterId < 0)
      return null;

    checkIfActive();
    return getStorage().getPhysicalClusterNameById(iClusterId);
  }

  @Override
  public long getClusterRecordSizeByName(final String clusterName) {
    checkIfActive();
    try {
      return getStorage().getClusterById(getClusterIdByName(clusterName)).getRecordsSize();
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Error on reading records size for cluster '" + clusterName + "'"), e);
    }
  }

  @Override
  public long getClusterRecordSizeById(final int clusterId) {
    checkIfActive();
    try {
      return getStorage().getClusterById(clusterId).getRecordsSize();
    } catch (Exception e) {
      throw OException
          .wrapException(new ODatabaseException("Error on reading records size for cluster with id '" + clusterId + "'"), e);
    }
  }

  @Override
  public boolean isClosed() {
    return status == STATUS.CLOSED || getStorage().isClosed();
  }

  @Override
  public int addCluster(final String iClusterName, final Object... iParameters) {
    checkIfActive();
    return getStorage().addCluster(iClusterName, false, iParameters);
  }

  @Override
  public int addCluster(final String iClusterName, final int iRequestedId, final Object... iParameters) {
    checkIfActive();
    return getStorage().addCluster(iClusterName, iRequestedId, false, iParameters);
  }

  @Override
  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    checkIfActive();
    final int clusterId = getClusterIdByName(iClusterName);
    OSchemaProxy schema = metadata.getSchema();
    OClass clazz = schema.getClassByClusterId(clusterId);
    if (clazz != null)
      clazz.removeClusterId(clusterId);
    if (schema.getBlobClusters().contains(clusterId))
      schema.removeBlobCluster(iClusterName);
    getLocalCache().freeCluster(clusterId);
    checkForClusterPermissions(iClusterName);
    return getStorage().dropCluster(iClusterName, iTruncate);
  }

  @Override
  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    checkIfActive();

    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_DELETE, getClusterNameById(iClusterId));

    OSchemaProxy schema = metadata.getSchema();
    final OClass clazz = schema.getClassByClusterId(iClusterId);
    if (clazz != null)
      clazz.removeClusterId(iClusterId);
    getLocalCache().freeCluster(iClusterId);
    if (schema.getBlobClusters().contains(iClusterId))
      schema.removeBlobCluster(getClusterNameById(iClusterId));

    checkForClusterPermissions(getClusterNameById(iClusterId));
    return getStorage().dropCluster(iClusterId, iTruncate);
  }

  public void checkForClusterPermissions(final String iClusterName) {
    // CHECK FOR ORESTRICTED
    final Set<OClass> classes = getMetadata().getImmutableSchemaSnapshot().getClassesRelyOnCluster(iClusterName);
    for (OClass c : classes) {
      if (c.isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME))
        throw new OSecurityException(
            "Class '" + c.getName() + "' cannot be truncated because has record level security enabled (extends '"
                + OSecurityShared.RESTRICTED_CLASSNAME + "')");
    }
  }

  @Override
  public Object setProperty(final String iName, final Object iValue) {
    if (iValue == null)
      return properties.remove(iName.toLowerCase(Locale.ENGLISH));
    else
      return properties.put(iName.toLowerCase(Locale.ENGLISH), iValue);
  }

  @Override
  public Object getProperty(final String iName) {
    return properties.get(iName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Iterator<Map.Entry<String, Object>> getProperties() {
    return properties.entrySet().iterator();
  }

  @Override
  public Object get(final ATTRIBUTES iAttribute) {
    checkIfActive();

    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");
    final OStorage storage = getStorage();
    switch (iAttribute) {
    case STATUS:
      return getStatus();
    case DEFAULTCLUSTERID:
      return getDefaultClusterId();
    case TYPE:
      return getMetadata().getImmutableSchemaSnapshot().existsClass("V") ? "graph" : "document";
    case DATEFORMAT:
      return storage.getConfiguration().dateFormat;

    case DATETIMEFORMAT:
      return storage.getConfiguration().dateTimeFormat;

    case TIMEZONE:
      return storage.getConfiguration().getTimeZone().getID();

    case LOCALECOUNTRY:
      return storage.getConfiguration().getLocaleCountry();

    case LOCALELANGUAGE:
      return storage.getConfiguration().getLocaleLanguage();

    case CHARSET:
      return storage.getConfiguration().getCharset();

    case CUSTOM:
      return storage.getConfiguration().getProperties();

    case CLUSTERSELECTION:
      return storage.getConfiguration().getClusterSelection();

    case MINIMUMCLUSTERS:
      return storage.getConfiguration().getMinimumClusters();

    case CONFLICTSTRATEGY:
      return storage.getConfiguration().getConflictStrategy();

    case VALIDATION:
      return storage.getConfiguration().isValidationEnabled();
    }

    return null;
  }

  @Override
  public <DB extends ODatabase> DB set(final ATTRIBUTES iAttribute, final Object iValue) {
    checkIfActive();

    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = OIOUtils.getStringContent(iValue != null ? iValue.toString() : null);
    final OStorage storage = getStorage();
    switch (iAttribute) {
    case STATUS:
      if (stringValue == null)
        throw new IllegalArgumentException("DB status can't be null");
      setStatus(STATUS.valueOf(stringValue.toUpperCase(Locale.ENGLISH)));
      break;

    case DEFAULTCLUSTERID:
      if (iValue != null) {
        if (iValue instanceof Number)
          storage.setDefaultClusterId(((Number) iValue).intValue());
        else
          storage.setDefaultClusterId(storage.getClusterIdByName(iValue.toString()));
      }
      break;

    case TYPE:
      throw new IllegalArgumentException("Database type cannot be changed at run-time");

    case DATEFORMAT:
      if (stringValue == null)
        throw new IllegalArgumentException("date format is null");

      // CHECK FORMAT
      new SimpleDateFormat(stringValue).format(new Date());

      storage.getConfiguration().dateFormat = stringValue;
      storage.getConfiguration().update();
      break;

    case DATETIMEFORMAT:
      if (stringValue == null)
        throw new IllegalArgumentException("date format is null");

      // CHECK FORMAT
      new SimpleDateFormat(stringValue).format(new Date());

      storage.getConfiguration().dateTimeFormat = stringValue;
      storage.getConfiguration().update();
      break;

    case TIMEZONE:
      if (stringValue == null)
        throw new IllegalArgumentException("Timezone can't be null");

      // for backward compatibility, until 2.1.13 OrientDB accepted timezones in lowercase as well
      TimeZone timeZoneValue = TimeZone.getTimeZone(stringValue.toUpperCase(Locale.ENGLISH));
      if (timeZoneValue.equals(TimeZone.getTimeZone("GMT"))) {
        timeZoneValue = TimeZone.getTimeZone(stringValue);
      }

      storage.getConfiguration().setTimeZone(timeZoneValue);
      storage.getConfiguration().update();
      break;

    case LOCALECOUNTRY:
      storage.getConfiguration().setLocaleCountry(stringValue);
      storage.getConfiguration().update();
      break;

    case LOCALELANGUAGE:
      storage.getConfiguration().setLocaleLanguage(stringValue);
      storage.getConfiguration().update();
      break;

    case CHARSET:
      storage.getConfiguration().setCharset(stringValue);
      storage.getConfiguration().update();
      break;

    case CUSTOM:
      int indx = stringValue != null ? stringValue.indexOf('=') : -1;
      if (indx < 0) {
        if ("clear".equalsIgnoreCase(stringValue)) {
          clearCustomInternal();
        } else
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        String customName = stringValue.substring(0, indx).trim();
        String customValue = stringValue.substring(indx + 1).trim();
        if (customValue.isEmpty())
          removeCustomInternal(customName);
        else
          setCustomInternal(customName, customValue);
      }
      break;

    case CLUSTERSELECTION:
      storage.getConfiguration().setClusterSelection(stringValue);
      storage.getConfiguration().update();
      break;

    case MINIMUMCLUSTERS:
      if (iValue != null) {
        if (iValue instanceof Number)
          storage.getConfiguration().setMinimumClusters(((Number) iValue).intValue());
        else
          storage.getConfiguration().setMinimumClusters(Integer.parseInt(stringValue));
      } else
        // DEFAULT = 1
        storage.getConfiguration().setMinimumClusters(1);

      storage.getConfiguration().update();
      break;

    case CONFLICTSTRATEGY:
      storage.setConflictStrategy(Orient.instance().getRecordConflictStrategy().getStrategy(stringValue));
      storage.getConfiguration().setConflictStrategy(stringValue);
      storage.getConfiguration().update();
      break;

    case VALIDATION:
      storage.getConfiguration().setValidation(Boolean.parseBoolean(stringValue));
      storage.getConfiguration().update();
      break;

    default:
      throw new IllegalArgumentException("Option '" + iAttribute + "' not supported on alter database");

    }

    return (DB) this;
  }

  public <DB extends ODatabase> DB setCustom(final String name, final Object iValue) {
    checkIfActive();

    if ("clear".equalsIgnoreCase(name) && iValue == null) {
      clearCustomInternal();
    } else {
      String customName = name;
      String customValue = iValue == null ? null : "" + iValue;
      if (customName == null || customValue.isEmpty())
        removeCustomInternal(customName);
      else
        setCustomInternal(customName, customValue);
    }

    return (DB) this;
  }

  @Override
  public ORecordMetadata getRecordMetadata(final ORID rid) {
    checkIfActive();
    return getStorage().getRecordMetadata(rid);
  }

  public OTransaction getTransaction() {
    checkIfActive();
    return currentTx;
  }

  @Override
  public OBasicTransaction getMicroOrRegularTransaction() {
    return microTransaction != null && microTransaction.isActive() ? microTransaction : getTransaction();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORecord iRecord, final String iFetchPlan) {
    checkIfActive();
    return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  @Override
  @Deprecated
  public <RET extends ORecord> RET load(ORecord iRecord, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkIfActive();
    return (RET) currentTx
        .loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache, !iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  @Deprecated
  public <RET extends ORecord> RET load(final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache,
      final boolean iUpdateCache, final boolean loadTombstone, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkIfActive();
    return (RET) currentTx
        .loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache, iUpdateCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORecord iRecord) {
    checkIfActive();
    return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, null, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORID recordId) {
    return (RET) currentTx.loadRecord(recordId, null, null, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORID iRecordId, final String iFetchPlan) {
    checkIfActive();
    return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan, false);
  }

  @SuppressWarnings("unchecked")
  public <RET extends ORecord> RET loadIfVersionIsNotLatest(final ORID rid, final int recordVersion, String fetchPlan,
      boolean ignoreCache) throws ORecordNotFoundException {
    checkIfActive();
    return (RET) currentTx.loadRecordIfVersionIsNotLatest(rid, recordVersion, fetchPlan, ignoreCache);
  }

  @SuppressWarnings("unchecked")
  @Override
  @Deprecated
  public <RET extends ORecord> RET load(final ORID iRecordId, String iFetchPlan, final boolean iIgnoreCache,
      final boolean loadTombstone, OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkIfActive();
    return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan, iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  @Deprecated
  public <RET extends ORecord> RET load(final ORID iRecordId, String iFetchPlan, final boolean iIgnoreCache,
      final boolean iUpdateCache, final boolean loadTombstone, OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkIfActive();
    return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan, iIgnoreCache, iUpdateCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  public <RET extends ORecord> RET reload(final ORecord iRecord) {
    return reload(iRecord, null, false);
  }

  @SuppressWarnings("unchecked")
  public <RET extends ORecord> RET reload(final ORecord iRecord, final String iFetchPlan) {
    return reload(iRecord, iFetchPlan, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET reload(final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache) {
    return reload(iRecord, iFetchPlan, iIgnoreCache, true);
  }

  @Override
  public <RET extends ORecord> RET reload(ORecord record, String fetchPlan, boolean ignoreCache, boolean force) {
    checkIfActive();

    final ORecord loadedRecord = currentTx.reloadRecord(record.getIdentity(), record, fetchPlan, ignoreCache, force);

    if (loadedRecord != null && record != loadedRecord) {
      record.fromStream(loadedRecord.toStream());
      ORecordInternal.setVersion(record, loadedRecord.getVersion());
    } else if (loadedRecord == null) {
      throw new ORecordNotFoundException(record.getIdentity());
    }

    return (RET) record;
  }

  /**
   * Deletes the record without checking the version.
   */
  public ODatabaseDocument delete(final ORID iRecord) {
    checkOpenness();
    checkIfActive();

    final ORecord rec = load(iRecord);
    if (rec != null)
      delete(rec);
    return this;
  }

  @Override
  public boolean hide(ORID rid) {
    checkOpenness();
    checkIfActive();

    if (currentTx.isActive())
      throw new ODatabaseException("This operation can be executed only in non transaction mode");

    return executeHideRecord(rid, OPERATION_MODE.SYNCHRONOUS);
  }

  @Override
  public OBinarySerializerFactory getSerializerFactory() {
    return componentsFactory.binarySerializerFactory;
  }

  public ODatabaseDocument begin(final OTransaction iTx) {
    begin();
    return this;
  }

  public void rawBegin(final OTransaction iTx) {
    checkOpenness();
    checkIfActive();

    if (currentTx.isActive() && iTx.equals(currentTx)) {
      currentTx.begin();
    }

    currentTx.rollback(true, 0);

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxBegin(this);
      } catch (Exception e) {
        final String message = "Error before the transaction begin";

        OLogManager.instance().error(this, message, e);
        throw OException.wrapException(new OTransactionBlockedException(message), e);
      }

    currentTx = iTx;
    currentTx.begin();
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends ORecord> RET load(final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache) {
    return (RET) executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, -1, iFetchPlan, iIgnoreCache, !iIgnoreCache, false,
        OStorage.LOCKING_STRATEGY.NONE, new SimpleRecordReader(prefetchRecords));
  }

  @Override
  public void setPrefetchRecords(boolean prefetchRecords) {
    this.prefetchRecords = prefetchRecords;
  }

  @Override
  public boolean isPrefetchRecords() {
    return prefetchRecords;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public <RET extends ORecord> RET executeReadRecord(final ORecordId rid, ORecord iRecord, final int recordVersion,
      final String fetchPlan, final boolean ignoreCache, final boolean iUpdateCache, final boolean loadTombstones,
      final OStorage.LOCKING_STRATEGY lockingStrategy, RecordReader recordReader) {
    checkOpenness();
    checkIfActive();

    getMetadata().makeThreadLocalSchemaSnapshot();
    ORecordSerializationContext.pushContext();
    try {
      checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, getClusterNameById(rid.getClusterId()));

      // either regular or micro tx must be active or both inactive
      assert !(getTransaction().isActive() && (microTransaction != null && microTransaction.isActive()));

      // SEARCH IN LOCAL TX
      ORecord record = getTransaction().getRecord(rid);
      if (record == OBasicTransaction.DELETED_RECORD)
        // DELETED IN TX
        return null;

      if (record == null) {
        if (microTransaction != null && microTransaction.isActive()) {
          record = microTransaction.getRecord(rid);
          if (record == OBasicTransaction.DELETED_RECORD)
            return null;
        }
      }

      if (record == null && !ignoreCache)
        // SEARCH INTO THE CACHE
        record = getLocalCache().findRecord(rid);

      if (record != null) {
        if (iRecord != null) {
          iRecord.fromStream(record.toStream());
          ORecordInternal.setVersion(iRecord, record.getVersion());
          record = iRecord;
        }

        OFetchHelper.checkFetchPlanValid(fetchPlan);
        if (callbackHooks(ORecordHook.TYPE.BEFORE_READ, record) == ORecordHook.RESULT.SKIP)
          return null;

        if (record.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
          record.reload();

        if (lockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_SHARED_LOCK) {
          OLogManager.instance()
              .warn(this, "You use deprecated record locking strategy: %s it may lead to deadlocks " + lockingStrategy);
          record.lock(false);

        } else if (lockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK) {
          OLogManager.instance()
              .warn(this, "You use deprecated record locking strategy: %s it may lead to deadlocks " + lockingStrategy);
          record.lock(true);
        }

        callbackHooks(ORecordHook.TYPE.AFTER_READ, record);
        if (record instanceof ODocument)
          ODocumentInternal.checkClass((ODocument) record, this);
        return (RET) record;
      }

      final ORawBuffer recordBuffer;
      if (!rid.isValid())
        recordBuffer = null;
      else {
        OFetchHelper.checkFetchPlanValid(fetchPlan);

        int version;
        if (iRecord != null)
          version = iRecord.getVersion();
        else
          version = recordVersion;

        recordBuffer = recordReader.readRecord(getStorage(), rid, fetchPlan, ignoreCache, version);
      }

      if (recordBuffer == null)
        return null;

      if (iRecord == null || ORecordInternal.getRecordType(iRecord) != recordBuffer.recordType)
        // NO SAME RECORD TYPE: CAN'T REUSE OLD ONE BUT CREATE A NEW ONE FOR IT
        iRecord = Orient.instance().getRecordFactoryManager().newInstance(recordBuffer.recordType);

      ORecordInternal.fill(iRecord, rid, recordBuffer.version, recordBuffer.buffer, false);

      if (iRecord instanceof ODocument)
        ODocumentInternal.checkClass((ODocument) iRecord, this);

      if (ORecordVersionHelper.isTombstone(iRecord.getVersion()))
        return (RET) iRecord;

      if (callbackHooks(ORecordHook.TYPE.BEFORE_READ, iRecord) == ORecordHook.RESULT.SKIP)
        return null;

      iRecord.fromStream(recordBuffer.buffer);

      callbackHooks(ORecordHook.TYPE.AFTER_READ, iRecord);

      if (iUpdateCache)
        getLocalCache().updateRecord(iRecord);

      return (RET) iRecord;
    } catch (OOfflineClusterException t) {
      throw t;
    } catch (ORecordNotFoundException t) {
      throw t;
    } catch (Throwable t) {
      if (rid.isTemporary())
        throw OException.wrapException(new ODatabaseException("Error on retrieving record using temporary RID: " + rid), t);
      else
        throw OException.wrapException(new ODatabaseException(
            "Error on retrieving record " + rid + " (cluster: " + getStorage().getPhysicalClusterNameById(rid.getClusterId())
                + ")"), t);
    } finally {
      ORecordSerializationContext.pullContext();
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  public int assignAndCheckCluster(ORecord record, String iClusterName) {
    ORecordId rid = (ORecordId) record.getIdentity();
    // if provided a cluster name use it.
    if (rid.getClusterId() <= ORID.CLUSTER_POS_INVALID && iClusterName != null) {
      rid.setClusterId(getClusterIdByName(iClusterName));
      if (rid.getClusterId() == -1)
        throw new IllegalArgumentException("Cluster name '" + iClusterName + "' is not configured");

    }
    OClass schemaClass = null;
    // if cluster id is not set yet try to find it out
    if (rid.getClusterId() <= ORID.CLUSTER_ID_INVALID && getStorage().isAssigningClusterIds()) {
      if (record instanceof ODocument) {
        schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) record));
        if (schemaClass != null) {
          if (schemaClass.isAbstract())
            throw new OSchemaException("Document belongs to abstract class " + schemaClass.getName() + " and cannot be saved");
          rid.setClusterId(schemaClass.getClusterForNewInstance((ODocument) record));
        } else
          rid.setClusterId(getDefaultClusterId());
      } else {
        rid.setClusterId(getDefaultClusterId());
        if (record instanceof OBlob && rid.getClusterId() != ORID.CLUSTER_ID_INVALID) {
          // Set<Integer> blobClusters = getMetadata().getSchema().getBlobClusters();
          // if (!blobClusters.contains(rid.clusterId) && rid.clusterId != getDefaultClusterId() && rid.clusterId != 0) {
          // if (iClusterName == null)
          // iClusterName = getClusterNameById(rid.clusterId);
          // throw new IllegalArgumentException(
          // "Cluster name '" + iClusterName + "' (id=" + rid.clusterId + ") is not configured to store blobs, valid are "
          // + blobClusters.toString());
          // }
        }
      }
    } else if (record instanceof ODocument)
      schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) record));
    // If the cluster id was set check is validity
    if (rid.getClusterId() > ORID.CLUSTER_ID_INVALID) {
      if (schemaClass != null) {
        String messageClusterName = getClusterNameById(rid.getClusterId());
        checkRecordClass(schemaClass, messageClusterName, rid);
        if (!schemaClass.hasClusterId(rid.getClusterId())) {
          throw new IllegalArgumentException(
              "Cluster name '" + messageClusterName + "' (id=" + rid.getClusterId() + ") is not configured to store the class '"
                  + schemaClass.getName() + "', valid are " + Arrays.toString(schemaClass.getClusterIds()));
        }
      }
    }
    return rid.getClusterId();
  }

  public <RET extends ORecord> RET executeSaveEmptyRecord(ORecord record, String clusterName) {
    ORecordId rid = (ORecordId) record.getIdentity();
    assert rid.isNew();

    ORecordInternal.onBeforeIdentityChanged(record);
    int id = assignAndCheckCluster(record, clusterName);
    clusterName = getClusterNameById(id);
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_CREATE, clusterName);

    byte[] content = getSerializer().writeClassOnly(record);

    final OStorageOperationResult<OPhysicalPosition> ppos = getStorage()
        .createRecord(rid, content, record.getVersion(), recordType, OPERATION_MODE.SYNCHRONOUS.ordinal(), null);

    ORecordInternal.setVersion(record, ppos.getResult().recordVersion);
    ((ORecordId) record.getIdentity()).copyFrom(rid);
    ORecordInternal.onAfterIdentityChanged(record);

    return (RET) record;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public <RET extends ORecord> RET executeSaveRecord(final ORecord record, String clusterName, final int ver,
      final OPERATION_MODE mode, boolean forceCreate, final ORecordCallback<? extends Number> recordCreatedCallback,
      ORecordCallback<Integer> recordUpdatedCallback) {

    checkOpenness();
    checkIfActive();
    if (!record.isDirty())
      return (RET) record;

    final ORecordId rid = (ORecordId) record.getIdentity();

    if (rid == null)
      throw new ODatabaseException(
          "Cannot create record because it has no identity. Probably is not a regular record or contains projections of fields rather than a full record");

    if (supportsMicroTransactions(record)) {
      final OMicroTransaction microTx = beginMicroTransaction();
      if (microTx != null) {
        try {
          microTx.saveRecord(record, clusterName, mode, forceCreate, recordCreatedCallback, recordUpdatedCallback);
        } catch (Exception e) {
          endMicroTransaction(false);
          throw e;
        }
        endMicroTransaction(true);
        return (RET) record;
      }
    }

    record.setInternalStatus(ORecordElement.STATUS.MARSHALLING);
    try {

      byte[] stream = null;
      final OStorageOperationResult<Integer> operationResult;

      getMetadata().makeThreadLocalSchemaSnapshot();
      if (record instanceof ODocument)
        ODocumentInternal.checkClass((ODocument) record, this);
      ORecordSerializationContext.pushContext();
      final boolean isNew = forceCreate || rid.isNew();
      try {

        final ORecordHook.TYPE triggerType;
        if (isNew) {
          // NOTIFY IDENTITY HAS CHANGED
          ORecordInternal.onBeforeIdentityChanged(record);
          int id = assignAndCheckCluster(record, clusterName);
          clusterName = getClusterNameById(id);
          checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_CREATE, clusterName);
          triggerType = ORecordHook.TYPE.BEFORE_CREATE;
        } else {
          clusterName = getClusterNameById(record.getIdentity().getClusterId());
          checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_UPDATE, clusterName);
          triggerType = ORecordHook.TYPE.BEFORE_UPDATE;
        }
        stream = getSerializer().toStream(record, false);

        final ORecordHook.RESULT hookResult = callbackHooks(triggerType, record);

        if (hookResult == ORecordHook.RESULT.RECORD_CHANGED) {
          if (record instanceof ODocument)
            ((ODocument) record).validate();
          stream = updateStream(record);
        } else if (hookResult == ORecordHook.RESULT.SKIP_IO)
          return (RET) record;
        else if (hookResult == ORecordHook.RESULT.RECORD_REPLACED)
          // RETURNED THE REPLACED RECORD
          return (RET) OHookReplacedRecordThreadLocal.INSTANCE.get();

        ORecordSaveThreadLocal.setLast(record);
        try {
          // SAVE IT
          boolean updateContent = ORecordInternal.isContentChanged(record);
          byte[] content = (stream == null) ? OCommonConst.EMPTY_BYTE_ARRAY : stream;
          byte recordType = ORecordInternal.getRecordType(record);
          final int modeIndex = mode.ordinal();

          // CHECK IF RECORD TYPE IS SUPPORTED
          Orient.instance().getRecordFactoryManager().getRecordTypeClass(recordType);

          if (forceCreate || ORecordId.isNew(rid.getClusterPosition())) {
            // CREATE
            final OStorageOperationResult<OPhysicalPosition> ppos = getStorage()
                .createRecord(rid, content, ver, recordType, modeIndex, (ORecordCallback<Long>) recordCreatedCallback);
            operationResult = new OStorageOperationResult<Integer>(ppos.getResult().recordVersion, ppos.isMoved());

          } else {
            // UPDATE
            operationResult = getStorage()
                .updateRecord(rid, updateContent, content, ver, recordType, modeIndex, recordUpdatedCallback);
          }

          final int version = operationResult.getResult();

          if (isNew) {
            // UPDATE INFORMATION: CLUSTER ID+POSITION
            ((ORecordId) record.getIdentity()).copyFrom(rid);
            // NOTIFY IDENTITY HAS CHANGED
            ORecordInternal.onAfterIdentityChanged(record);
            // UPDATE INFORMATION: CLUSTER ID+POSITION
          }

          if (operationResult.getModifiedRecordContent() != null)
            stream = operationResult.getModifiedRecordContent();
          else if (version > record.getVersion() + 1 && getStorage() instanceof OStorageProxy)
            // IN CASE OF REMOTE CONFLICT STRATEGY FORCE UNLOAD DUE TO INVALID CONTENT
            record.unload();

          ORecordInternal.fill(record, rid, version, stream, false);

          callbackHookSuccess(record, isNew, stream, operationResult);
        } catch (Exception t) {
          callbackHookFailure(record, isNew, stream);
          throw t;
        }
      } finally {
        callbackHookFinalize(record, isNew, stream);
        ORecordSerializationContext.pullContext();
        getMetadata().clearThreadLocalSchemaSnapshot();
        ORecordSaveThreadLocal.removeLast();
      }

      if (stream != null && stream.length > 0 && !operationResult.isMoved())
        // ADD/UPDATE IT IN CACHE IF IT'S ACTIVE
        getLocalCache().updateRecord(record);
    } catch (OException e) {
      throw e;
    } catch (Exception t) {
      if (!ORecordId.isValid(record.getIdentity().getClusterPosition()))
        throw OException
            .wrapException(new ODatabaseException("Error on saving record in cluster #" + record.getIdentity().getClusterId()), t);
      else
        throw OException.wrapException(new ODatabaseException("Error on saving record " + record.getIdentity()), t);

    } finally {
      record.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
    return (RET) record;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public void executeDeleteRecord(OIdentifiable record, final int iVersion, final boolean iRequired, final OPERATION_MODE iMode,
      boolean prohibitTombstones) {
    checkOpenness();
    checkIfActive();

    final ORecordId rid = (ORecordId) record.getIdentity();

    if (rid == null)
      throw new ODatabaseException(
          "Cannot delete record because it has no identity. Probably was created from scratch or contains projections of fields rather than a full record");

    if (!rid.isValid())
      return;

    record = record.getRecord();
    if (record == null)
      return;

    final OMicroTransaction microTx = beginMicroTransaction();
    if (microTx != null) {
      try {
        microTx.deleteRecord(record.getRecord(), iMode);
      } catch (Exception e) {
        endMicroTransaction(false);
        throw e;
      }
      endMicroTransaction(true);
      return;
    }

    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_DELETE, getClusterNameById(rid.getClusterId()));

    ORecordSerializationContext.pushContext();
    getMetadata().makeThreadLocalSchemaSnapshot();
    try {
      if (record instanceof ODocument) {
        ODocumentInternal.checkClass((ODocument) record, this);
      }
      try {
        // if cache is switched off record will be unreachable after delete.
        ORecord rec = record.getRecord();
        if (rec != null) {
          callbackHooks(ORecordHook.TYPE.BEFORE_DELETE, rec);

          if (rec instanceof ODocument)
            ORidBagDeleter.deleteAllRidBags((ODocument) rec);
        }

        final OStorageOperationResult<Boolean> operationResult;
        try {
          if (prohibitTombstones) {
            final boolean result = getStorage().cleanOutRecord(rid, iVersion, iMode.ordinal(), null);
            if (!result && iRequired)
              throw new ORecordNotFoundException(rid);
            operationResult = new OStorageOperationResult<Boolean>(result);
          } else {
            final OStorageOperationResult<Boolean> result = getStorage().deleteRecord(rid, iVersion, iMode.ordinal(), null);
            if (!result.getResult() && iRequired)
              throw new ORecordNotFoundException(rid);
            operationResult = new OStorageOperationResult<Boolean>(result.getResult());
          }

          if (!operationResult.isMoved() && rec != null)
            callbackHooks(ORecordHook.TYPE.AFTER_DELETE, rec);
          else if (rec != null)
            callbackHooks(ORecordHook.TYPE.DELETE_REPLICATED, rec);
        } catch (Exception t) {
          callbackHooks(ORecordHook.TYPE.DELETE_FAILED, rec);
          throw t;
        } finally {
          callbackHooks(ORecordHook.TYPE.FINALIZE_DELETION, rec);
        }

        clearDocumentTracking(rec);

        // REMOVE THE RECORD FROM 1 AND 2 LEVEL CACHES
        if (!operationResult.isMoved()) {
          getLocalCache().deleteRecord(rid);
        }

      } catch (OException e) {
        // RE-THROW THE EXCEPTION
        throw e;

      } catch (Exception t) {
        // WRAP IT AS ODATABASE EXCEPTION
        throw OException
            .wrapException(new ODatabaseException("Error on deleting record in cluster #" + record.getIdentity().getClusterId()),
                t);
      }
    } finally {
      ORecordSerializationContext.pullContext();
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public boolean executeHideRecord(OIdentifiable record, final OPERATION_MODE iMode) {
    checkOpenness();
    checkIfActive();

    final ORecordId rid = (ORecordId) record.getIdentity();

    if (rid == null)
      throw new ODatabaseException(
          "Cannot hide record because it has no identity. Probably was created from scratch or contains projections of fields rather than a full record");

    if (!rid.isValid())
      return false;

    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_DELETE, getClusterNameById(rid.getClusterId()));

    getMetadata().makeThreadLocalSchemaSnapshot();
    if (record instanceof ODocument)
      ODocumentInternal.checkClass((ODocument) record, this);
    ORecordSerializationContext.pushContext();
    try {

      final OStorageOperationResult<Boolean> operationResult;
      operationResult = getStorage().hideRecord(rid, iMode.ordinal(), null);

      // REMOVE THE RECORD FROM 1 AND 2 LEVEL CACHES
      if (!operationResult.isMoved())
        getLocalCache().deleteRecord(rid);

      return operationResult.getResult();
    } finally {
      ORecordSerializationContext.pullContext();
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  public ODatabaseDocumentAbstract begin() {
    return begin(OTransaction.TXTYPE.OPTIMISTIC);
  }

  public ODatabaseDocumentAbstract begin(final OTransaction.TXTYPE iType) {
    checkOpenness();
    checkIfActive();

    if (currentTx.isActive()) {
      if (iType == OTransaction.TXTYPE.OPTIMISTIC && currentTx instanceof OTransactionOptimistic) {
        currentTx.begin();
        return this;
      }

      currentTx.rollback(true, 0);
    }

    // CHECK IT'S NOT INSIDE A HOOK
    if (!inHook.isEmpty())
      throw new IllegalStateException("Cannot begin a transaction while a hook is executing");

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxBegin(this);
      } catch (Throwable t) {
        OLogManager.instance().error(this, "Error before tx begin", t);
      }

    switch (iType) {
    case NOTX:
      setDefaultTransactionMode();
      break;

    case OPTIMISTIC:
      currentTx = new OTransactionOptimistic(this);
      break;

    case PESSIMISTIC:
      throw new UnsupportedOperationException("Pessimistic transaction");
    }

    currentTx.begin();
    return this;
  }

  public void setDefaultTransactionMode() {
    if (!(currentTx instanceof OTransactionNoTx))
      currentTx = new OTransactionNoTx(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void freeze(final boolean throwException) {
    checkOpenness();
    if (!(getStorage() instanceof OFreezableStorageComponent)) {
      OLogManager.instance().error(this,
          "Only local paginated storage supports freeze. If you are using remote client please use OServerAdmin instead");

      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    final OFreezableStorageComponent storage = getFreezableStorage();
    if (storage != null) {
      storage.freeze(throwException);
    }

    Orient.instance().getProfiler()
        .stopChrono("db." + getName() + ".freeze", "Time to freeze the database", startTime, "db.*.freeze");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void freeze() {
    checkOpenness();
    if (!(getStorage() instanceof OFreezableStorageComponent)) {
      OLogManager.instance().error(this,
          "Only local paginated storage supports freeze. " + "If you use remote client please use OServerAdmin instead");

      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    final OFreezableStorageComponent storage = getFreezableStorage();
    if (storage != null) {
      storage.freeze(false);
    }

    Orient.instance().getProfiler()
        .stopChrono("db." + getName() + ".freeze", "Time to freeze the database", startTime, "db.*.freeze");
  }

  @Override
  public boolean isFrozen() {
    if (!(getStorage() instanceof OFreezableStorageComponent))
      return false;

    final OFreezableStorageComponent storage = getFreezableStorage();
    if (storage != null)
      return storage.isFrozen();
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void release() {
    checkOpenness();
    if (!(getStorage() instanceof OFreezableStorageComponent)) {
      OLogManager.instance().error(this,
          "Only local paginated storage supports release. If you are using remote client please use OServerAdmin instead");
      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    final OFreezableStorageComponent storage = getFreezableStorage();
    if (storage != null) {
      storage.release();
    }

    Orient.instance().getProfiler()
        .stopChrono("db." + getName() + ".release", "Time to release the database", startTime, "db.*.release");
  }

  /**
   * Creates a new ODocument.
   */
  public ODocument newInstance() {
    return new ODocument();
  }

  @Override
  public OBlob newBlob(byte[] bytes) {
    return new ORecordBytes(bytes);
  }

  @Override
  public OBlob newBlob() {
    return new ORecordBytes();
  }

  /**
   * Creates a document with specific class.
   *
   * @param iClassName the name of class that should be used as a class of created document.
   *
   * @return new instance of document.
   */
  @Override
  public ODocument newInstance(final String iClassName) {
    return new ODocument(iClassName);
  }

  @Override
  public OElement newElement() {
    return newInstance();
  }

  @Override
  public OElement newElement(String className) {
    return newInstance(className);
  }

  public OElement newElement(OClass clazz) {
    return newInstance(clazz.getName());
  }

  public OVertex newVertex(final String iClassName) {
    ODocument doc = newInstance(iClassName);
    if (!doc.isVertex()) {
      throw new IllegalArgumentException("" + iClassName + " is not a vertex class");
    }
    return doc.asVertex().get();
  }

  @Override
  public OVertex newVertex(OClass type) {
    if (type == null) {
      return newVertex("E");
    }
    return newVertex(type.getName());
  }

  @Override
  public OEdge newEdge(OVertex from, OVertex to, String type) {
    ODocument doc = newInstance(type);
    if (!doc.isEdge()) {
      throw new IllegalArgumentException("" + type + " is not an edge class");
    }

    return addEdgeInternal(from, to, type);
  }

  @Override
  public OEdge newEdge(OVertex from, OVertex to, OClass type) {
    if (type == null) {
      return newEdge(from, to, "E");
    }
    return newEdge(from, to, type.getName());
  }

  private OEdge addEdgeInternal(final OVertex currentVertex, final OVertex inVertex, String iClassName, final Object... fields) {

    OEdge edge = null;
    ODocument outDocument = null;
    ODocument inDocument = null;
    boolean outDocumentModified = false;

    if (checkDeletedInTx(currentVertex))
      throw new ORecordNotFoundException(currentVertex.getIdentity(),
          "The vertex " + currentVertex.getIdentity() + " has been deleted");

    if (checkDeletedInTx(inVertex))
      throw new ORecordNotFoundException(inVertex.getIdentity(), "The vertex " + inVertex.getIdentity() + " has been deleted");

    final int maxRetries = 1;//TODO
    for (int retry = 0; retry < maxRetries; ++retry) {
      try {
        // TEMPORARY STATIC LOCK TO AVOID MT PROBLEMS AGAINST OMVRBTreeRID
        if (outDocument == null) {
          outDocument = currentVertex.getRecord();
          if (outDocument == null)
            throw new IllegalArgumentException("source vertex is invalid (rid=" + currentVertex.getIdentity() + ")");
        }

        if (inDocument == null) {
          inDocument = inVertex.getRecord();
          if (inDocument == null)
            throw new IllegalArgumentException("source vertex is invalid (rid=" + inVertex.getIdentity() + ")");
        }

        if (!ODocumentInternal.getImmutableSchemaClass(outDocument).isVertexType())
          throw new IllegalArgumentException("source record is not a vertex");

        if (!ODocumentInternal.getImmutableSchemaClass(outDocument).isVertexType())
          throw new IllegalArgumentException("destination record is not a vertex");

        OVertex to = inVertex;
        OVertex from = currentVertex;

        OSchema schema = getMetadata().getSchema();
        final OClass edgeType = schema.getClass(iClassName);
        if (edgeType == null)
          // AUTO CREATE CLASS
          schema.createClass(iClassName);
        else
          // OVERWRITE CLASS NAME BECAUSE ATTRIBUTES ARE CASE SENSITIVE
          iClassName = edgeType.getName();

        final String outFieldName = getConnectionFieldName(ODirection.OUT, iClassName);
        final String inFieldName = getConnectionFieldName(ODirection.IN, iClassName);

        // since the label for the edge can potentially get re-assigned
        // before being pushed into the OrientEdge, the
        // null check has to go here.
        if (iClassName == null)
          throw new IllegalArgumentException("Class " + iClassName + " cannot be found");

        // CREATE THE EDGE DOCUMENT TO STORE FIELDS TOO

        if (isUseLightweightEdges() && (fields == null || fields.length == 0)) {
          edge = newLightweightEdge(iClassName, from, to);
          OVertexDelegate.createLink(from.getRecord(), to.getRecord(), outFieldName);
          OVertexDelegate.createLink(to.getRecord(), from.getRecord(), inFieldName);
        } else {
          edge = newInstance(iClassName).asEdge().get();
          edge.setProperty("out", currentVertex.getRecord());
          edge.setProperty("in", inDocument.getRecord());

          if (fields != null) {
            for (int i = 0; i < fields.length; i += 2) {
              String fieldName = "" + fields[i];
              if (fields.length <= i + 1) {
                break;
              }
              Object fieldValue = fields[i + 1];
              edge.setProperty(fieldName, fieldValue);

            }
          }

          if (!outDocumentModified) {
            // OUT-VERTEX ---> IN-VERTEX/EDGE
            OVertexDelegate.createLink(outDocument, edge.getRecord(), outFieldName);

          }

          // IN-VERTEX ---> OUT-VERTEX/EDGE
          OVertexDelegate.createLink(inDocument, edge.getRecord(), inFieldName);

        }

        // OK
        break;

      } catch (ONeedRetryException e) {
        // RETRY
        if (!outDocumentModified)
          outDocument.reload();
        else if (inDocument != null)
          inDocument.reload();
      } catch (RuntimeException e) {
        // REVERT CHANGES. EDGE.REMOVE() TAKES CARE TO UPDATE ALSO BOTH VERTICES IN CASE
        try {
          edge.delete();
        } catch (Exception ex) {
        }
        throw e;
      } catch (Throwable e) {
        // REVERT CHANGES. EDGE.REMOVE() TAKES CARE TO UPDATE ALSO BOTH VERTICES IN CASE
        try {
          edge.delete();
        } catch (Exception ex) {
        }
        throw new IllegalStateException("Error on addEdge in non tx environment", e);
      }
    }
    return edge;
  }

  private boolean checkDeletedInTx(OVertex currentVertex) {
    ORID id;
    if (currentVertex.getRecord() != null)
      id = currentVertex.getRecord().getIdentity();
    else
      return false;

    final ORecordOperation oper = getTransaction().getRecordEntry(id);
    if (oper == null)
      return id.isTemporary();
    else
      return oper.type == ORecordOperation.DELETED;
  }

  private static String getConnectionFieldName(final ODirection iDirection, final String iClassName) {
    if (iDirection == null || iDirection == ODirection.BOTH)
      throw new IllegalArgumentException("Direction not valid");

    // PREFIX "out_" or "in_" TO THE FIELD NAME
    final String prefix = iDirection == ODirection.OUT ? "out_" : "in_";
    if (iClassName == null || iClassName.isEmpty() || iClassName.equals("E"))
      return prefix;

    return prefix + iClassName;
  }

  /**
   * {@inheritDoc}
   */
  public ORecordIteratorClass<ODocument> browseClass(final String iClassName) {
    return browseClass(iClassName, true);
  }

  /**
   * {@inheritDoc}
   */
  public ORecordIteratorClass<ODocument> browseClass(final String iClassName, final boolean iPolymorphic) {
    if (getMetadata().getImmutableSchemaSnapshot().getClass(iClassName) == null)
      throw new IllegalArgumentException("Class '" + iClassName + "' not found in current database");

    checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, iClassName);
    return new ORecordIteratorClass<ODocument>(this, this, iClassName, iPolymorphic, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ORecordIteratorCluster<ODocument> browseCluster(final String iClusterName) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<ODocument>(this, this, getClusterIdByName(iClusterName));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<ODatabaseListener> getListeners() {
    return getListenersCopy();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Deprecated
  public ORecordIteratorCluster<ODocument> browseCluster(String iClusterName, long startClusterPosition, long endClusterPosition,
      boolean loadTombstones) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<ODocument>(this, this, getClusterIdByName(iClusterName), startClusterPosition,
        endClusterPosition, loadTombstones, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  /**
   * Saves a document to the database. Behavior depends by the current running transaction if any. If no transaction is running then
   * changes apply immediately. If an Optimistic transaction is running then the record will be changed at commit time. The current
   * transaction will continue to see the record as modified, while others not. If a Pessimistic transaction is running, then an
   * exclusive lock is acquired against the record. Current transaction will continue to see the record as modified, while others
   * cannot access to it since it's locked.
   * <p>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown.Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   *
   * @param iRecord Record to save.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @throws OConcurrentModificationException if the version of the document is different by the version contained in the database.
   * @throws OValidationException             if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord) {
    return (RET) save(iRecord, null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves a document to the database. Behavior depends by the current running transaction if any. If no transaction is running then
   * changes apply immediately. If an Optimistic transaction is running then the record will be changed at commit time. The current
   * transaction will continue to see the record as modified, while others not. If a Pessimistic transaction is running, then an
   * exclusive lock is acquired against the record. Current transaction will continue to see the record as modified, while others
   * cannot access to it since it's locked.
   * <p>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown.Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   *
   * @param iRecord                Record to save.
   * @param iForceCreate           Flag that indicates that record should be created. If record with current rid already exists,
   *                               exception is thrown
   * @param iRecordCreatedCallback callback that is called after creation of new record
   * @param iRecordUpdatedCallback callback that is called after record update
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @throws OConcurrentModificationException if the version of the document is different by the version contained in the database.
   * @throws OValidationException             if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord, final OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<Integer> iRecordUpdatedCallback) {
    return save(iRecord, null, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  /**
   * Saves a document specifying a cluster where to store the record. Behavior depends by the current running transaction if any. If
   * no transaction is running then changes apply immediately. If an Optimistic transaction is running then the record will be
   * changed at commit time. The current transaction will continue to see the record as modified, while others not. If a Pessimistic
   * transaction is running, then an exclusive lock is acquired against the record. Current transaction will continue to see the
   * record as modified, while others cannot access to it since it's locked.
   * <p>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown. Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   *
   * @param iRecord      Record to save
   * @param iClusterName Cluster name where to save the record
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @throws OConcurrentModificationException if the version of the document is different by the version contained in the database.
   * @throws OValidationException             if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, ODocument#validate()
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord, final String iClusterName) {
    return (RET) save(iRecord, iClusterName, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves a document specifying a cluster where to store the record. Behavior depends by the current running transaction if any. If
   * no transaction is running then changes apply immediately. If an Optimistic transaction is running then the record will be
   * changed at commit time. The current transaction will continue to see the record as modified, while others not. If a Pessimistic
   * transaction is running, then an exclusive lock is acquired against the record. Current transaction will continue to see the
   * record as modified, while others cannot access to it since it's locked.
   * <p>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown. Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   *
   * @param iRecord                Record to save
   * @param iClusterName           Cluster name where to save the record
   * @param iMode                  Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate           Flag that indicates that record should be created. If record with current rid already exists,
   *                               exception is thrown
   * @param iRecordCreatedCallback callback that is called after creation of new record
   * @param iRecordUpdatedCallback callback that is called after record update
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @throws OConcurrentModificationException if the version of the document is different by the version contained in the database.
   * @throws OValidationException             if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, ODocument#validate()
   */
  @Override
  public <RET extends ORecord> RET save(ORecord iRecord, String iClusterName, final OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<Integer> iRecordUpdatedCallback) {
    checkOpenness();
    if (iRecord instanceof OVertexDelegate) {
      iRecord = iRecord.getRecord();
    }
    if (iRecord instanceof OEdgeDelegate) {
      iRecord = iRecord.getRecord();
    }
    if (!(iRecord instanceof ODocument)) {
      assignAndCheckCluster(iRecord, iClusterName);
      return (RET) currentTx.saveRecord(iRecord, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
    }

    ODocument doc = (ODocument) iRecord;
    ODocumentInternal.checkClass(doc, this);
    //  IN TX THE VALIDATION MAY BE RUN TWICE BUT IS CORRECT BECAUSE OF DIFFERENT RECORD STATUS
    try {
      doc.validate();
    } catch (OValidationException e) {
      doc.undo();
      throw e;
    }
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);

    if (iForceCreate || !doc.getIdentity().isValid()) {
      if (doc.getClassName() != null)
        checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_CREATE, doc.getClassName());

      assignAndCheckCluster(doc, iClusterName);

    } else {
      // UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
      if (doc.getClassName() != null)
        checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_UPDATE, doc.getClassName());
    }

    doc = (ODocument) currentTx
        .saveRecord(iRecord, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);

    return (RET) doc;
  }

  /**
   * Deletes a document. Behavior depends by the current running transaction if any. If no transaction is running then the record is
   * deleted immediately. If an Optimistic transaction is running then the record will be deleted at commit time. The current
   * transaction will continue to see the record as deleted, while others not. If a Pessimistic transaction is running, then an
   * exclusive lock is acquired against the record. Current transaction will continue to see the record as deleted, while others
   * cannot access to it since it's locked.
   * <p>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown.
   *
   * @param record record to delete
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  public ODatabaseDocumentAbstract delete(final ORecord record) {
    checkOpenness();
    if (record == null)
      throw new ODatabaseException("Cannot delete null document");
    if (record instanceof OVertexDelegate) {
      OVertexDelegate.deleteLinks((OVertexDelegate) record);
    } else if (record instanceof OEdgeDelegate) {
      OEdgeDelegate.deleteLinks((OEdgeDelegate) record);
    }

    // CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
    if (record instanceof ODocument && ((ODocument) record).getClassName() != null)
      checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_DELETE, ((ODocument) record).getClassName());

    try {
      currentTx.deleteRecord(record, OPERATION_MODE.SYNCHRONOUS);
    } catch (OException e) {
      throw e;
    } catch (Exception e) {
      if (record instanceof ODocument)
        throw OException.wrapException(new ODatabaseException(
            "Error on deleting record " + record.getIdentity() + " of class '" + ((ODocument) record).getClassName() + "'"), e);
      else
        throw OException.wrapException(new ODatabaseException("Error on deleting record " + record.getIdentity()), e);
    }
    return this;
  }

  /**
   * Returns the number of the records of the class iClassName.
   */
  public long countClass(final String iClassName) {
    return countClass(iClassName, true);
  }

  /**
   * Returns the number of the records of the class iClassName considering also sub classes if polymorphic is true.
   */
  public long countClass(final String iClassName, final boolean iPolymorphic) {
    final OClass cls = getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    if (cls == null)
      throw new IllegalArgumentException("Class '" + iClassName + "' not found in database");

    long totalOnDb = cls.count(iPolymorphic);

    long deletedInTx = 0;
    long addedInTx = 0;
    if (getTransaction().isActive())
      for (ORecordOperation op : getTransaction().getAllRecordEntries()) {
        if (op.type == ORecordOperation.DELETED) {
          final ORecord rec = op.getRecord();
          if (rec != null && rec instanceof ODocument) {
            OClass schemaClass = ((ODocument) rec).getSchemaClass();
            if (iPolymorphic) {
              if (schemaClass.isSubClassOf(iClassName))
                deletedInTx++;
            } else {
              if (iClassName.equals(schemaClass.getName()) || iClassName.equals(schemaClass.getShortName()))
                deletedInTx++;
            }
          }
        }
        if (op.type == ORecordOperation.CREATED) {
          final ORecord rec = op.getRecord();
          if (rec != null && rec instanceof ODocument) {
            OClass schemaClass = ((ODocument) rec).getSchemaClass();
            if (schemaClass != null) {
              if (iPolymorphic) {
                if (schemaClass.isSubClassOf(iClassName))
                  addedInTx++;
              } else {
                if (iClassName.equals(schemaClass.getName()) || iClassName.equals(schemaClass.getShortName()))
                  addedInTx++;
              }
            }
          }
        }
      }

    return (totalOnDb + addedInTx) - deletedInTx;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabase<ORecord> commit() {
    return commit(false);
  }

  @Override
  public ODatabaseDocument commit(boolean force) throws OTransactionException {
    checkOpenness();
    checkIfActive();

    if (!currentTx.isActive())
      return this;

    if (!force && currentTx.amountOfNestedTxs() > 1) {
      currentTx.commit();
      return this;
    }

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxCommit(this);
      } catch (Exception e) {
        rollback(force);

        OLogManager.instance().error(this, "Cannot commit the transaction: caught exception on execution of %s.onBeforeTxCommit()",
            listener.getClass().getName(), e);
        throw OException.wrapException(new OTransactionException(
            "Cannot commit the transaction: caught exception on execution of " + listener.getClass().getName()
                + "#onBeforeTxCommit()"), e);
      }

    try {
      currentTx.commit(force);
    } catch (RuntimeException e) {
      OLogManager.instance().debug(this, "Error on transaction commit", e);

      // WAKE UP ROLLBACK LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onBeforeTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error before transaction rollback", t);
        }

      // ROLLBACK TX AT DB LEVEL
      ((OTransactionAbstract) currentTx).internalRollback();
      getLocalCache().clear();

      // WAKE UP ROLLBACK LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onAfterTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error after transaction rollback", t);
        }
      throw e;
    }

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onAfterTxCommit(this);
      } catch (Exception e) {
        final String message =
            "Error after the transaction has been committed. The transaction remains valid. The exception caught was on execution of "
                + listener.getClass() + ".onAfterTxCommit()";

        OLogManager.instance().error(this, message, e);

        throw OException.wrapException(new OTransactionBlockedException(message), e);

      }

    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabase<ORecord> rollback() {
    return rollback(false);
  }

  @Override
  public ODatabaseDocument rollback(boolean force) throws OTransactionException {
    checkOpenness();
    if (currentTx.isActive()) {

      if (!force && currentTx.amountOfNestedTxs() > 1) {
        currentTx.rollback();
        return this;
      }

      // WAKE UP LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onBeforeTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error before transactional rollback", t);
        }

      currentTx.rollback(force, -1);

      // WAKE UP LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onAfterTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error after transaction rollback", t);
        }
    }

    getLocalCache().clear();

    return this;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  @Override
  public <DB extends ODatabase> DB getUnderlying() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> V callInLock(final Callable<V> iCallable, final boolean iExclusiveLock) {
    return getStorage().callInLock(iCallable, iExclusiveLock);
  }

  @Override
  public List<String> backup(final OutputStream out, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener, final int compressionLevel, final int bufferSize) throws IOException {
    checkOpenness();
    return getStorage().backup(out, options, callable, iListener, compressionLevel, bufferSize);
  }

  @Override
  public void restore(final InputStream in, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener) throws IOException {
    checkOpenness();

    getStorage().restore(in, options, callable, iListener);

    if (!isClosed()) {
      loadMetadata();
      sharedContext = null;
    }
  }

  /**
   * {@inheritDoc}
   */
  public OSBTreeCollectionManager getSbTreeCollectionManager() {
    return getStorage().getSBtreeCollectionManager();
  }

  @Override
  public OCurrentStorageComponentsFactory getStorageVersions() {
    return componentsFactory;
  }

  public ORecordSerializer getSerializer() {
    return serializer;
  }

  /**
   * Sets serializer for the database which will be used for document serialization.
   *
   * @param serializer the serializer to set.
   */
  public void setSerializer(ORecordSerializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public void resetInitialization() {
    for (ORecordHook h : hooks.keySet())
      h.onUnregister();

    hooks.clear();
    compileHooks();

    close();

    initialized = false;
  }

  @Override
  public String incrementalBackup(final String path) {
    checkOpenness();
    checkIfActive();

    return getStorage().incrementalBackup(path);
  }

  @Override
  @Deprecated
  public <DB extends ODatabaseDocument> DB checkSecurity(final String iResource, final int iOperation) {
    final String resourceSpecific = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric = ORule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*"))
      checkSecurity(resourceGeneric, null, iOperation);

    return checkSecurity(resourceGeneric, resourceSpecific, iOperation);
  }

  @Override
  @Deprecated
  public <DB extends ODatabaseDocument> DB checkSecurity(final String iResourceGeneric, final int iOperation,
      final Object iResourceSpecific) {
    final ORule.ResourceGeneric resourceGeneric = ORule.mapLegacyResourceToGenericResource(iResourceGeneric);
    if (iResourceSpecific == null || iResourceSpecific.equals("*"))
      return checkSecurity(resourceGeneric, iOperation, (Object) null);

    return checkSecurity(resourceGeneric, iOperation, iResourceSpecific);
  }

  @Override
  @Deprecated
  public <DB extends ODatabaseDocument> DB checkSecurity(final String iResourceGeneric, final int iOperation,
      final Object... iResourcesSpecific) {
    final ORule.ResourceGeneric resourceGeneric = ORule.mapLegacyResourceToGenericResource(iResourceGeneric);
    return checkSecurity(resourceGeneric, iOperation, iResourcesSpecific);
  }

  /**
   * @return <code>true</code> if database is obtained from the pool and <code>false</code> otherwise.
   */
  @Override
  public boolean isPooled() {
    return false;
  }

  /**
   * Use #activateOnCurrentThread instead.
   */
  @Deprecated
  public void setCurrentDatabaseInThreadLocal() {
    activateOnCurrentThread();
  }

  /**
   * Activates current database instance on current thread.
   */
  @Override
  public ODatabaseDocumentAbstract activateOnCurrentThread() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.INSTANCE;
    if (tl != null)
      tl.set(this);
    return this;
  }

  @Override
  public boolean isActiveOnCurrentThread() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.INSTANCE;
    final ODatabaseDocumentInternal db = tl != null ? tl.getIfDefined() : null;
    return db == this;
  }

  protected void checkOpenness() {
    if (status == STATUS.CLOSED)
      throw new ODatabaseException("Database '" + getURL() + "' is closed");
  }

  private void popInHook(OIdentifiable id) {
    inHook.remove(id);
  }

  private boolean pushInHook(OIdentifiable id) {
    return inHook.add(id);
  }

  private void clearCustomInternal() {
    getStorage().getConfiguration().clearProperties();
  }

  private void removeCustomInternal(final String iName) {
    setCustomInternal(iName, null);
  }

  private void setCustomInternal(final String iName, final String iValue) {
    final OStorage storage = getStorage();
    if (iValue == null || "null".equalsIgnoreCase(iValue))
      // REMOVE
      storage.getConfiguration().removeProperty(iName);
    else
      // SET
      storage.getConfiguration().setProperty(iName, iValue);

    storage.getConfiguration().update();
  }

  private void callbackHookFailure(ORecord record, boolean wasNew, byte[] stream) {
    if (stream != null && stream.length > 0)
      callbackHooks(wasNew ? ORecordHook.TYPE.CREATE_FAILED : ORecordHook.TYPE.UPDATE_FAILED, record);
  }

  private void callbackHookSuccess(final ORecord record, final boolean wasNew, final byte[] stream,
      final OStorageOperationResult<Integer> operationResult) {
    if (stream != null && stream.length > 0) {
      final ORecordHook.TYPE hookType;
      if (!operationResult.isMoved()) {
        hookType = wasNew ? ORecordHook.TYPE.AFTER_CREATE : ORecordHook.TYPE.AFTER_UPDATE;
      } else {
        hookType = wasNew ? ORecordHook.TYPE.CREATE_REPLICATED : ORecordHook.TYPE.UPDATE_REPLICATED;
      }
      callbackHooks(hookType, record);

    }
  }

  private void callbackHookFinalize(final ORecord record, final boolean wasNew, final byte[] stream) {
    if (stream != null && stream.length > 0) {
      final ORecordHook.TYPE hookType;
      hookType = wasNew ? ORecordHook.TYPE.FINALIZE_CREATION : ORecordHook.TYPE.FINALIZE_UPDATE;
      callbackHooks(hookType, record);

      clearDocumentTracking(record);
    }
  }

  private void clearDocumentTracking(final ORecord record) {
    if (record instanceof ODocument && ((ODocument) record).isTrackingChanges()) {
      ODocumentInternal.clearTrackData((ODocument) record);
    }
  }

  protected void checkRecordClass(final OClass recordClass, final String iClusterName, final ORecordId rid) {
    final OClass clusterIdClass = metadata.getImmutableSchemaSnapshot().getClassByClusterId(rid.getClusterId());
    if (recordClass == null && clusterIdClass != null || clusterIdClass == null && recordClass != null || (recordClass != null
        && !recordClass.equals(clusterIdClass)))
      throw new IllegalArgumentException(
          "Record saved into cluster '" + iClusterName + "' should be saved with class '" + clusterIdClass
              + "' but has been created with class '" + recordClass + "'");
  }

  private byte[] updateStream(final ORecord record) {
    ORecordSerializationContext.pullContext();

    ODirtyManager manager = ORecordInternal.getDirtyManager(record);
    Set<ORecord> newRecords = manager.getNewRecords();
    Set<ORecord> updatedRecords = manager.getUpdateRecords();
    manager.clearForSave();
    if (newRecords != null) {
      for (ORecord newRecord : newRecords) {
        if (newRecord != record)
          getTransaction().saveRecord(newRecord, null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
      }
    }
    if (updatedRecords != null) {
      for (ORecord updatedRecord : updatedRecords) {
        if (updatedRecord != record)
          getTransaction().saveRecord(updatedRecord, null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
      }
    }

    ORecordSerializationContext.pushContext();
    ORecordInternal.unsetDirty(record);
    record.setDirty();
    return serializer.toStream(record, false);
  }

  protected void init() {
    currentTx = new OTransactionNoTx(this);
  }

  private OFreezableStorageComponent getFreezableStorage() {
    OStorage s = getStorage();
    if (s instanceof OFreezableStorageComponent)
      return (OFreezableStorageComponent) s;
    else {
      OLogManager.instance().error(this, "Storage of type " + s.getType() + " does not support freeze operation");
      return null;
    }
  }

  public void checkIfActive() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.INSTANCE;
    ODatabaseDocumentInternal currentDatabase = tl != null ? tl.get() : null;
    if (currentDatabase instanceof ODatabaseDocumentTx) {
      currentDatabase = ((ODatabaseDocumentTx) currentDatabase).internal;
    }
    if (currentDatabase != this)
      throw new IllegalStateException(
          "The current database instance (" + toString() + ") is not active on the current thread (" + Thread.currentThread()
              + "). Current active database is: " + currentDatabase);
  }

  @Override
  public int addBlobCluster(final String iClusterName, final Object... iParameters) {
    int id;
    if (getStorage() instanceof OStorageProxy) {
      id = command(new OCommandSQL("create blob cluster :1")).execute(iClusterName);
      getMetadata().getSchema().reload();
    } else {
      if (!existsCluster(iClusterName)) {
        id = addCluster(iClusterName, iParameters);
      } else
        id = getClusterIdByName(iClusterName);
      getMetadata().getSchema().addBlobCluster(id);
    }
    return id;
  }

  public Set<Integer> getBlobClusterIds() {
    return getMetadata().getSchema().getBlobClusters();
  }

  private void compileHooks() {
    final List<ORecordHook>[] intermediateHooksByScope = new List[ORecordHook.SCOPE.values().length];
    for (ORecordHook.SCOPE scope : ORecordHook.SCOPE.values())
      intermediateHooksByScope[scope.ordinal()] = new ArrayList<>();

    for (ORecordHook hook : hooks.keySet())
      for (ORecordHook.SCOPE scope : hook.getScopes())
        intermediateHooksByScope[scope.ordinal()].add(hook);

    for (ORecordHook.SCOPE scope : ORecordHook.SCOPE.values()) {
      final int ordinal = scope.ordinal();
      final List<ORecordHook> scopeHooks = intermediateHooksByScope[ordinal];
      hooksByScope[ordinal] = scopeHooks.toArray(new ORecordHook[scopeHooks.size()]);
    }
  }

  @Override
  public OSharedContext getSharedContext() {
    // NOW NEED TO GET THE CONTEXT FROM RESOURCES IN FUTURE WILL BE NOT NEEDED
    if (sharedContext == null) {
      sharedContext = getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
        @Override
        public OSharedContext call() throws Exception {
          throw new ODatabaseException("Accessing to the database context before the database has bean initialized");
        }
      });
    }
    return sharedContext;
  }

  public static Object executeWithRetries(final OCallable<Object, Integer> callback, final int maxRetry) {
    return executeWithRetries(callback, maxRetry, 0, null);
  }

  public static Object executeWithRetries(final OCallable<Object, Integer> callback, final int maxRetry,
      final int waitBetweenRetry) {
    return executeWithRetries(callback, maxRetry, waitBetweenRetry, null);
  }

  public static Object executeWithRetries(final OCallable<Object, Integer> callback, final int maxRetry, final int waitBetweenRetry,
      final ORecord[] recordToReloadOnRetry) {
    ONeedRetryException lastException = null;
    for (int retry = 0; retry < maxRetry; ++retry) {
      try {
        return callback.call(retry);
      } catch (ONeedRetryException e) {
        // SAVE LAST EXCEPTION AND RETRY
        lastException = e;

        if (recordToReloadOnRetry != null) {
          // RELOAD THE RECORDS
          for (ORecord r : recordToReloadOnRetry)
            r.reload();
        }

        if (waitBetweenRetry > 0)
          try {
            Thread.sleep(waitBetweenRetry);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            break;
          }
      }
    }
    throw lastException;
  }

  private void bindPropertiesToContext(OContextConfiguration configuration, final Map<String, Object> iProperties) {
    final String connectionStrategy = iProperties != null ? (String) iProperties.get("connectionStrategy") : null;
    if (connectionStrategy != null)
      configuration.setValue(OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY, connectionStrategy);

    final String compressionMethod = iProperties != null ?
        (String) iProperties.get(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getKey().toLowerCase(Locale.ENGLISH)) :
        null;
    if (compressionMethod != null)
      // SAVE COMPRESSION METHOD IN CONFIGURATION
      configuration.setValue(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD, compressionMethod);

    final String encryptionMethod = iProperties != null ?
        (String) iProperties.get(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey().toLowerCase(Locale.ENGLISH)) :
        null;
    if (encryptionMethod != null)
      // SAVE ENCRYPTION METHOD IN CONFIGURATION
      configuration.setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD, encryptionMethod);

    final String encryptionKey = iProperties != null ?
        (String) iProperties.get(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey().toLowerCase(Locale.ENGLISH)) :
        null;
    if (encryptionKey != null)
      // SAVE ENCRYPTION KEY IN CONFIGURATION
      configuration.setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, encryptionKey);
  }

  private void bindPropertiesToContextGlobal(OContextConfiguration configuration,
      final Map<OGlobalConfiguration, Object> iProperties) {
    final String connectionStrategy = iProperties != null ? (String) iProperties.get("connectionStrategy") : null;
    if (connectionStrategy != null)
      configuration.setValue(OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY, connectionStrategy);

    final String compressionMethod =
        iProperties != null ? (String) iProperties.get(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD) : null;
    if (compressionMethod != null)
      // SAVE COMPRESSION METHOD IN CONFIGURATION
      configuration.setValue(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD, compressionMethod);

    final String encryptionMethod =
        iProperties != null ? (String) iProperties.get(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD) : null;
    if (encryptionMethod != null)
      // SAVE ENCRYPTION METHOD IN CONFIGURATION
      configuration.setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD, encryptionMethod);

    final String encryptionKey = iProperties != null ? (String) iProperties.get(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY) : null;
    if (encryptionKey != null)
      // SAVE ENCRYPTION KEY IN CONFIGURATION
      configuration.setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, encryptionKey);
  }

  public boolean isUseLightweightEdges() {
    final List<OStorageEntryConfiguration> custom = (List<OStorageEntryConfiguration>) this.get(ATTRIBUTES.CUSTOM);
    for (OStorageEntryConfiguration c : custom) {
      if (c.name.equals("useLightweightEdges"))
        return Boolean.parseBoolean(c.value);
    }
    return false;
  }

  public void setUseLightweightEdges(boolean b) {
    this.setCustom("useLightweightEdges", b);
  }

  public OEdge newLightweightEdge(String iClassName, OVertex from, OVertex to) {
    OClass clazz = getMetadata().getSchema().getClass(iClassName);
    OEdgeDelegate result = new OEdgeDelegate(from, to, clazz);

    return result;
  }

  protected boolean supportsMicroTransactions(ORecord record) {
    return true;
  }

  protected abstract OMicroTransaction beginMicroTransaction();

  private void endMicroTransaction(boolean success) {
    assert microTransaction != null;

    try {
      if (success)
        try {
          microTransaction.commit();
        } catch (Exception e) {
          microTransaction.rollbackAfterFailedCommit();
          throw e;
        }
      else
        microTransaction.rollback();
    } finally {
      if (!microTransaction.isActive())
        microTransaction = null;
    }
  }

  public void queryStarted(String id , OResultSet rs) {
    this.activeQueries.put(id, rs);
  }

  public void queryClosed(String id) {
    this.activeQueries.remove(id);
  }

  protected void closeActiveQueries() {
    while (activeQueries.size() > 0) {
      this.activeQueries.values().iterator().next().close();//the query automatically unregisters itself
    }
  }

  public OResultSet getActiveQuery(String id) {
    return activeQueries.get(id);
  }

  @Override
  public void internalCommit(OTransactionOptimistic transaction) {
    this.getStorage().commit(transaction, null);
  }
}