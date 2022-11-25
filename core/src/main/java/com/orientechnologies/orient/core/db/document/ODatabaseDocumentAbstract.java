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
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.*;
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
import com.orientechnologies.orient.core.metadata.schema.OImmutableView;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.*;
import com.orientechnologies.orient.core.record.impl.*;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageInfo;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Document API entrypoint.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public abstract class ODatabaseDocumentAbstract extends OListenerManger<ODatabaseListener>
    implements ODatabaseDocumentInternal {
  protected final Map<String, Object> properties = new HashMap<String, Object>();
  protected Map<ORecordHook, ORecordHook.HOOK_POSITION> unmodifiableHooks;
  protected final Set<OIdentifiable> inHook = new HashSet<OIdentifiable>();
  protected ORecordSerializer serializer;
  protected String url;
  protected STATUS status;
  protected OIntent currentIntent;
  protected ODatabaseInternal<?> databaseOwner;
  protected OMetadataDefault metadata;
  protected OImmutableUser user;
  protected final byte recordType = ODocument.RECORD_TYPE;
  protected final Map<ORecordHook, ORecordHook.HOOK_POSITION> hooks =
      new LinkedHashMap<ORecordHook, ORecordHook.HOOK_POSITION>();
  protected boolean retainRecords = true;
  protected OLocalRecordCache localCache;
  protected OCurrentStorageComponentsFactory componentsFactory;
  protected boolean initialized = false;
  protected OTransaction currentTx;

  protected final ORecordHook[][] hooksByScope =
      new ORecordHook[ORecordHook.SCOPE.values().length][];
  protected OSharedContext sharedContext;

  private boolean prefetchRecords;

  protected Map<String, OResultSet> activeQueries = new ConcurrentHashMap<>();
  private Map<UUID, OBonsaiCollectionPointer> collectionsChanges;

  // database stats!
  protected long loadedRecordsCount;
  protected long totalRecordLoadMs;
  protected long minRecordLoadMs;
  protected long maxRecordLoadMs;
  protected long ridbagPrefetchCount;
  protected long totalRidbagPrefetchMs;
  protected long minRidbagPrefetchMs;
  protected long maxRidbagPrefetchMs;

  protected ODatabaseDocumentAbstract() {
    // DO NOTHING IS FOR EXTENDED OBJECTS
    super(false);
  }

  /**
   * @return default serializer which is used to serialize documents. Default serializer is common
   *     for all database instances.
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
    wakeupOnOpenDbLifecycleListeners();
    wakeupOnOpenListeners();
  }

  protected abstract void loadMetadata();

  public void callOnCloseListeners() {
    wakeupOnCloseDbLifecycleListeners();
    wakeupOnCloseListeners();
  }

  private void wakeupOnOpenDbLifecycleListeners() {
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners();
        it.hasNext(); ) {
      it.next().onOpen(getDatabaseOwner());
    }
  }

  private void wakeupOnOpenListeners() {
    for (ODatabaseListener listener : getListenersCopy()) {
      try {
        listener.onOpen(getDatabaseOwner());
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during call of database listener", e);
      }
    }
  }

  private void wakeupOnCloseDbLifecycleListeners() {
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners();
        it.hasNext(); ) {
      it.next().onClose(getDatabaseOwner());
    }
  }

  private void wakeupOnCloseListeners() {
    for (ODatabaseListener listener : getListenersCopy()) {
      try {
        listener.onClose(getDatabaseOwner());
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during call of database listener", e);
      }
    }
  }

  public void callOnDropListeners() {
    wakeupOnDropListeners();
  }

  private void wakeupOnDropListeners() {
    for (ODatabaseListener listener : getListenersCopy()) {
      try {
        activateOnCurrentThread();
        listener.onDelete(getDatabaseOwner());
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during call of database listener", e);
      }
    }
  }

  /** {@inheritDoc} */
  public <RET extends ORecord> RET getRecord(final OIdentifiable iIdentifiable) {
    if (iIdentifiable instanceof ORecord) return (RET) iIdentifiable;
    return load(iIdentifiable.getIdentity());
  }

  /** {@inheritDoc} */
  public <RET extends ORecord> RET load(
      final ORID iRecordId, final String iFetchPlan, final boolean iIgnoreCache) {
    return executeReadRecord(
        (ORecordId) iRecordId,
        null,
        -1,
        iFetchPlan,
        iIgnoreCache,
        !iIgnoreCache,
        false,
        OStorage.LOCKING_STRATEGY.DEFAULT,
        new SimpleRecordReader(prefetchRecords));
  }

  /** Deletes the record checking the version. */
  public ODatabase<ORecord> delete(final ORID iRecord, final int iVersion) {
    final ORecord record = load(iRecord);
    ORecordInternal.setVersion(record, iVersion);
    delete(record);
    return this;
  }

  public ODatabaseDocumentInternal cleanOutRecord(final ORID iRecord, final int iVersion) {
    delete(iRecord, iVersion);
    return this;
  }

  public String getType() {
    return TYPE;
  }

  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
      final String iClusterName, final Class<REC> iClass) {
    return (ORecordIteratorCluster<REC>) browseCluster(iClusterName);
  }

  /** {@inheritDoc} */
  @Override
  @Deprecated
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
      final String iClusterName,
      final Class<REC> iRecordClass,
      final long startClusterPosition,
      final long endClusterPosition,
      final boolean loadTombstones) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    checkIfActive();
    final int clusterId = getClusterIdByName(iClusterName);
    return new ORecordIteratorCluster<REC>(
        this,
        clusterId,
        startClusterPosition,
        endClusterPosition,
        OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @Override
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    checkIfActive();
    final int clusterId = getClusterIdByName(iClusterName);
    return new ORecordIteratorCluster<REC>(
        this, clusterId, startClusterPosition, endClusterPosition);
  }

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
  public <RET extends List<?>> RET query(final OQuery<?> iCommand, final Object... iArgs) {
    checkIfActive();
    iCommand.reset();
    return iCommand.execute(iArgs);
  }

  /** {@inheritDoc} */
  public byte getRecordType() {
    return recordType;
  }

  /** {@inheritDoc} */
  @Override
  public long countClusterElements(final int[] iClusterIds) {
    return countClusterElements(iClusterIds, false);
  }

  /** {@inheritDoc} */
  @Override
  public long countClusterElements(final int iClusterId) {
    return countClusterElements(iClusterId, false);
  }

  /** {@inheritDoc} */
  @Override
  public void truncateCluster(String clusterName) {
    command("truncate cluster " + clusterName).close();
  }

  /** {@inheritDoc} */
  public OMetadataDefault getMetadata() {
    checkOpenness();
    return metadata;
  }

  /** {@inheritDoc} */
  @Override
  public ODatabaseInternal<?> getDatabaseOwner() {
    ODatabaseInternal<?> current = databaseOwner;
    while (current != null && current != this && current.getDatabaseOwner() != current) {
      current = current.getDatabaseOwner();
    }
    return current;
  }

  /** {@inheritDoc} */
  @Override
  public ODatabaseInternal<ORecord> setDatabaseOwner(ODatabaseInternal<?> iOwner) {
    databaseOwner = iOwner;
    return this;
  }

  /** {@inheritDoc} */
  public boolean isRetainRecords() {
    return retainRecords;
  }

  /** {@inheritDoc} */
  public ODatabaseDocument setRetainRecords(boolean retainRecords) {
    this.retainRecords = retainRecords;
    return this;
  }

  /** {@inheritDoc} */
  public <DB extends ODatabase> DB setStatus(final STATUS status) {
    checkIfActive();
    setStatusInternal(status);
    return (DB) this;
  }

  public void setStatusInternal(final STATUS status) {
    this.status = status;
  }

  /** {@inheritDoc} */
  public void setInternal(final ATTRIBUTES iAttribute, final Object iValue) {
    set(iAttribute, iValue);
  }

  /** {@inheritDoc} */
  public OSecurityUser getUser() {
    return user;
  }

  /** {@inheritDoc} */
  public void setUser(final OSecurityUser user) {
    checkIfActive();
    if (user instanceof OUser) {
      final OMetadata metadata = getMetadata();
      if (metadata != null) {
        final OSecurityInternal security = sharedContext.getSecurity();
        this.user = new OImmutableUser(security.getVersion(this), user);
      } else this.user = new OImmutableUser(-1, user);
    } else this.user = (OImmutableUser) user;
  }

  public void reloadUser() {
    if (user != null) {
      activateOnCurrentThread();
      if (user.checkIfAllowed(ORule.ResourceGeneric.CLASS, OUser.CLASS_NAME, ORole.PERMISSION_READ)
          != null) {
        OMetadata metadata = getMetadata();
        if (metadata != null) {
          final OSecurityInternal security = sharedContext.getSecurity();
          final OUser secGetUser = security.getUser(this, user.getName());

          if (secGetUser != null) {
            user = new OImmutableUser(security.getVersion(this), secGetUser);
          } else {
            user = new OImmutableUser(-1, new OUser());
          }
        } else {
          user = new OImmutableUser(-1, new OUser());
        }
      }
    }
  }

  /** {@inheritDoc} */
  public boolean isMVCC() {
    return true;
  }

  /** {@inheritDoc} */
  public <DB extends ODatabase<?>> DB setMVCC(boolean mvcc) {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Deprecated
  public ODictionary<ORecord> getDictionary() {
    checkOpenness();
    return metadata.getIndexManagerInternal().getDictionary(this);
  }

  /** {@inheritDoc} */
  public <DB extends ODatabase<?>> DB registerHook(
      final ORecordHook iHookImpl, final ORecordHook.HOOK_POSITION iPosition) {
    checkOpenness();
    checkIfActive();

    final Map<ORecordHook, ORecordHook.HOOK_POSITION> tmp =
        new LinkedHashMap<ORecordHook, ORecordHook.HOOK_POSITION>(hooks);
    tmp.put(iHookImpl, iPosition);
    hooks.clear();
    for (ORecordHook.HOOK_POSITION p : ORecordHook.HOOK_POSITION.values()) {
      for (Map.Entry<ORecordHook, ORecordHook.HOOK_POSITION> e : tmp.entrySet()) {
        if (e.getValue() == p) hooks.put(e.getKey(), e.getValue());
      }
    }
    compileHooks();
    return (DB) this;
  }

  /** {@inheritDoc} */
  public <DB extends ODatabase<?>> DB registerHook(final ORecordHook iHookImpl) {
    return registerHook(iHookImpl, ORecordHook.HOOK_POSITION.REGULAR);
  }

  /** {@inheritDoc} */
  public <DB extends ODatabase<?>> DB unregisterHook(final ORecordHook iHookImpl) {
    checkIfActive();
    if (iHookImpl != null) {
      iHookImpl.onUnregister();
      hooks.remove(iHookImpl);
      compileHooks();
    }
    return (DB) this;
  }

  /** {@inheritDoc} */
  @Override
  public OLocalRecordCache getLocalCache() {
    return localCache;
  }

  /** {@inheritDoc} */
  public Map<ORecordHook, ORecordHook.HOOK_POSITION> getHooks() {
    return unmodifiableHooks;
  }

  /**
   * Callback the registered hooks if any.
   *
   * @param type Hook type. Define when hook is called.
   * @param id Record received in the callback
   * @return True if the input record is changed, otherwise false
   */
  public ORecordHook.RESULT callbackHooks(final ORecordHook.TYPE type, final OIdentifiable id) {
    if (id == null || hooks.isEmpty() || id.getIdentity().getClusterId() == 0)
      return ORecordHook.RESULT.RECORD_NOT_CHANGED;

    final ORecordHook.SCOPE scope = ORecordHook.SCOPE.typeToScope(type);
    final int scopeOrdinal = scope.ordinal();

    final ORID identity = id.getIdentity().copy();
    if (!pushInHook(identity)) return ORecordHook.RESULT.RECORD_NOT_CHANGED;

    try {
      final ORecord rec = id.getRecord();
      if (rec == null) return ORecordHook.RESULT.RECORD_NOT_CHANGED;

      final OScenarioThreadLocal.RUN_MODE runMode = OScenarioThreadLocal.INSTANCE.getRunMode();

      boolean recordChanged = false;
      for (ORecordHook hook : hooksByScope[scopeOrdinal]) {
        switch (runMode) {
          case DEFAULT: // NON_DISTRIBUTED OR PROXIED DB
            if (isDistributed()
                && hook.getDistributedExecutionMode()
                    == ORecordHook.DISTRIBUTED_EXECUTION_MODE.TARGET_NODE)
              // SKIP
              continue;
            break; // TARGET NODE
          case RUNNING_DISTRIBUTED:
            if (hook.getDistributedExecutionMode()
                == ORecordHook.DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE) continue;
        }

        final ORecordHook.RESULT res = hook.onTrigger(type, rec);

        if (res == ORecordHook.RESULT.RECORD_CHANGED) recordChanged = true;
        else if (res == ORecordHook.RESULT.SKIP_IO)
          // SKIP IO OPERATION
          return res;
        else if (res == ORecordHook.RESULT.SKIP)
          // SKIP NEXT HOOKS AND RETURN IT
          return res;
        else if (res == ORecordHook.RESULT.RECORD_REPLACED) return res;
      }
      return recordChanged
          ? ORecordHook.RESULT.RECORD_CHANGED
          : ORecordHook.RESULT.RECORD_NOT_CHANGED;
    } finally {
      popInHook(identity);
    }
  }

  /** {@inheritDoc} */
  public boolean isValidationEnabled() {
    return (Boolean) get(ATTRIBUTES.VALIDATION);
  }

  /** {@inheritDoc} */
  public <DB extends ODatabaseDocument> DB setValidationEnabled(final boolean iEnabled) {
    set(ATTRIBUTES.VALIDATION, iEnabled);
    return (DB) this;
  }

  @Override
  public OContextConfiguration getConfiguration() {
    checkIfActive();
    if (getStorageInfo() != null)
      return getStorageInfo().getConfiguration().getContextConfiguration();
    return null;
  }

  @Override
  public boolean declareIntent(final OIntent intent) {
    checkIfActive();
    if (currentIntent != null) {
      if (intent != null && intent.getClass().equals(currentIntent.getClass())) {
        // SAME INTENT: JUMP IT
        return false;
      }
      // END CURRENT INTENT
      currentIntent.end(this);
    }
    currentIntent = intent;

    if (intent != null) {
      intent.begin(this);
    }
    return true;
  }

  @Override
  public OIntent getActiveIntent() {
    return currentIntent;
  }

  @Override
  public void close() {
    internalClose(false);
  }

  @Override
  public STATUS getStatus() {
    return status;
  }

  @Override
  public String getName() {
    return getStorageInfo() != null ? getStorageInfo().getName() : url;
  }

  @Override
  public String getURL() {
    return url != null ? url : getStorageInfo().getURL();
  }

  @Override
  public int getDefaultClusterId() {
    checkIfActive();
    return getStorageInfo().getDefaultClusterId();
  }

  @Override
  public int getClusters() {
    checkIfActive();
    return getStorageInfo().getClusters();
  }

  @Override
  public boolean existsCluster(final String iClusterName) {
    checkIfActive();
    return getStorageInfo().getClusterNames().contains(iClusterName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Collection<String> getClusterNames() {
    checkIfActive();
    return getStorageInfo().getClusterNames();
  }

  @Override
  public int getClusterIdByName(final String iClusterName) {
    if (iClusterName == null) return -1;

    checkIfActive();
    return getStorageInfo().getClusterIdByName(iClusterName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public String getClusterNameById(final int iClusterId) {
    if (iClusterId < 0) return null;

    checkIfActive();
    return getStorageInfo().getPhysicalClusterNameById(iClusterId);
  }

  public void checkForClusterPermissions(final String iClusterName) {
    // CHECK FOR ORESTRICTED
    final Set<OClass> classes =
        getMetadata().getImmutableSchemaSnapshot().getClassesRelyOnCluster(iClusterName);
    for (OClass c : classes) {
      if (c.isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME))
        throw new OSecurityException(
            "Class '"
                + c.getName()
                + "' cannot be truncated because has record level security enabled (extends '"
                + OSecurityShared.RESTRICTED_CLASSNAME
                + "')");
    }
  }

  @Override
  public Object setProperty(final String iName, final Object iValue) {
    if (iValue == null) return properties.remove(iName.toLowerCase(Locale.ENGLISH));
    else return properties.put(iName.toLowerCase(Locale.ENGLISH), iValue);
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

    if (iAttribute == null) throw new IllegalArgumentException("attribute is null");
    final OStorageInfo storage = getStorageInfo();
    switch (iAttribute) {
      case STATUS:
        return getStatus();
      case DEFAULTCLUSTERID:
        return getDefaultClusterId();
      case TYPE:
        return getMetadata().getImmutableSchemaSnapshot().existsClass("V") ? "graph" : "document";
      case DATEFORMAT:
        return storage.getConfiguration().getDateFormat();

      case DATETIMEFORMAT:
        return storage.getConfiguration().getDateTimeFormat();

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

  public OTransaction getTransaction() {
    checkIfActive();
    return currentTx;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORecord iRecord, final String iFetchPlan) {
    checkIfActive();
    return (RET)
        currentTx.loadRecord(
            iRecord.getIdentity(),
            iRecord,
            iFetchPlan,
            false,
            false,
            OStorage.LOCKING_STRATEGY.DEFAULT);
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
  public <RET extends ORecord> RET loadIfVersionIsNotLatest(
      final ORID rid, final int recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException {
    checkIfActive();
    return (RET)
        currentTx.loadRecordIfVersionIsNotLatest(rid, recordVersion, fetchPlan, ignoreCache);
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
  public <RET extends ORecord> RET reload(
      final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache) {
    return reload(iRecord, iFetchPlan, iIgnoreCache, true);
  }

  @Override
  public <RET extends ORecord> RET reload(
      ORecord record, String fetchPlan, boolean ignoreCache, boolean force) {
    checkIfActive();

    final ORecord loadedRecord =
        currentTx.reloadRecord(record.getIdentity(), record, fetchPlan, ignoreCache, force);

    if (loadedRecord != null && record != loadedRecord) {
      record.fromStream(loadedRecord.toStream());
      ORecordInternal.setVersion(record, loadedRecord.getVersion());
    } else if (loadedRecord == null) {
      throw new ORecordNotFoundException(record.getIdentity());
    }

    return (RET) record;
  }

  /** Deletes the record without checking the version. */
  public ODatabaseDocument delete(final ORID iRecord) {
    checkOpenness();
    checkIfActive();

    final ORecord rec = load(iRecord);
    if (rec != null) delete(rec);
    return this;
  }

  @Override
  public OBinarySerializerFactory getSerializerFactory() {
    return componentsFactory.binarySerializerFactory;
  }

  @Deprecated
  public ODatabaseDocument begin(final OTransaction iTx) {
    begin();
    return this;
  }

  public OTransaction swapTx(OTransaction newTx) {
    OTransaction old = getTransaction();
    currentTx = newTx;
    return old;
  }

  public void rawBegin(final OTransaction iTx) {
    checkOpenness();
    checkIfActive();

    if (currentTx.isActive() && iTx.equals(currentTx)) {
      currentTx.begin();
      return;
    }
    Map<ORID, OTransactionAbstract.LockedRecordMetadata> noTxLockedRecords = null;

    if (!currentTx.isActive() && iTx instanceof OTransactionOptimistic) {
      noTxLockedRecords = ((OTransactionAbstract) currentTx).getInternalLocks();
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
    if (iTx instanceof OTransactionOptimistic) {
      ((OTransactionOptimistic) iTx).setNoTxLocks(noTxLockedRecords);
    }
    currentTx.begin();
  }

  /** {@inheritDoc} */
  public <RET extends ORecord> RET load(
      final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache) {
    return executeReadRecord(
        (ORecordId) iRecord.getIdentity(),
        iRecord,
        -1,
        iFetchPlan,
        iIgnoreCache,
        !iIgnoreCache,
        false,
        OStorage.LOCKING_STRATEGY.NONE,
        new SimpleRecordReader(prefetchRecords));
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
   * This method is internal, it can be subject to signature change or be removed, do not
   * use. @Internal
   */
  public abstract <RET extends ORecord> RET executeReadRecord(
      final ORecordId rid,
      ORecord iRecord,
      final int recordVersion,
      final String fetchPlan,
      final boolean ignoreCache,
      final boolean iUpdateCache,
      final boolean loadTombstones,
      final OStorage.LOCKING_STRATEGY lockingStrategy,
      RecordReader recordReader);

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
    if (rid.getClusterId() <= ORID.CLUSTER_ID_INVALID && getStorageInfo().isAssigningClusterIds()) {
      if (record instanceof ODocument) {
        schemaClass = ODocumentInternal.getImmutableSchemaClass(this, ((ODocument) record));
        if (schemaClass != null) {
          if (schemaClass.isAbstract())
            throw new OSchemaException(
                "Document belongs to abstract class "
                    + schemaClass.getName()
                    + " and cannot be saved");
          rid.setClusterId(schemaClass.getClusterForNewInstance((ODocument) record));
        } else
          throw new ODatabaseException(
              "Cannot save (1) document " + record + ": no class or cluster defined");
      } else {
        if (record instanceof ORecordBytes) {
          Set<Integer> blobs = getBlobClusterIds();
          if (blobs.size() == 0) {
            rid.setClusterId(getDefaultClusterId());
          } else {
            rid.setClusterId(blobs.iterator().next());
          }
        } else {
          throw new ODatabaseException(
              "Cannot save (3) document " + record + ": no class or cluster defined");
        }
      }
    } else if (record instanceof ODocument)
      schemaClass = ODocumentInternal.getImmutableSchemaClass(this, ((ODocument) record));
    // If the cluster id was set check is validity
    if (rid.getClusterId() > ORID.CLUSTER_ID_INVALID) {
      if (schemaClass != null) {
        String messageClusterName = getClusterNameById(rid.getClusterId());
        checkRecordClass(schemaClass, messageClusterName, rid);
        if (!schemaClass.hasClusterId(rid.getClusterId())) {
          throw new IllegalArgumentException(
              "Cluster name '"
                  + messageClusterName
                  + "' (id="
                  + rid.getClusterId()
                  + ") is not configured to store the class '"
                  + schemaClass.getName()
                  + "', valid are "
                  + Arrays.toString(schemaClass.getClusterIds()));
        }
      }
    }
    return rid.getClusterId();
  }

  public ODatabaseDocumentAbstract begin() {
    return begin(OTransaction.TXTYPE.OPTIMISTIC);
  }

  public ODatabaseDocumentAbstract begin(final OTransaction.TXTYPE iType) {
    checkOpenness();
    checkIfActive();

    // CHECK IT'S NOT INSIDE A HOOK
    if (!inHook.isEmpty())
      throw new IllegalStateException("Cannot begin a transaction while a hook is executing");

    if (currentTx.isActive()) {
      if (iType == OTransaction.TXTYPE.OPTIMISTIC && currentTx instanceof OTransactionOptimistic) {
        currentTx.begin();
        return this;
      }
      currentTx.rollback(true, 0);
    }

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxBegin(this);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error before tx begin", e);
      }

    switch (iType) {
      case NOTX:
        setDefaultTransactionMode(null);
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

  public void setDefaultTransactionMode(
      Map<ORID, OTransactionAbstract.LockedRecordMetadata> noTxLocks) {
    if (!(currentTx instanceof OTransactionNoTx)) currentTx = new OTransactionNoTx(this, noTxLocks);
  }

  /** Creates a new ODocument. */
  public ODocument newInstance() {
    return new ODocument(this);
  }

  @Override
  public OBlob newBlob(byte[] bytes) {
    return new ORecordBytes(this, bytes);
  }

  @Override
  public OBlob newBlob() {
    return new ORecordBytes(this);
  }

  /**
   * Creates a document with specific class.
   *
   * @param iClassName the name of class that should be used as a class of created document.
   * @return new instance of document.
   */
  @Override
  public ODocument newInstance(final String iClassName) {
    return new ODocument(this, iClassName);
  }

  @Override
  public OElement newEmbeddedElement() {
    return new ODocumentEmbedded(this);
  }

  @Override
  public OElement newEmbeddedElement(String className) {
    return new ODocumentEmbedded(className, this);
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
    return new OVertexDocument(this, iClassName);
  }

  private OEdge newEdgeInternal(final String iClassName) {
    return new OEdgeDocument(this, iClassName);
  }

  @Override
  public OVertex newVertex(OClass type) {
    if (type == null) {
      return newVertex("V");
    }
    return newVertex(type.getName());
  }

  @Override
  public OEdge newEdge(OVertex from, OVertex to, String type) {
    OClass cl = getMetadata().getImmutableSchemaSnapshot().getClass(type);
    if (cl == null || !cl.isEdgeType()) {
      throw new IllegalArgumentException("" + type + " is not an edge class");
    }
    return addEdgeInternal(from, to, type, false);
  }

  @Override
  public OEdge newEdge(OVertex from, OVertex to, OClass type) {
    if (type == null) {
      return newEdge(from, to, "E");
    }
    return newEdge(from, to, type.getName());
  }

  private OEdge addEdgeInternal(
      final OVertex currentVertex,
      final OVertex inVertex,
      String iClassName,
      boolean forceRegular,
      final Object... fields) {
    if (currentVertex == null) throw new IllegalArgumentException("To vertex is null");
    if (inVertex == null) throw new IllegalArgumentException("To vertex is null");
    OEdge edge = null;
    ODocument outDocument = null;
    ODocument inDocument = null;
    boolean outDocumentModified = false;

    if (checkDeletedInTx(currentVertex))
      throw new ORecordNotFoundException(
          currentVertex.getIdentity(),
          "The vertex " + currentVertex.getIdentity() + " has been deleted");

    if (checkDeletedInTx(inVertex))
      throw new ORecordNotFoundException(
          inVertex.getIdentity(), "The vertex " + inVertex.getIdentity() + " has been deleted");

    final int maxRetries = 1; // TODO
    for (int retry = 0; retry < maxRetries; ++retry) {
      try {
        // TEMPORARY STATIC LOCK TO AVOID MT PROBLEMS AGAINST OMVRBTreeRID
        if (outDocument == null) {
          outDocument = currentVertex.getRecord();
          if (outDocument == null)
            throw new IllegalArgumentException(
                "source vertex is invalid (rid=" + currentVertex.getIdentity() + ")");
        }

        if (inDocument == null) {
          inDocument = inVertex.getRecord();
          if (inDocument == null)
            throw new IllegalArgumentException(
                "source vertex is invalid (rid=" + inVertex.getIdentity() + ")");
        }

        if (!ODocumentInternal.getImmutableSchemaClass(this, outDocument).isVertexType())
          throw new IllegalArgumentException("source record is not a vertex");

        if (!ODocumentInternal.getImmutableSchemaClass(this, outDocument).isVertexType())
          throw new IllegalArgumentException("destination record is not a vertex");

        OVertex to = inVertex;
        OVertex from = currentVertex;

        OSchema schema = getMetadata().getImmutableSchemaSnapshot();
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

        boolean regularEdgeBasedOnSchema =
            hasLinkedClass(from, outFieldName, edgeType)
                || hasLinkedClass(to, inFieldName, edgeType);

        if (isUseLightweightEdges()
            && !regularEdgeBasedOnSchema
            && (fields == null || fields.length == 0)
            && !forceRegular) {
          edge = newLightweightEdge(iClassName, from, to);
          OVertexDelegate.createLink(from.getRecord(), to.getRecord(), outFieldName);
          OVertexDelegate.createLink(to.getRecord(), from.getRecord(), inFieldName);
        } else {
          edge = newEdgeInternal(iClassName);
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

      } catch (ONeedRetryException ignore) {
        // RETRY
        if (!outDocumentModified) outDocument.reload();
        else if (inDocument != null) inDocument.reload();
      }
    }
    return edge;
  }

  private boolean hasLinkedClass(OVertex vertex, String edgeFieldName, OClass linkedClass) {
    Optional<OClass> clazz = vertex.getSchemaType();
    if (!clazz.isPresent()) {
      return false;
    }
    OProperty prop = clazz.get().getProperty(edgeFieldName);
    if (prop == null) {
      return false;
    }
    if (prop.getLinkedClass() == null) {
      return false;
    }
    return linkedClass.isSubClassOf(prop.getLinkedClass());
  }

  private boolean checkDeletedInTx(OVertex currentVertex) {
    ORID id;
    if (currentVertex.getRecord() != null) id = currentVertex.getRecord().getIdentity();
    else return false;

    final ORecordOperation oper = getTransaction().getRecordEntry(id);
    if (oper == null) return id.isTemporary();
    else return oper.type == ORecordOperation.DELETED;
  }

  private static String getConnectionFieldName(
      final ODirection iDirection, final String iClassName) {
    if (iDirection == null || iDirection == ODirection.BOTH)
      throw new IllegalArgumentException("Direction not valid");

    // PREFIX "out_" or "in_" TO THE FIELD NAME
    final String prefix = iDirection == ODirection.OUT ? "out_" : "in_";
    if (iClassName == null || iClassName.isEmpty() || iClassName.equals("E")) return prefix;

    return prefix + iClassName;
  }

  /** {@inheritDoc} */
  public ORecordIteratorClass<ODocument> browseClass(final String iClassName) {
    return browseClass(iClassName, true);
  }

  /** {@inheritDoc} */
  public ORecordIteratorClass<ODocument> browseClass(
      final String iClassName, final boolean iPolymorphic) {
    if (getMetadata().getImmutableSchemaSnapshot().getClass(iClassName) == null)
      throw new IllegalArgumentException(
          "Class '" + iClassName + "' not found in current database");

    checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, iClassName);
    return new ORecordIteratorClass<ODocument>(this, iClassName, iPolymorphic, false);
  }

  /** {@inheritDoc} */
  @Override
  public ORecordIteratorCluster<ODocument> browseCluster(final String iClusterName) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<ODocument>(this, getClusterIdByName(iClusterName));
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<ODatabaseListener> getListeners() {
    return getListenersCopy();
  }

  /** {@inheritDoc} */
  @Override
  @Deprecated
  public ORecordIteratorCluster<ODocument> browseCluster(
      String iClusterName,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<ODocument>(
        this,
        getClusterIdByName(iClusterName),
        startClusterPosition,
        endClusterPosition,
        OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  /**
   * Saves a document to the database. Behavior depends by the current running transaction if any.
   * If no transaction is running then changes apply immediately. If an Optimistic transaction is
   * running then the record will be changed at commit time. The current transaction will continue
   * to see the record as modified, while others not. If a Pessimistic transaction is running, then
   * an exclusive lock is acquired against the record. Current transaction will continue to see the
   * record as modified, while others cannot access to it since it's locked.
   *
   * <p>If MVCC is enabled and the version of the document is different by the version stored in the
   * database, then a {@link OConcurrentModificationException} exception is thrown.Before to save
   * the document it must be valid following the constraints declared in the schema if any (can work
   * also in schema-less mode). To validate the document the {@link ODocument#validate()} is called.
   *
   * @param iRecord Record to save.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   * @throws OConcurrentModificationException if the version of the document is different by the
   *     version contained in the database.
   * @throws OValidationException if the document breaks some validation constraints defined in the
   *     schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord) {
    return save(iRecord, null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves a document to the database. Behavior depends by the current running transaction if any.
   * If no transaction is running then changes apply immediately. If an Optimistic transaction is
   * running then the record will be changed at commit time. The current transaction will continue
   * to see the record as modified, while others not. If a Pessimistic transaction is running, then
   * an exclusive lock is acquired against the record. Current transaction will continue to see the
   * record as modified, while others cannot access to it since it's locked.
   *
   * <p>If MVCC is enabled and the version of the document is different by the version stored in the
   * database, then a {@link OConcurrentModificationException} exception is thrown.Before to save
   * the document it must be valid following the constraints declared in the schema if any (can work
   * also in schema-less mode). To validate the document the {@link ODocument#validate()} is called.
   *
   * @param iRecord Record to save.
   * @param iForceCreate Flag that indicates that record should be created. If record with current
   *     rid already exists, exception is thrown
   * @param iRecordCreatedCallback callback that is called after creation of new record
   * @param iRecordUpdatedCallback callback that is called after record update
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   * @throws OConcurrentModificationException if the version of the document is different by the
   *     version contained in the database.
   * @throws OValidationException if the document breaks some validation constraints defined in the
   *     schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends ORecord> RET save(
      final ORecord iRecord,
      final OPERATION_MODE iMode,
      boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<Integer> iRecordUpdatedCallback) {
    return save(iRecord, null, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  /**
   * Saves a document specifying a cluster where to store the record. Behavior depends by the
   * current running transaction if any. If no transaction is running then changes apply
   * immediately. If an Optimistic transaction is running then the record will be changed at commit
   * time. The current transaction will continue to see the record as modified, while others not. If
   * a Pessimistic transaction is running, then an exclusive lock is acquired against the record.
   * Current transaction will continue to see the record as modified, while others cannot access to
   * it since it's locked.
   *
   * <p>If MVCC is enabled and the version of the document is different by the version stored in the
   * database, then a {@link OConcurrentModificationException} exception is thrown. Before to save
   * the document it must be valid following the constraints declared in the schema if any (can work
   * also in schema-less mode). To validate the document the {@link ODocument#validate()} is called.
   *
   * @param iRecord Record to save
   * @param iClusterName Cluster name where to save the record
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   * @throws OConcurrentModificationException if the version of the document is different by the
   *     version contained in the database.
   * @throws OValidationException if the document breaks some validation constraints defined in the
   *     schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, ODocument#validate()
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord, final String iClusterName) {
    return save(iRecord, iClusterName, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves a document specifying a cluster where to store the record. Behavior depends by the
   * current running transaction if any. If no transaction is running then changes apply
   * immediately. If an Optimistic transaction is running then the record will be changed at commit
   * time. The current transaction will continue to see the record as modified, while others not. If
   * a Pessimistic transaction is running, then an exclusive lock is acquired against the record.
   * Current transaction will continue to see the record as modified, while others cannot access to
   * it since it's locked.
   *
   * <p>If MVCC is enabled and the version of the document is different by the version stored in the
   * database, then a {@link OConcurrentModificationException} exception is thrown. Before to save
   * the document it must be valid following the constraints declared in the schema if any (can work
   * also in schema-less mode). To validate the document the {@link ODocument#validate()} is called.
   *
   * @param iRecord Record to save
   * @param iClusterName Cluster name where to save the record
   * @param iMode Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate Flag that indicates that record should be created. If record with current
   *     rid already exists, exception is thrown
   * @param iRecordCreatedCallback callback that is called after creation of new record
   * @param iRecordUpdatedCallback callback that is called after record update
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   * @throws OConcurrentModificationException if the version of the document is different by the
   *     version contained in the database.
   * @throws OValidationException if the document breaks some validation constraints defined in the
   *     schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, ODocument#validate()
   */
  @Override
  public <RET extends ORecord> RET save(
      ORecord iRecord,
      String iClusterName,
      final OPERATION_MODE iMode,
      boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<Integer> iRecordUpdatedCallback) {
    checkOpenness();

    if (iRecord instanceof OVertex) {
      iRecord = iRecord.getRecord();
    }
    if (iRecord instanceof OEdge) {
      if (((OEdge) iRecord).isLightweight()) {
        iRecord = ((OEdge) iRecord).getFrom();
      } else {
        iRecord = iRecord.getRecord();
      }
    }
    return saveInternal(
        iRecord, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  private <RET extends ORecord> RET saveInternal(
      ORecord iRecord,
      String iClusterName,
      OPERATION_MODE iMode,
      boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<Integer> iRecordUpdatedCallback) {

    if (!(iRecord instanceof ODocument)) {
      assignAndCheckCluster(iRecord, iClusterName);
      return (RET)
          currentTx.saveRecord(
              iRecord,
              iClusterName,
              iMode,
              iForceCreate,
              iRecordCreatedCallback,
              iRecordUpdatedCallback);
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

    if (!getSerializer().equals(ORecordInternal.getRecordSerializer(doc))) {
      ORecordInternal.setRecordSerializer(doc, getSerializer());
    }
    doc =
        (ODocument)
            currentTx.saveRecord(
                iRecord,
                iClusterName,
                iMode,
                iForceCreate,
                iRecordCreatedCallback,
                iRecordUpdatedCallback);

    return (RET) doc;
  }

  /** Returns the number of the records of the class iClassName. */
  public long countView(final String viewName) {
    final OImmutableView cls =
        (OImmutableView) getMetadata().getImmutableSchemaSnapshot().getView(viewName);
    if (cls == null) throw new IllegalArgumentException("View '" + cls + "' not found in database");

    return countClass(cls, false);
  }

  /** Returns the number of the records of the class iClassName. */
  public long countClass(final String iClassName) {
    return countClass(iClassName, true);
  }

  /**
   * Returns the number of the records of the class iClassName considering also sub classes if
   * polymorphic is true.
   */
  public long countClass(final String iClassName, final boolean iPolymorphic) {
    final OImmutableClass cls =
        (OImmutableClass) getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);
    if (cls == null)
      throw new IllegalArgumentException("Class '" + cls + "' not found in database");

    return countClass(cls, iPolymorphic);
  }

  protected long countClass(final OImmutableClass cls, final boolean iPolymorphic) {

    long totalOnDb = cls.countImpl(iPolymorphic);

    long deletedInTx = 0;
    long addedInTx = 0;
    String className = cls.getName();
    if (getTransaction().isActive())
      for (ORecordOperation op : getTransaction().getRecordOperations()) {
        if (op.type == ORecordOperation.DELETED) {
          final ORecord rec = op.getRecord();
          if (rec != null && rec instanceof ODocument) {
            OClass schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) rec));
            if (iPolymorphic) {
              if (schemaClass.isSubClassOf(className)) deletedInTx++;
            } else {
              if (className.equals(schemaClass.getName())
                  || className.equals(schemaClass.getShortName())) deletedInTx++;
            }
          }
        }
        if (op.type == ORecordOperation.CREATED) {
          final ORecord rec = op.getRecord();
          if (rec != null && rec instanceof ODocument) {
            OClass schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) rec));
            if (schemaClass != null) {
              if (iPolymorphic) {
                if (schemaClass.isSubClassOf(className)) addedInTx++;
              } else {
                if (className.equals(schemaClass.getName())
                    || className.equals(schemaClass.getShortName())) addedInTx++;
              }
            }
          }
        }
      }

    return (totalOnDb + addedInTx) - deletedInTx;
  }

  /** {@inheritDoc} */
  @Override
  public ODatabase<ORecord> commit() {
    return commit(false);
  }

  @Override
  public ODatabaseDocument commit(boolean force) throws OTransactionException {
    checkOpenness();
    checkIfActive();

    if (!currentTx.isActive()) return this;

    if (!force && currentTx.amountOfNestedTxs() > 1) {
      // This just do count down no real commit here
      currentTx.commit();
      return this;
    }

    // WAKE UP LISTENERS

    try {
      beforeCommitOperations();
    } catch (OException e) {
      try {
        rollback(force);
      } catch (Exception re) {
        OLogManager.instance()
            .error(this, "Exception during rollback `%08X`", re, System.identityHashCode(re));
      }
      throw e;
    }
    try {
      currentTx.commit(force);
    } catch (RuntimeException e) {

      if ((e instanceof OHighLevelException) || (e instanceof ONeedRetryException))
        OLogManager.instance()
            .debug(this, "Error on transaction commit `%08X`", e, System.identityHashCode(e));
      else
        OLogManager.instance()
            .error(this, "Error on transaction commit `%08X`", e, System.identityHashCode(e));

      // WAKE UP ROLLBACK LISTENERS
      beforeRollbackOperations();

      try {
        // ROLLBACK TX AT DB LEVEL
        ((OTransactionAbstract) currentTx).internalRollback();
      } catch (Exception re) {
        OLogManager.instance()
            .error(
                this, "Error during transaction rollback `%08X`", re, System.identityHashCode(re));
      }

      // WAKE UP ROLLBACK LISTENERS
      afterRollbackOperations();
      throw e;
    }

    // WAKE UP LISTENERS
    afterCommitOperations();

    return this;
  }

  protected void beforeCommitOperations() {
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxCommit(this);
      } catch (Exception e) {
        OLogManager.instance()
            .error(
                this,
                "Cannot commit the transaction: caught exception on execution of %s.onBeforeTxCommit() `%08X`",
                e,
                listener.getClass().getName(),
                System.identityHashCode(e));
        throw OException.wrapException(
            new OTransactionException(
                "Cannot commit the transaction: caught exception on execution of "
                    + listener.getClass().getName()
                    + "#onBeforeTxCommit()"),
            e);
      }
  }

  protected void afterCommitOperations() {
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onAfterTxCommit(this);
      } catch (Exception e) {
        final String message =
            "Error after the transaction has been committed. The transaction remains valid. The exception caught was on execution of "
                + listener.getClass()
                + ".onAfterTxCommit() `%08X`";

        OLogManager.instance().error(this, message, e, System.identityHashCode(e));

        throw OException.wrapException(new OTransactionBlockedException(message), e);
      }
  }

  protected void beforeRollbackOperations() {
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxRollback(this);
      } catch (Exception t) {
        OLogManager.instance()
            .error(this, "Error before transaction rollback `%08X`", t, System.identityHashCode(t));
      }
  }

  protected void afterRollbackOperations() {
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onAfterTxRollback(this);
      } catch (Exception t) {
        OLogManager.instance()
            .error(this, "Error after transaction rollback `%08X`", t, System.identityHashCode(t));
      }
  }

  /** {@inheritDoc} */
  @Override
  public ODatabase<ORecord> rollback() {
    return rollback(false);
  }

  @Override
  public ODatabaseDocument rollback(boolean force) throws OTransactionException {
    checkOpenness();
    if (currentTx.isActive()) {

      if (!force && currentTx.amountOfNestedTxs() > 1) {
        // This just decrement the counter no real rollback here
        currentTx.rollback();
        return this;
      }

      // WAKE UP LISTENERS
      beforeRollbackOperations();
      currentTx.rollback(force, -1);
      // WAKE UP LISTENERS
      afterRollbackOperations();
    }
    return this;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not
   * use. @Internal
   */
  @Override
  public <DB extends ODatabase> DB getUnderlying() {
    throw new UnsupportedOperationException();
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
    for (ORecordHook h : hooks.keySet()) h.onUnregister();

    hooks.clear();
    compileHooks();

    close();

    initialized = false;
  }

  public void checkSecurity(final int operation, final OIdentifiable record, String cluster) {
    if (cluster == null) {
      cluster = getClusterNameById(record.getIdentity().getClusterId());
    }
    checkSecurity(ORule.ResourceGeneric.CLUSTER, operation, cluster);

    if (record instanceof ODocument) {
      String clazzName = ((ODocument) record).getClassName();
      if (clazzName != null) {
        checkSecurity(ORule.ResourceGeneric.CLASS, operation, clazzName);
      }
    }
  }

  /**
   * @return <code>true</code> if database is obtained from the pool and <code>false</code>
   *     otherwise.
   */
  @Override
  public boolean isPooled() {
    return false;
  }

  /** Use #activateOnCurrentThread instead. */
  @Deprecated
  public void setCurrentDatabaseInThreadLocal() {
    activateOnCurrentThread();
  }

  /** Activates current database instance on current thread. */
  @Override
  public ODatabaseDocumentAbstract activateOnCurrentThread() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.instance();
    if (tl != null) tl.set(this);
    return this;
  }

  @Override
  public boolean isActiveOnCurrentThread() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.instance();
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

  protected void callbackHookFailure(ORecord record, boolean wasNew, byte[] stream) {
    if (stream != null && stream.length > 0)
      callbackHooks(
          wasNew ? ORecordHook.TYPE.CREATE_FAILED : ORecordHook.TYPE.UPDATE_FAILED, record);
  }

  protected void callbackHookSuccess(
      final ORecord record,
      final boolean wasNew,
      final byte[] stream,
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

  protected void callbackHookFinalize(
      final ORecord record, final boolean wasNew, final byte[] stream) {
    if (stream != null && stream.length > 0) {
      final ORecordHook.TYPE hookType;
      hookType = wasNew ? ORecordHook.TYPE.FINALIZE_CREATION : ORecordHook.TYPE.FINALIZE_UPDATE;
      callbackHooks(hookType, record);

      clearDocumentTracking(record);
    }
  }

  protected void clearDocumentTracking(final ORecord record) {
    if (record instanceof ODocument && ((ODocument) record).isTrackingChanges()) {
      ODocumentInternal.clearTrackData((ODocument) record);
    }
  }

  protected void checkRecordClass(
      final OClass recordClass, final String iClusterName, final ORecordId rid) {
    final OClass clusterIdClass =
        metadata.getImmutableSchemaSnapshot().getClassByClusterId(rid.getClusterId());
    if (recordClass == null && clusterIdClass != null
        || clusterIdClass == null && recordClass != null
        || (recordClass != null && !recordClass.equals(clusterIdClass)))
      throw new IllegalArgumentException(
          "Record saved into cluster '"
              + iClusterName
              + "' should be saved with class '"
              + clusterIdClass
              + "' but has been created with class '"
              + recordClass
              + "'");
  }

  protected void init() {
    currentTx = new OTransactionNoTx(this, null);
  }

  public void checkIfActive() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.instance();
    ODatabaseDocumentInternal currentDatabase = tl != null ? tl.get() : null;
    if (currentDatabase instanceof ODatabaseDocumentTx) {
      currentDatabase = ((ODatabaseDocumentTx) currentDatabase).internal;
    }
    if (currentDatabase != this)
      throw new IllegalStateException(
          "The current database instance ("
              + toString()
              + ") is not active on the current thread ("
              + Thread.currentThread()
              + "). Current active database is: "
              + currentDatabase);
  }

  public Set<Integer> getBlobClusterIds() {
    return getMetadata().getSchema().getBlobClusters();
  }

  private void compileHooks() {
    final List<ORecordHook>[] intermediateHooksByScope =
        new List[ORecordHook.SCOPE.values().length];
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
    return sharedContext;
  }

  public static Object executeWithRetries(
      final OCallable<Object, Integer> callback, final int maxRetry) {
    return executeWithRetries(callback, maxRetry, 0, null);
  }

  public static Object executeWithRetries(
      final OCallable<Object, Integer> callback, final int maxRetry, final int waitBetweenRetry) {
    return executeWithRetries(callback, maxRetry, waitBetweenRetry, null);
  }

  public static Object executeWithRetries(
      final OCallable<Object, Integer> callback,
      final int maxRetry,
      final int waitBetweenRetry,
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
          for (ORecord r : recordToReloadOnRetry) r.reload();
        }

        if (waitBetweenRetry > 0)
          try {
            Thread.sleep(waitBetweenRetry);
          } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
            break;
          }
      }
    }
    throw lastException;
  }

  public boolean isUseLightweightEdges() {
    final List<OStorageEntryConfiguration> custom =
        (List<OStorageEntryConfiguration>) this.get(ATTRIBUTES.CUSTOM);
    for (OStorageEntryConfiguration c : custom) {
      if (c.name.equals("useLightweightEdges")) return Boolean.parseBoolean(c.value);
    }
    return false;
  }

  public void setUseLightweightEdges(boolean b) {
    this.setCustom("useLightweightEdges", b);
  }

  public OEdge newLightweightEdge(String iClassName, OVertex from, OVertex to) {
    OClass clazz = getMetadata().getSchema().getClass(iClassName);
    OEdgeDelegate result = new OEdgeDelegate(from, to, clazz, iClassName);

    return result;
  }

  public OEdge newRegularEdge(String iClassName, OVertex from, OVertex to) {
    OClass cl = getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);
    if (cl == null || !cl.isEdgeType()) {
      throw new IllegalArgumentException("" + iClassName + " is not an edge class");
    }
    return addEdgeInternal(from, to, iClassName, true);
  }

  public synchronized void queryStarted(String id, OResultSet rs) {
    if (this.activeQueries.size() > 1 && this.activeQueries.size() % 10 == 0) {
      StringBuilder msg = new StringBuilder();
      msg.append("This database instance has ");
      msg.append(activeQueries.size());
      msg.append(
          " open command/query result sets, please make sure you close them with OResultSet.close()");
      OLogManager.instance().warn(this, msg.toString(), null);
      if (OLogManager.instance().isDebugEnabled()) {
        activeQueries.values().stream()
            .map(pendingQuery -> pendingQuery.getExecutionPlan())
            .filter(plan -> plan != null)
            .forEach(plan -> OLogManager.instance().debug(this, plan.toString()));
      }
    }
    this.activeQueries.put(id, rs);

    getListeners().forEach((it) -> it.onCommandStart(this, rs));
  }

  public void queryClosed(String id) {
    OResultSet removed = this.activeQueries.remove(id);
    getListeners().forEach((it) -> it.onCommandEnd(this, removed));
  }

  protected synchronized void closeActiveQueries() {
    while (activeQueries.size() > 0) {
      this.activeQueries
          .values()
          .iterator()
          .next()
          .close(); // the query automatically unregisters itself
    }
  }

  public Map<String, OResultSet> getActiveQueries() {
    return activeQueries;
  }

  public OResultSet getActiveQuery(String id) {
    return activeQueries.get(id);
  }

  @Override
  public boolean isClusterEdge(int cluster) {
    OClass clazz = getMetadata().getImmutableSchemaSnapshot().getClassByClusterId(cluster);
    return clazz != null && clazz.isEdgeType();
  }

  @Override
  public boolean isClusterVertex(int cluster) {
    OClass clazz = getMetadata().getImmutableSchemaSnapshot().getClassByClusterId(cluster);
    return clazz != null && clazz.isVertexType();
  }

  @Override
  public boolean isClusterView(int cluster) {
    OView view = getViewFromCluster(cluster);
    return view != null;
  }

  public OView getViewFromCluster(int cluster) {
    return getMetadata().getImmutableSchemaSnapshot().getViewByClusterId(cluster);
  }

  protected void pessimisticLockChecks(ORID recordId) {
    ORecord record = getTransaction().getRecord(recordId);
    if (record != null && record.isDirty()) {
      throw new ODatabaseException("Impossible to lock a record modified in transaction");
    }
    if (!recordId.isPersistent()) {
      throw new ODatabaseException("Impossible to lock an not persistent record");
    }
  }

  public Map<UUID, OBonsaiCollectionPointer> getCollectionsChanges() {
    if (collectionsChanges == null) {
      collectionsChanges = new HashMap<>();
    }
    return collectionsChanges;
  }
}
