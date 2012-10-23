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
package com.orientechnologies.orient.core.db.record;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLevel1RecordCache;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODataSegmentStrategy;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseWrapperAbstract;
import com.orientechnologies.orient.core.db.ODefaultDataSegmentStrategy;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.hook.OHookThreadLocal;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.RESULT;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORestrictedAccessHook;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.OUserTrigger;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.tx.OTransactionRealAbstract;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

@SuppressWarnings("unchecked")
public abstract class ODatabaseRecordAbstract extends ODatabaseWrapperAbstract<ODatabaseRaw> implements ODatabaseRecord {

  private OMetadata                       metadata;
  private OUser                           user;
  private static final String             DEF_RECORD_FORMAT   = "csv";
  private byte                            recordType;
  private String                          recordFormat;
  private Set<ORecordHook>                hooks               = new HashSet<ORecordHook>();
  private final Set<ORecordHook>          unmodifiableHooks;
  private boolean                         retainRecords       = true;
  private OLevel1RecordCache              level1Cache;
  private boolean                         mvcc;
  private boolean                         validation;
  private ODictionary<ORecordInternal<?>> dictionary;
  private ODataSegmentStrategy            dataSegmentStrategy = new ODefaultDataSegmentStrategy();

  public ODatabaseRecordAbstract(final String iURL, final byte iRecordType) {
    super(new ODatabaseRaw(iURL));
    setCurrentDatabaseinThreadLocal();

    underlying.setOwner(this);

    unmodifiableHooks = Collections.unmodifiableSet(hooks);

    databaseOwner = this;

    recordType = iRecordType;
    level1Cache = new OLevel1RecordCache();

    mvcc = OGlobalConfiguration.DB_MVCC.getValueAsBoolean();
    validation = OGlobalConfiguration.DB_VALIDATION.getValueAsBoolean();
  }

  @Override
  public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword) {
    setCurrentDatabaseinThreadLocal();

    try {
      super.open(iUserName, iUserPassword);
      level1Cache.startup();

      metadata = new OMetadata();
      metadata.load();

      recordFormat = DEF_RECORD_FORMAT;

      if (!(getStorage() instanceof OStorageProxy)) {
        user = getMetadata().getSecurity().authenticate(iUserName, iUserPassword);
        if (user != null) {
          final Set<ORole> roles = user.getRoles();
          if (roles == null || roles.isEmpty() || roles.iterator().next() == null) {
            // SEEMS CORRUPTED: INSTALL DEFAULT ROLE
            for (ODatabaseListener l : underlying.getListeners()) {
              if (l.onCorruptionRepairDatabase(this, "Security metadata is broken: current user '" + user.getName()
                  + "' has no roles defined",
                  "The 'admin' user will be reinstalled with default role ('admin') and password 'admin'")) {
                user = null;
                user = metadata.getSecurity().repair();
                break;
              }
            }
          }
        }
        registerHook(new ORestrictedAccessHook());
        registerHook(new OUserTrigger());
        registerHook(new OClassIndexManager());
      } else
        // REMOTE CREATE DUMMY USER
        user = new OUser(iUserName, OUser.encryptPassword(iUserPassword)).addRole(new ORole("passthrough", null,
            ORole.ALLOW_MODES.ALLOW_ALL_BUT));

      checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_READ);

      if (!metadata.getSchema().existsClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME))
        // @COMPATIBILITY 1.0RC9
        metadata.getSchema().createClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME);
    } catch (OException e) {
      close();
      throw e;
    } catch (Exception e) {
      close();
      throw new ODatabaseException("Cannot open database", e);
    }
    return (DB) this;
  }

  @Override
  public <DB extends ODatabase> DB create() {
    setCurrentDatabaseinThreadLocal();

    try {
      super.create();

      level1Cache.startup();

      getStorage().getConfiguration().update();

      if (!(getStorage() instanceof OStorageProxy)) {
        registerHook(new ORestrictedAccessHook());
        registerHook(new OUserTrigger());
        registerHook(new OClassIndexManager());
      }

      // CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
      metadata = new OMetadata();
      metadata.create();

      user = getMetadata().getSecurity().getUser(OUser.ADMIN);

      if (!metadata.getSchema().existsClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME))
        // @COMPATIBILITY 1.0RC9
        metadata.getSchema().createClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME);

    } catch (Exception e) {
      throw new ODatabaseException("Cannot create database", e);
    }
    return (DB) this;
  }

  @Override
  public void drop() {
    checkOpeness();
    checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_DELETE);
    super.drop();
  }

  @Override
  public void close() {
    setCurrentDatabaseinThreadLocal();

    if (metadata != null) {
      metadata.close();
      metadata = null;
    }

    super.close();

    hooks.clear();
    dictionary = null;

    user = null;
    level1Cache.shutdown();
  }

  public ODictionary<ORecordInternal<?>> getDictionary() {
    checkOpeness();
    if (dictionary == null)
      dictionary = metadata.getIndexManager().getDictionary();
    return dictionary;
  }

  public <RET extends ORecordInternal<?>> RET getRecord(final OIdentifiable iIdentifiable) {
    if (iIdentifiable instanceof ORecord<?>)
      return (RET) iIdentifiable;
    return (RET) load(iIdentifiable.getIdentity());
  }

  public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord) {
    return (RET) load(iRecord, null);
  }

  @Override
  public void reload() {
    metadata.reload();
    super.reload();
  }

  public <RET extends ORecordInternal<?>> RET reload(final ORecordInternal<?> iRecord) {
    return (RET) executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, null, true);
  }

  public <RET extends ORecordInternal<?>> RET reload(final ORecordInternal<?> iRecord, final String iFetchPlan) {
    return (RET) executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, iFetchPlan, true);
  }

  public <RET extends ORecordInternal<?>> RET reload(final ORecordInternal<?> iRecord, final String iFetchPlan, boolean iIgnoreCache) {
    return (RET) executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache);
  }

  /**
   * Loads a record using a fetch plan.
   */
  public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord, final String iFetchPlan) {
    return (RET) executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, iFetchPlan, false);
  }

  public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord, final String iFetchPlan,
      final boolean iIgnoreCache) {
    return (RET) executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache);
  }

  public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId) {
    return (RET) executeReadRecord((ORecordId) iRecordId, null, null, false);
  }

  public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, final String iFetchPlan) {
    return (RET) executeReadRecord((ORecordId) iRecordId, null, iFetchPlan, false);
  }

  public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, final String iFetchPlan, final boolean iIgnoreCache) {
    return (RET) executeReadRecord((ORecordId) iRecordId, null, iFetchPlan, iIgnoreCache);
  }

  /**
   * Updates the record without checking the version.
   */
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iContent) {
    return (RET) executeSaveRecord(iContent, null, iContent.getVersion(), iContent.getRecordType(), true,
        OPERATION_MODE.SYNCHRONOUS, false, null);
  }

  /**
   * Updates the record without checking the version.
   * 
   * @param iForceCreate
   *          Flag that indicates that record should be created. If record with current rid already exists, exception is thrown
   * @param iCallback
   */
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iContent, final OPERATION_MODE iMode,
      boolean iForceCreate, final ORecordCallback<? extends Number> iCallback) {
    return (RET) executeSaveRecord(iContent, null, iContent.getVersion(), iContent.getRecordType(), true, iMode, iForceCreate,
        iCallback);
  }

  /**
   * Updates the record in the requested cluster without checking the version.
   */
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iContent, final String iClusterName) {
    return (RET) executeSaveRecord(iContent, iClusterName, iContent.getVersion(), iContent.getRecordType(), true,
        OPERATION_MODE.SYNCHRONOUS, false, null);
  }

  /**
   * Updates the record in the requested cluster without checking the version.
   * 
   * @param iForceCreate
   */
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iContent, final String iClusterName,
      final OPERATION_MODE iMode, boolean iForceCreate, final ORecordCallback<? extends Number> iCallback) {
    return (RET) executeSaveRecord(iContent, iClusterName, iContent.getVersion(), iContent.getRecordType(), true, iMode,
        iForceCreate, iCallback);
  }

  /**
   * Deletes the record without checking the version.
   */
  public ODatabaseRecord delete(final ORID iRecord) {
    executeDeleteRecord(iRecord, -1, true, true, OPERATION_MODE.SYNCHRONOUS);
    return this;
  }

  /**
   * Deletes the record checking the version.
   */
  public ODatabaseRecord delete(final ORID iRecord, final int iVersion) {
    executeDeleteRecord(iRecord, iVersion, true, true, OPERATION_MODE.SYNCHRONOUS);
    return this;
  }

  /**
   * Deletes the record without checking the version.
   */
  public ODatabaseRecord delete(final ORID iRecord, final OPERATION_MODE iMode) {
    executeDeleteRecord(iRecord, -1, true, true, iMode);
    return this;
  }

  /**
   * Deletes the record without checking the version.
   */
  public ODatabaseRecord delete(final ORecordInternal<?> iRecord) {
    executeDeleteRecord(iRecord, -1, true, true, OPERATION_MODE.SYNCHRONOUS);
    return this;
  }

  /**
   * Deletes the record without checking the version.
   */
  public ODatabaseRecord delete(final ORecordInternal<?> iRecord, final OPERATION_MODE iMode) {
    executeDeleteRecord(iRecord, -1, true, true, iMode);
    return this;
  }

  public <REC extends ORecordInternal<?>> ORecordIteratorCluster<REC> browseCluster(final String iClusterName,
      final Class<REC> iClass) {
    checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    setCurrentDatabaseinThreadLocal();

    final int clusterId = getClusterIdByName(iClusterName);

    return new ORecordIteratorCluster<REC>(this, this, clusterId);
  }

  public ORecordIteratorCluster<?> browseCluster(final String iClusterName) {
    checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    setCurrentDatabaseinThreadLocal();

    final int clusterId = getClusterIdByName(iClusterName);

    return new ORecordIteratorCluster<ORecordInternal<?>>(this, this, clusterId);
  }

  public OCommandRequest command(final OCommandRequest iCommand) {
    checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    setCurrentDatabaseinThreadLocal();

    final OCommandRequestInternal command = (OCommandRequestInternal) iCommand;

    try {
      command.reset();
      return command;

    } catch (Exception e) {
      throw new ODatabaseException("Error on command execution", e);
    }
  }

  public <RET extends List<?>> RET query(final OQuery<? extends Object> iCommand, final Object... iArgs) {
    setCurrentDatabaseinThreadLocal();

    iCommand.reset();
    return (RET) iCommand.execute(iArgs);
  }

  public byte getRecordType() {
    return recordType;
  }

  public <RET extends Object> RET newInstance() {
    return (RET) Orient.instance().getRecordFactoryManager().newInstance(recordType);
  }

  @Override
  public long countClusterElements(final int[] iClusterIds) {
    String name;
    for (int i = 0; i < iClusterIds.length; ++i) {
      name = getClusterNameById(iClusterIds[i]);
      checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, name);
    }

    return super.countClusterElements(iClusterIds);
  }

  @Override
  public long countClusterElements(final int iClusterId) {
    final String name = getClusterNameById(iClusterId);
    if (name == null)
      return 0;
    checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, name);
    setCurrentDatabaseinThreadLocal();
    return super.countClusterElements(name);
  }

  @Override
  public long countClusterElements(final String iClusterName) {
    checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    setCurrentDatabaseinThreadLocal();
    return super.countClusterElements(iClusterName);
  }

  public OMetadata getMetadata() {
    checkOpeness();
    return metadata;
  }

  public <DB extends ODatabaseRecord> DB checkSecurity(final String iResource, final int iOperation) {
    if (user != null) {
      try {
        user.allow(iResource, iOperation);
      } catch (OSecurityAccessException e) {

        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this,
              "[checkSecurity] User '%s' tried to access to the reserved resource '%s', operation '%s'", getUser(), iResource,
              iOperation);

        throw e;
      }
    }
    return (DB) this;
  }

  public <DB extends ODatabaseRecord> DB checkSecurity(final String iResourceGeneric, final int iOperation,
      final Object... iResourcesSpecific) {

    if (user != null) {
      try {
        final StringBuilder keyBuffer = new StringBuilder();

        boolean ruleFound = false;
        for (Object target : iResourcesSpecific) {
          if (target != null) {
            keyBuffer.setLength(0);
            keyBuffer.append(iResourceGeneric);
            keyBuffer.append('.');
            keyBuffer.append(target.toString());

            final String key = keyBuffer.toString();

            if (user.isRuleDefined(key)) {
              ruleFound = true;
              // RULE DEFINED: CHECK AGAINST IT
              user.allow(key, iOperation);
            }
          }
        }

        if (!ruleFound) {
          // CHECK AGAINST GENERIC RULE
          keyBuffer.setLength(0);
          keyBuffer.append(iResourceGeneric);
          keyBuffer.append('.');
          keyBuffer.append(ODatabaseSecurityResources.ALL);

          user.allow(keyBuffer.toString(), iOperation);
        }

      } catch (OSecurityAccessException e) {
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this,
              "[checkSecurity] User '%s' tried to access to the reserved resource '%s', target(s) '%s', operation '%s'", getUser(),
              iResourceGeneric, Arrays.toString(iResourcesSpecific), iOperation);

        throw e;
      }
    }
    return (DB) this;
  }

  public <DB extends ODatabaseRecord> DB checkSecurity(final String iResourceGeneric, final int iOperation,
      final Object iResourceSpecific) {

    if (user != null) {
      try {
        final StringBuilder keyBuffer = new StringBuilder();

        boolean ruleFound = false;
        if (iResourceSpecific != null) {
          keyBuffer.setLength(0);
          keyBuffer.append(iResourceGeneric);
          keyBuffer.append('.');
          keyBuffer.append(iResourceSpecific.toString());

          final String key = keyBuffer.toString();

          if (user.isRuleDefined(key)) {
            ruleFound = true;
            // RULE DEFINED: CHECK AGAINST IT
            user.allow(key, iOperation);
          }
        }

        if (!ruleFound) {
          // CHECK AGAINST GENERIC RULE
          keyBuffer.setLength(0);
          keyBuffer.append(iResourceGeneric);
          keyBuffer.append('.');
          keyBuffer.append(ODatabaseSecurityResources.ALL);

          user.allow(keyBuffer.toString(), iOperation);
        }

      } catch (OSecurityAccessException e) {
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this,
              "[checkSecurity] User '%s' tried to access to the reserved resource '%s', target '%s', operation '%s'", getUser(),
              iResourceGeneric, iResourceSpecific, iOperation);

        throw e;
      }
    }
    return (DB) this;
  }

  public <RET extends ORecordInternal<?>> RET executeReadRecord(final ORecordId iRid, ORecordInternal<?> iRecord,
      final String iFetchPlan, final boolean iIgnoreCache) {
    checkOpeness();

    // setCurrentDatabaseinThreadLocal();

    try {
      checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, getClusterNameById(iRid.getClusterId()));

      // SEARCH IN LOCAL TX
      ORecordInternal<?> record = getTransaction().getRecord(iRid);
      if (record == OTransactionRealAbstract.DELETED_RECORD)
        // DELETED IN TX
        return null;

      if (record == null && !iIgnoreCache)
        // SEARCH INTO THE CACHE
        record = getLevel1Cache().findRecord(iRid);

      if (record != null) {
        if (iRecord != null) {
          iRecord.fromStream(record.toStream());
          record = iRecord;
        }

        OFetchHelper.checkFetchPlanValid(iFetchPlan);
        if (callbackHooks(TYPE.BEFORE_READ, record) == ORecordHook.RESULT.SKIP)
          return null;

        if (record.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
          record.reload();

        callbackHooks(TYPE.AFTER_READ, record);
        return (RET) record;
      }

      final ORawBuffer recordBuffer = underlying.read(iRid, iFetchPlan, iIgnoreCache).getResult();
      if (recordBuffer == null)
        return null;

      if (iRecord == null || iRecord.getRecordType() != recordBuffer.recordType)
        // NO SAME RECORD TYPE: CAN'T REUSE OLD ONE BUT CREATE A NEW ONE FOR IT
        iRecord = Orient.instance().getRecordFactoryManager().newInstance(recordBuffer.recordType);

      iRecord.fill(iRid, recordBuffer.version, recordBuffer.buffer, false);

      if (callbackHooks(TYPE.BEFORE_READ, iRecord) == RESULT.SKIP)
        return null;

      iRecord.fromStream(recordBuffer.buffer);

      callbackHooks(TYPE.AFTER_READ, iRecord);

      if (!iIgnoreCache)
        getLevel1Cache().updateRecord(iRecord);

      return (RET) iRecord;
    } catch (OException e) {
      // RE-THROW THE EXCEPTION
      throw e;

    } catch (Exception e) {
      // WRAP IT AS ODATABASE EXCEPTION
      OLogManager.instance().exception("Error on retrieving record " + iRid, e, ODatabaseException.class);
    }
    return null;
  }

  public <RET extends ORecordInternal<?>> RET executeSaveRecord(final ORecordInternal<?> iRecord, String iClusterName,
      final int iVersion, final byte iRecordType, final boolean iCallTriggers, final OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iCallback) {
    checkOpeness();

    if (!iRecord.isDirty())
      return (RET) iRecord;

    final ORecordId rid = (ORecordId) iRecord.getIdentity();

    if (rid == null)
      throw new ODatabaseException(
          "Cannot create record because it has no identity. Probably is not a regular record or contains projections of fields rather than a full record");

    setCurrentDatabaseinThreadLocal();

    try {
      final boolean wasNew = iForceCreate || rid.isNew();
      if (wasNew && rid.clusterId == -1 && iClusterName != null)
        // ASSIGN THE CLUSTER ID
        rid.clusterId = getClusterIdByName(iClusterName);

      // STREAM.LENGTH == 0 -> RECORD IN STACK: WILL BE SAVED AFTER
      byte[] stream = iRecord.toStream();

      final boolean isNew = iForceCreate || rid.isNew();
      if (isNew)
        // NOTIFY IDENTITY HAS CHANGED
        iRecord.onBeforeIdentityChanged(rid);
      else if (stream == null || stream.length == 0)
        // ALREADY CREATED AND WAITING FOR THE RIGHT UPDATE (WE'RE IN A GRAPH)
        return (RET) iRecord;

      if (isNew && rid.clusterId < 0)
        rid.clusterId = iClusterName != null ? getClusterIdByName(iClusterName) : getDefaultClusterId();

      if (rid.clusterId > -1 && iClusterName == null)
        iClusterName = getClusterNameById(rid.clusterId);

      if (stream != null && stream.length > 0) {
        if (iCallTriggers)
          if (wasNew) {
            // CHECK ACCESS ON CLUSTER
            checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_CREATE, iClusterName);
            if (callbackHooks(TYPE.BEFORE_CREATE, iRecord) == RESULT.RECORD_CHANGED)
              // RECORD CHANGED IN TRIGGER, REACQUIRE IT
              stream = iRecord.toStream();
          } else {
            // CHECK ACCESS ON CLUSTER
            checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_UPDATE, iClusterName);
            if (callbackHooks(TYPE.BEFORE_UPDATE, iRecord) == RESULT.RECORD_CHANGED)
              // RECORD CHANGED IN TRIGGER, REACQUIRE IT
              stream = iRecord.toStream();
          }
      }

      // CHECK IF ENABLE THE MVCC OR BYPASS IT
      final int realVersion = !mvcc || iVersion == -1 ? -1 : iRecord.getVersion();

      final int dataSegmentId = dataSegmentStrategy.assignDataSegmentId(this, iRecord);

      try {
        // SAVE IT
        final OStorageOperationResult<Integer> operationResult = underlying.save(dataSegmentId, rid, stream == null ? new byte[0]
            : stream, realVersion, iRecord.getRecordType(), iMode.ordinal(), iForceCreate, iCallback);

        final int version = operationResult.getResult();

        if (isNew) {
          // UPDATE INFORMATION: CLUSTER ID+POSITION
          ((ORecordId) iRecord.getIdentity()).copyFrom(rid);
          // NOTIFY IDENTITY HAS CHANGED
          iRecord.onAfterIdentityChanged(iRecord);
          // UPDATE INFORMATION: CLUSTER ID+POSITION
          iRecord.fill(rid, version, stream, stream == null || stream.length == 0);
        } else {
          // UPDATE INFORMATION: VERSION
          iRecord.fill(rid, version, stream, stream == null || stream.length == 0);
        }

        if (iCallTriggers && stream != null && stream.length > 0) {
          if (!operationResult.isMoved()) {
            callbackHooks(wasNew ? TYPE.AFTER_CREATE : TYPE.AFTER_UPDATE, iRecord);
          } else {
            callbackHooks(wasNew ? TYPE.CREATE_REPLICATED : TYPE.UPDATE_REPLICATED, iRecord);
          }
        }

        if (stream != null && stream.length > 0 && !operationResult.isMoved())
          // ADD/UPDATE IT IN CACHE IF IT'S ACTIVE
          getLevel1Cache().updateRecord(iRecord);
      } catch (Throwable t) {
        if (iCallTriggers && stream != null && stream.length > 0)
          callbackHooks(wasNew ? TYPE.CREATE_FAILED : TYPE.UPDATE_FAILED, iRecord);
        throw t;
      }
    } catch (OException e) {
      // RE-THROW THE EXCEPTION
      throw e;

    } catch (Throwable t) {
      // WRAP IT AS ODATABASE EXCEPTION
      throw new ODatabaseException("Error on saving record in cluster #" + iRecord.getIdentity().getClusterId(), t);
    }
    return (RET) iRecord;
  }

  public void executeDeleteRecord(final OIdentifiable iRecord, final int iVersion, final boolean iRequired, boolean iCallTriggers,
      final OPERATION_MODE iMode) {
    checkOpeness();
    final ORecordId rid = (ORecordId) iRecord.getIdentity();

    if (rid == null)
      throw new ODatabaseException(
          "Cannot delete record because it has no identity. Probably was created from scratch or contains projections of fields rather than a full record");

    if (!rid.isValid())
      return;

    checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_DELETE, getClusterNameById(rid.clusterId));

    setCurrentDatabaseinThreadLocal();

    try {
      // if cache is switched off record will be unreachable after delete.
      ORecord<?> rec = iRecord.getRecord();
      if (iCallTriggers)
        callbackHooks(TYPE.BEFORE_DELETE, rec);

      // CHECK IF ENABLE THE MVCC OR BYPASS IT
      final int realVersion = mvcc ? iVersion : -1;
      OStorageOperationResult<Boolean> operationResult = null;

      try {
        operationResult = underlying.delete(rid, realVersion, iRequired, (byte) iMode.ordinal());
      } catch (Throwable t) {
        if (iCallTriggers)
          callbackHooks(TYPE.DELETE_FAILED, rec);
      }

      if (iCallTriggers) {
        if (operationResult == null || !operationResult.isMoved()) {
          callbackHooks(TYPE.AFTER_DELETE, rec);
        } else {
          callbackHooks(TYPE.DELETE_REPLICATED, rec);
        }
      }

      // REMOVE THE RECORD FROM 1 AND 2 LEVEL CACHES
      if (operationResult == null || !operationResult.isMoved()) {
        getLevel1Cache().deleteRecord(rid);
      }

    } catch (OException e) {
      // RE-THROW THE EXCEPTION
      throw e;

    } catch (Throwable t) {
      // WRAP IT AS ODATABASE EXCEPTION
      throw new ODatabaseException("Error on deleting record in cluster #" + iRecord.getIdentity().getClusterId(), t);
    }
  }

  @Override
  public ODatabaseComplex<?> getDatabaseOwner() {
    ODatabaseComplex<?> current = databaseOwner;

    while (current != null && current != this && current.getDatabaseOwner() != current)
      current = current.getDatabaseOwner();

    return current;
  }

  @Override
  public ODatabaseComplex<ORecordInternal<?>> setDatabaseOwner(ODatabaseComplex<?> iOwner) {
    databaseOwner = iOwner;
    return this;
  }

  public boolean isRetainRecords() {
    return retainRecords;
  }

  public ODatabaseRecord setRetainRecords(boolean retainRecords) {
    this.retainRecords = retainRecords;
    return this;
  }

  public <DB extends ODatabase> DB setStatus(final STATUS status) {
    final String cmd = String.format("alter database status %s", status.toString());
    command(new OCommandSQL(cmd)).execute();
    return (DB) this;
  }

  public void setStatusInternal(final STATUS status) {
    underlying.setStatus(status);
  }

  public void setDefaultClusterIdInternal(final int iDefClusterId) {
    getStorage().setDefaultClusterId(iDefClusterId);
  }

  public void setInternal(final ATTRIBUTES iAttribute, final Object iValue) {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;

    switch (iAttribute) {
    case STATUS:
      setStatusInternal(STATUS.valueOf(stringValue.toUpperCase(Locale.ENGLISH)));
      break;
    case DEFAULTCLUSTERID:
      if (iValue != null) {
        if (iValue instanceof Number)
          getStorage().setDefaultClusterId(((Number) iValue).intValue());
        else
          getStorage().setDefaultClusterId(getStorage().getClusterIdByName(iValue.toString()));
      }
      break;
    case TYPE:
      if (stringValue.equalsIgnoreCase("graph")) {
        if (getDatabaseOwner() instanceof OGraphDatabase)
          ((OGraphDatabase) getDatabaseOwner()).checkForGraphSchema();
        else if (this instanceof ODatabaseRecordTx)
          new OGraphDatabase((ODatabaseRecordTx) this).checkForGraphSchema();
        else if (getDatabaseOwner() instanceof ODatabaseRecordTx)
          new OGraphDatabase((ODatabaseRecordTx) getDatabaseOwner()).checkForGraphSchema();
        else
          new OGraphDatabase(getURL()).checkForGraphSchema();
      } else
        throw new IllegalArgumentException("Database type '" + stringValue + "' is not supported");

      break;
    default:
      throw new IllegalArgumentException("Option '" + iAttribute + "' not supported on alter database");
    }
  }

  public OUser getUser() {
    return user;
  }

  public boolean isMVCC() {
    return mvcc;
  }

  public <DB extends ODatabaseComplex<?>> DB setMVCC(boolean mvcc) {
    this.mvcc = mvcc;
    return (DB) this;
  }

  public <DB extends ODatabaseComplex<?>> DB registerHook(final ORecordHook iHookImpl) {
    hooks.add(iHookImpl);
    return (DB) this;
  }

  public <DB extends ODatabaseComplex<?>> DB unregisterHook(final ORecordHook iHookImpl) {
    hooks.remove(iHookImpl);
    return (DB) this;
  }

  @Override
  public OLevel1RecordCache getLevel1Cache() {
    return level1Cache;
  }

  public Set<ORecordHook> getHooks() {
    return unmodifiableHooks;
  }

  /**
   * Callback the registeted hooks if any.
   * 
   * @param iType
   * @param id
   *          Record received in the callback
   * @return True if the input record is changed, otherwise false
   */
  public ORecordHook.RESULT callbackHooks(final TYPE iType, final OIdentifiable id) {
    if (id == null || !OHookThreadLocal.INSTANCE.push(id))
      return RESULT.RECORD_NOT_CHANGED;

    try {
      final ORecord<?> rec = id.getRecord();
      if (rec == null)
        return RESULT.RECORD_NOT_CHANGED;

      boolean recordChanged = false;
      for (ORecordHook hook : hooks) {
        final RESULT res = hook.onTrigger(iType, rec);

        if (res == RESULT.RECORD_CHANGED)
          recordChanged = true;
        else if (res == RESULT.SKIP)
          // SKIP NEXT HOOKS AND RETURN IT
          return res;
      }

      return recordChanged ? RESULT.RECORD_CHANGED : RESULT.RECORD_NOT_CHANGED;

    } finally {
      OHookThreadLocal.INSTANCE.pop(id);
    }
  }

  protected ORecordSerializer resolveFormat(final Object iObject) {
    return ORecordSerializerFactory.instance().getFormatForObject(iObject, recordFormat);
  }

  @Override
  protected void checkOpeness() {
    if (isClosed())
      throw new ODatabaseException("Database '" + getURL() + "' is closed");
  }

  protected void setCurrentDatabaseinThreadLocal() {
    ODatabaseRecordThreadLocal.INSTANCE.set(this);
  }

  public boolean isValidationEnabled() {
    return !getStatus().equals(STATUS.IMPORTING) && validation;
  }

  public <DB extends ODatabaseRecord> DB setValidationEnabled(final boolean iEnabled) {
    validation = iEnabled;
    return (DB) this;
  }

  public ODataSegmentStrategy getDataSegmentStrategy() {
    return dataSegmentStrategy;
  }

  public void setDataSegmentStrategy(ODataSegmentStrategy dataSegmentStrategy) {
    this.dataSegmentStrategy = dataSegmentStrategy;
  }
}
