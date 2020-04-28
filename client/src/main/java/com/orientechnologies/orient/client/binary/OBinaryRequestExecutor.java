package com.orientechnologies.orient.client.binary;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.*;

public interface OBinaryRequestExecutor {

  OBinaryResponse executeListDatabases(OListDatabasesRequest request);

  OBinaryResponse executeServerInfo(OServerInfoRequest request);

  OBinaryResponse executeDBReload(OReloadRequest request);

  OBinaryResponse executeDBReload(OReloadRequest37 request);

  OBinaryResponse executeCreateDatabase(OCreateDatabaseRequest request);

  OBinaryResponse executeClose(OCloseRequest request);

  OBinaryResponse executeExistDatabase(OExistsDatabaseRequest request);

  OBinaryResponse executeDropDatabase(ODropDatabaseRequest request);

  OBinaryResponse executeGetSize(OGetSizeRequest request);

  OBinaryResponse executeCountRecords(OCountRecordsRequest request);

  OBinaryResponse executeDistributedStatus(ODistributedStatusRequest request);

  OBinaryResponse executeCountCluster(OCountRequest request);

  OBinaryResponse executeClusterDataRange(OGetClusterDataRangeRequest request);

  OBinaryResponse executeAddCluster(OAddClusterRequest request);

  OBinaryResponse executeDropCluster(ODropClusterRequest request);

  OBinaryResponse executeGetRecordMetadata(OGetRecordMetadataRequest request);

  OBinaryResponse executeReadRecord(OReadRecordRequest request);

  OBinaryResponse executeReadRecordIfNotLastest(OReadRecordIfVersionIsNotLatestRequest request);

  OBinaryResponse executeCreateRecord(OCreateRecordRequest request);

  OBinaryResponse executeUpdateRecord(OUpdateRecordRequest request);

  OBinaryResponse executeDeleteRecord(ODeleteRecordRequest request);

  OBinaryResponse executeHigherPosition(OHigherPhysicalPositionsRequest request);

  OBinaryResponse executeCeilingPosition(OCeilingPhysicalPositionsRequest request);

  OBinaryResponse executeLowerPosition(OLowerPhysicalPositionsRequest request);

  OBinaryResponse executeFloorPosition(OFloorPhysicalPositionsRequest request);

  OBinaryResponse executeCommand(OCommandRequest request);

  OBinaryResponse executeCommit(OCommitRequest request);

  OBinaryResponse executeBatchOperations(OBatchOperationsRequest request);

  OBinaryResponse executeGetGlobalConfiguration(OGetGlobalConfigurationRequest request);

  OBinaryResponse executeListGlobalConfigurations(OListGlobalConfigurationsRequest request);

  OBinaryResponse executeFreezeDatabase(OFreezeDatabaseRequest request);

  OBinaryResponse executeReleaseDatabase(OReleaseDatabaseRequest request);

  OBinaryResponse executeCleanOutRecord(OCleanOutRecordRequest request);

  OBinaryResponse executeSBTreeCreate(OSBTCreateTreeRequest request);

  OBinaryResponse executeSBTGet(OSBTGetRequest request);

  OBinaryResponse executeSBTFirstKey(OSBTFirstKeyRequest request);

  OBinaryResponse executeSBTFetchEntriesMajor(
      @SuppressWarnings("rawtypes") OSBTFetchEntriesMajorRequest request);

  OBinaryResponse executeSBTGetRealSize(OSBTGetRealBagSizeRequest request);

  OBinaryResponse executeIncrementalBackup(OIncrementalBackupRequest request);

  OBinaryResponse executeImport(OImportRequest request);

  OBinaryResponse executeSetGlobalConfig(OSetGlobalConfigurationRequest request);

  OBinaryResponse executeConnect(OConnectRequest request);

  OBinaryResponse executeConnect37(OConnect37Request request);

  OBinaryResponse executeDatabaseOpen(OOpenRequest request);

  OBinaryResponse executeDatabaseOpen37(OOpen37Request request);

  OBinaryResponse executeShutdown(OShutdownRequest request);

  OBinaryResponse executeReopen(OReopenRequest request);

  OBinaryResponse executeQuery(OQueryRequest request);

  OBinaryResponse executeServerQuery(OServerQueryRequest request);

  OBinaryResponse closeQuery(OCloseQueryRequest request);

  OBinaryResponse executeQueryNextPage(OQueryNextPageRequest request);

  OBinaryResponse executeBeginTransaction(OBeginTransactionRequest request);

  OBinaryResponse executeCommit37(OCommit37Request request);

  OBinaryResponse executeFetchTransaction(OFetchTransactionRequest request);

  OBinaryResponse executeRollback(ORollbackTransactionRequest request);

  OBinaryResponse executeSubscribe(OSubscribeRequest request);

  OBinaryResponse executeSubscribeDistributedConfiguration(
      OSubscribeDistributedConfigurationRequest request);

  OBinaryResponse executeSubscribeLiveQuery(OSubscribeLiveQueryRequest request);

  OBinaryResponse executeUnsubscribe(OUnsubscribeRequest request);

  OBinaryResponse executeUnsubscribeLiveQuery(OUnsubscribeLiveQueryRequest request);

  OBinaryResponse executeDistributedConnect(ODistributedConnectRequest request);

  OBinaryResponse executeSubscribeStorageConfiguration(
      OSubscribeStorageConfigurationRequest request);

  OBinaryResponse executeSubscribeSchema(OSubscribeSchemaRequest request);

  OBinaryResponse executeSubscribeIndexManager(OSubscribeIndexManagerRequest request);

  OBinaryResponse executeSubscribeFunctions(OSubscribeFunctionsRequest request);

  OBinaryResponse executeSubscribeSequences(OSubscribeSequencesRequest request);

  OBinaryResponse executeExperimental(OExperimentalRequest request);

  OBinaryResponse executeLockRecord(OLockRecordRequest request);

  OBinaryResponse executeUnlockRecord(OUnlockRecordRequest request);

  OBinaryResponse executeBeginTransaction38(OBeginTransaction38Request request);

  OBinaryResponse executeCommit38(OCommit38Request request);

  OBinaryResponse executeFetchTransaction38(OFetchTransaction38Request request);
}
