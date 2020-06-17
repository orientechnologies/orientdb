package com.orientechnologies.orient.client.binary;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.OAddClusterRequest;
import com.orientechnologies.orient.client.remote.message.OBatchOperationsRequest;
import com.orientechnologies.orient.client.remote.message.OBeginTransaction38Request;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCloseQueryRequest;
import com.orientechnologies.orient.client.remote.message.OCloseRequest;
import com.orientechnologies.orient.client.remote.message.OCommandRequest;
import com.orientechnologies.orient.client.remote.message.OCommit37Request;
import com.orientechnologies.orient.client.remote.message.OCommit38Request;
import com.orientechnologies.orient.client.remote.message.OCommitRequest;
import com.orientechnologies.orient.client.remote.message.OConnect37Request;
import com.orientechnologies.orient.client.remote.message.OConnectRequest;
import com.orientechnologies.orient.client.remote.message.OCountRecordsRequest;
import com.orientechnologies.orient.client.remote.message.OCountRequest;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OCreateRecordRequest;
import com.orientechnologies.orient.client.remote.message.ODeleteRecordRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedConnectRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusRequest;
import com.orientechnologies.orient.client.remote.message.ODropClusterRequest;
import com.orientechnologies.orient.client.remote.message.ODropDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OExperimentalRequest;
import com.orientechnologies.orient.client.remote.message.OFetchTransaction38Request;
import com.orientechnologies.orient.client.remote.message.OFetchTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeRequest;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataRequest;
import com.orientechnologies.orient.client.remote.message.OGetSizeRequest;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OImportRequest;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupRequest;
import com.orientechnologies.orient.client.remote.message.OListDatabasesRequest;
import com.orientechnologies.orient.client.remote.message.OListGlobalConfigurationsRequest;
import com.orientechnologies.orient.client.remote.message.OLockRecordRequest;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OOpen37Request;
import com.orientechnologies.orient.client.remote.message.OOpenRequest;
import com.orientechnologies.orient.client.remote.message.OQueryNextPageRequest;
import com.orientechnologies.orient.client.remote.message.OQueryRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordIfVersionIsNotLatestRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordRequest;
import com.orientechnologies.orient.client.remote.message.OReleaseDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OReloadRequest;
import com.orientechnologies.orient.client.remote.message.OReloadRequest37;
import com.orientechnologies.orient.client.remote.message.OReopenRequest;
import com.orientechnologies.orient.client.remote.message.ORollbackTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OSBTCreateTreeRequest;
import com.orientechnologies.orient.client.remote.message.OSBTFetchEntriesMajorRequest;
import com.orientechnologies.orient.client.remote.message.OSBTFirstKeyRequest;
import com.orientechnologies.orient.client.remote.message.OSBTGetRealBagSizeRequest;
import com.orientechnologies.orient.client.remote.message.OSBTGetRequest;
import com.orientechnologies.orient.client.remote.message.OServerInfoRequest;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OShutdownRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeDistributedConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeFunctionsRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeIndexManagerRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeLiveQueryRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeSchemaRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeSequencesRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeStorageConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OUnlockRecordRequest;
import com.orientechnologies.orient.client.remote.message.OUnsubscribeLiveQueryRequest;
import com.orientechnologies.orient.client.remote.message.OUnsubscribeRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordRequest;

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
