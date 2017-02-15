package com.orientechnologies.orient.client.binary;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.*;

public interface OBinaryRequestExecutor {

  OBinaryResponse executeListDatabases(OListDatabasesRequest request);

  OBinaryResponse executeServerInfo(OServerInfoRequest request);

  OBinaryResponse executeDBReload(OReloadRequest request);

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

  OBinaryResponse executeHideRecord(OHideRecordRequest request);

  OBinaryResponse executeHigherPosition(OHigherPhysicalPositionsRequest request);

  OBinaryResponse executeCeilingPosition(OCeilingPhysicalPositionsRequest request);

  OBinaryResponse executeLowerPosition(OLowerPhysicalPositionsRequest request);

  OBinaryResponse executeFloorPosition(OFloorPhysicalPositionsRequest request);

  OBinaryResponse executeCommand(OCommandRequest request);

  OBinaryResponse executeCommit(OCommitRequest request);

  OBinaryResponse executeGetGlobalConfiguration(OGetGlobalConfigurationRequest request);

  OBinaryResponse executeListGlobalConfigurations(OListGlobalConfigurationsRequest request);

  OBinaryResponse executeFreezeDatabase(OFreezeDatabaseRequest request);

  OBinaryResponse executeReleaseDatabase(OReleaseDatabaseRequest request);

  OBinaryResponse executeCleanOutRecord(OCleanOutRecordRequest request);

  OBinaryResponse executeSBTreeCreate(OSBTCreateTreeRequest request);

  OBinaryResponse executeSBTGet(OSBTGetRequest request);

  OBinaryResponse executeSBTFirstKey(OSBTFirstKeyRequest request);

  OBinaryResponse executeSBTFetchEntriesMajor(@SuppressWarnings("rawtypes") OSBTFetchEntriesMajorRequest request);

  OBinaryResponse executeSBTGetRealSize(OSBTGetRealBagSizeRequest request);

  OBinaryResponse executeIncrementalBackup(OIncrementalBackupRequest request);

  OBinaryResponse executeImport(OImportRequest request);

  OBinaryResponse executeSetGlobalConfig(OSetGlobalConfigurationRequest request);

  OBinaryResponse executeConnect(OConnectRequest request);

  OBinaryResponse executeDatabaseOpen(OOpenRequest request);

  OBinaryResponse executeShutdown(OShutdownRequest request);

  OBinaryResponse executeReopen(OReopenRequest request);

  OBinaryResponse executeQuery(OQueryRequest request);

  OBinaryResponse closeQuery(OCloseQueryRequest request);

  OBinaryResponse executeQueryNextPage(OQueryNextPageRequest request);

  OBinaryResponse executeBeginTransaction(OBeginTransactionRequest request);

  OBinaryResponse executeCommit37(OCommit37Request request);

  OBinaryResponse executeFetchTransaction(OFetchTransactionRequest request);

  OBinaryResponse executeRollback(ORollbackTransactionRequest request);
}
