package com.orientechnologies.orient.client.binary;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.OAddClusterRequest;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCloseRequest;
import com.orientechnologies.orient.client.remote.message.OCommandRequest;
import com.orientechnologies.orient.client.remote.message.OCommitRequest;
import com.orientechnologies.orient.client.remote.message.OConnectRequest;
import com.orientechnologies.orient.client.remote.message.OCountRecordsRequest;
import com.orientechnologies.orient.client.remote.message.OCountRequest;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OCreateRecordRequest;
import com.orientechnologies.orient.client.remote.message.ODeleteRecordRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusRequest;
import com.orientechnologies.orient.client.remote.message.ODropClusterRequest;
import com.orientechnologies.orient.client.remote.message.ODropDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeRequest;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataRequest;
import com.orientechnologies.orient.client.remote.message.OGetSizeRequest;
import com.orientechnologies.orient.client.remote.message.OHideRecordRequest;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OImportRequest;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupRequest;
import com.orientechnologies.orient.client.remote.message.OListDatabasesRequest;
import com.orientechnologies.orient.client.remote.message.OListGlobalConfigurationsRequest;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OOpenRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordIfVersionIsNotLatestRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordRequest;
import com.orientechnologies.orient.client.remote.message.OReleaseDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OReloadRequest;
import com.orientechnologies.orient.client.remote.message.OReopenRequest;
import com.orientechnologies.orient.client.remote.message.OSBTCreateTreeRequest;
import com.orientechnologies.orient.client.remote.message.OSBTFetchEntriesMajorRequest;
import com.orientechnologies.orient.client.remote.message.OSBTFirstKeyRequest;
import com.orientechnologies.orient.client.remote.message.OSBTGetRealBagSizeRequest;
import com.orientechnologies.orient.client.remote.message.OSBTGetRequest;
import com.orientechnologies.orient.client.remote.message.OServerInfoRequest;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OShutdownRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordRequest;

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

}
