/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.*;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import java.util.function.Function;

/** Created by Enrico Risa on 05/04/17. */
public class ONetworkBinaryProtocolFactory {

  private static Function<Integer, OBinaryRequest<? extends OBinaryResponse>> defaultProtocol =
      ONetworkBinaryProtocolFactory::createRequest;

  public static Function<Integer, OBinaryRequest<? extends OBinaryResponse>> defaultProtocol() {
    return defaultProtocol;
  }

  public static Function<Integer, OBinaryRequest<? extends OBinaryResponse>> matchProtocol(
      short protocolVersion) {
    switch (protocolVersion) {
      case 37:
        return ONetworkBinaryProtocolFactory::createRequest37;
      case 38:
        return ONetworkBinaryProtocolFactory::createRequest38;
      default:
        return ONetworkBinaryProtocolFactory::createRequest;
    }
  }

  /**
   * Legacy Protocol < 37
   *
   * @param requestType
   * @return
   */
  private static OBinaryRequest<? extends OBinaryResponse> createRequest(int requestType) {
    switch (requestType) {
      case OChannelBinaryProtocol.REQUEST_DB_OPEN:
        return new OOpenRequest();

      case OChannelBinaryProtocol.REQUEST_CONNECT:
        return new OConnectRequest();

      case OChannelBinaryProtocol.REQUEST_DB_REOPEN:
        return new OReopenRequest();

      case OChannelBinaryProtocol.REQUEST_SHUTDOWN:
        return new OShutdownRequest();

      case OChannelBinaryProtocol.REQUEST_DB_LIST:
        return new OListDatabasesRequest();

      case OChannelBinaryProtocol.REQUEST_SERVER_INFO:
        return new OServerInfoRequest();

      case OChannelBinaryProtocol.REQUEST_DB_RELOAD:
        return new OReloadRequest();

      case OChannelBinaryProtocol.REQUEST_DB_CREATE:
        return new OCreateDatabaseRequest();

      case OChannelBinaryProtocol.REQUEST_DB_CLOSE:
        return new OCloseRequest();

      case OChannelBinaryProtocol.REQUEST_DB_EXIST:
        return new OExistsDatabaseRequest();

      case OChannelBinaryProtocol.REQUEST_DB_DROP:
        return new ODropDatabaseRequest();

      case OChannelBinaryProtocol.REQUEST_DB_SIZE:
        return new OGetSizeRequest();

      case OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS:
        return new OCountRecordsRequest();

      case OChannelBinaryProtocol.REQUEST_CLUSTER:
        return new ODistributedStatusRequest();

      case OChannelBinaryProtocol.REQUEST_CLUSTER_COUNT:
        return new OCountRequest();

      case OChannelBinaryProtocol.REQUEST_CLUSTER_DATARANGE:
        return new OGetClusterDataRangeRequest();

      case OChannelBinaryProtocol.REQUEST_CLUSTER_ADD:
        return new OAddClusterRequest();

      case OChannelBinaryProtocol.REQUEST_CLUSTER_DROP:
        return new ODropClusterRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_METADATA:
        return new OGetRecordMetadataRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_LOAD:
        return new OReadRecordRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST:
        return new OReadRecordIfVersionIsNotLatestRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_CREATE:
        return new OCreateRecordRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE:
        return new OUpdateRecordRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_DELETE:
        return new ODeleteRecordRequest();

      case OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER:
        return new OHigherPhysicalPositionsRequest();

      case OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING:
        return new OCeilingPhysicalPositionsRequest();

      case OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER:
        return new OLowerPhysicalPositionsRequest();

      case OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR:
        return new OFloorPhysicalPositionsRequest();

      case OChannelBinaryProtocol.REQUEST_COMMAND:
        return new OCommandRequest();

      case OChannelBinaryProtocol.REQUEST_SERVER_QUERY:
        return new OServerQueryRequest();

      case OChannelBinaryProtocol.REQUEST_QUERY:
        return new OQueryRequest();

      case OChannelBinaryProtocol.REQUEST_CLOSE_QUERY:
        return new OCloseQueryRequest();

      case OChannelBinaryProtocol.REQUEST_QUERY_NEXT_PAGE:
        return new OQueryNextPageRequest();

      case OChannelBinaryProtocol.REQUEST_TX_COMMIT:
        return new OCommitRequest();

      case OChannelBinaryProtocol.REQUEST_CONFIG_GET:
        return new OGetGlobalConfigurationRequest();

      case OChannelBinaryProtocol.REQUEST_CONFIG_SET:
        return new OSetGlobalConfigurationRequest();

      case OChannelBinaryProtocol.REQUEST_CONFIG_LIST:
        return new OListGlobalConfigurationsRequest();

      case OChannelBinaryProtocol.REQUEST_DB_FREEZE:
        return new OFreezeDatabaseRequest();

      case OChannelBinaryProtocol.REQUEST_DB_RELEASE:
        return new OReleaseDatabaseRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT:
        return new OCleanOutRecordRequest();

      case OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI:
        return new OSBTCreateTreeRequest();

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET:
        return new OSBTGetRequest();

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY:
        return new OSBTFirstKeyRequest();

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR:
        return new OSBTFetchEntriesMajorRequest<>();

      case OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE:
        return new OSBTGetRealBagSizeRequest();

      case OChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP:
        return new OIncrementalBackupRequest();

      case OChannelBinaryProtocol.REQUEST_DB_IMPORT:
        return new OImportRequest();
      case OChannelBinaryProtocol.DISTRIBUTED_CONNECT:
        return new ODistributedConnectRequest();
      default:
        throw new ODatabaseException("binary protocol command with code: " + requestType);
    }
  }

  /**
   * Protocol 37
   *
   * @param requestType
   * @return
   */
  public static OBinaryRequest<? extends OBinaryResponse> createRequest37(int requestType) {
    switch (requestType) {
      case OChannelBinaryProtocol.SUBSCRIBE_PUSH:
        return new OSubscribeRequest();

      case OChannelBinaryProtocol.EXPERIMENTAL:
        return new OExperimentalRequest();

      case OChannelBinaryProtocol.UNSUBSCRIBE_PUSH:
        return new OUnsubscribeRequest();

      case OChannelBinaryProtocol.REQUEST_TX_FETCH:
        return new OFetchTransactionRequest();

      case OChannelBinaryProtocol.REQUEST_TX_REBEGIN:
        return new ORebeginTransactionRequest();

      case OChannelBinaryProtocol.REQUEST_TX_BEGIN:
        return new OBeginTransactionRequest();

      case OChannelBinaryProtocol.REQUEST_TX_COMMIT:
        return new OCommit37Request();

      case OChannelBinaryProtocol.REQUEST_TX_ROLLBACK:
        return new ORollbackTransactionRequest();

      case OChannelBinaryProtocol.REQUEST_BATCH_OPERATIONS:
        return new OBatchOperationsRequest();

      case OChannelBinaryProtocol.REQUEST_DB_OPEN:
        return new OOpen37Request();

      case OChannelBinaryProtocol.REQUEST_CONNECT:
        return new OConnect37Request();

      case OChannelBinaryProtocol.REQUEST_DB_REOPEN:
        return new OReopenRequest();

      case OChannelBinaryProtocol.REQUEST_SHUTDOWN:
        return new OShutdownRequest();

      case OChannelBinaryProtocol.REQUEST_DB_LIST:
        return new OListDatabasesRequest();

      case OChannelBinaryProtocol.REQUEST_SERVER_INFO:
        return new OServerInfoRequest();

      case OChannelBinaryProtocol.REQUEST_DB_RELOAD:
        return new OReloadRequest37();

      case OChannelBinaryProtocol.REQUEST_DB_CREATE:
        return new OCreateDatabaseRequest();

      case OChannelBinaryProtocol.REQUEST_DB_CLOSE:
        return new OCloseRequest();

      case OChannelBinaryProtocol.REQUEST_DB_EXIST:
        return new OExistsDatabaseRequest();

      case OChannelBinaryProtocol.REQUEST_DB_DROP:
        return new ODropDatabaseRequest();

      case OChannelBinaryProtocol.REQUEST_DB_SIZE:
        return new OGetSizeRequest();

      case OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS:
        return new OCountRecordsRequest();

      case OChannelBinaryProtocol.REQUEST_CLUSTER:
        return new ODistributedStatusRequest();

      case OChannelBinaryProtocol.REQUEST_CLUSTER_COUNT:
        return new OCountRequest();

      case OChannelBinaryProtocol.REQUEST_CLUSTER_DATARANGE:
        return new OGetClusterDataRangeRequest();

      case OChannelBinaryProtocol.REQUEST_CLUSTER_ADD:
        return new OAddClusterRequest();

      case OChannelBinaryProtocol.REQUEST_CLUSTER_DROP:
        return new ODropClusterRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_METADATA:
        return new OGetRecordMetadataRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_LOAD:
        return new OReadRecordRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST:
        return new OReadRecordIfVersionIsNotLatestRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_CREATE:
        return new OCreateRecordRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE:
        return new OUpdateRecordRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_DELETE:
        return new ODeleteRecordRequest();

      case OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER:
        return new OHigherPhysicalPositionsRequest();

      case OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING:
        return new OCeilingPhysicalPositionsRequest();

      case OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER:
        return new OLowerPhysicalPositionsRequest();

      case OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR:
        return new OFloorPhysicalPositionsRequest();

      case OChannelBinaryProtocol.REQUEST_COMMAND:
        return new OCommandRequest();

      case OChannelBinaryProtocol.REQUEST_SERVER_QUERY:
        return new OServerQueryRequest();

      case OChannelBinaryProtocol.REQUEST_QUERY:
        return new OQueryRequest();

      case OChannelBinaryProtocol.REQUEST_CLOSE_QUERY:
        return new OCloseQueryRequest();

      case OChannelBinaryProtocol.REQUEST_QUERY_NEXT_PAGE:
        return new OQueryNextPageRequest();

      case OChannelBinaryProtocol.REQUEST_CONFIG_GET:
        return new OGetGlobalConfigurationRequest();

      case OChannelBinaryProtocol.REQUEST_CONFIG_SET:
        return new OSetGlobalConfigurationRequest();

      case OChannelBinaryProtocol.REQUEST_CONFIG_LIST:
        return new OListGlobalConfigurationsRequest();

      case OChannelBinaryProtocol.REQUEST_DB_FREEZE:
        return new OFreezeDatabaseRequest();

      case OChannelBinaryProtocol.REQUEST_DB_RELEASE:
        return new OReleaseDatabaseRequest();

      case OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT:
        return new OCleanOutRecordRequest();

      case OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI:
        return new OSBTCreateTreeRequest();

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET:
        return new OSBTGetRequest();

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY:
        return new OSBTFirstKeyRequest();

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR:
        return new OSBTFetchEntriesMajorRequest<>();

      case OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE:
        return new OSBTGetRealBagSizeRequest();

      case OChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP:
        return new OIncrementalBackupRequest();

      case OChannelBinaryProtocol.REQUEST_DB_IMPORT:
        return new OImportRequest();
      case OChannelBinaryProtocol.DISTRIBUTED_CONNECT:
        return new ODistributedConnectRequest();
      default:
        throw new ODatabaseException(
            "binary protocol command with code: " + requestType + " for protocol version 37");
    }
  }

  /**
   * Protocol 38
   *
   * @param requestType
   * @return
   */
  public static OBinaryRequest<? extends OBinaryResponse> createRequest38(int requestType) {
    switch (requestType) {
      case OChannelBinaryProtocol.REQUEST_TX_FETCH:
        return new OFetchTransaction38Request();

      case OChannelBinaryProtocol.REQUEST_TX_REBEGIN:
        return new ORebeginTransaction38Request();

      case OChannelBinaryProtocol.REQUEST_TX_BEGIN:
        return new OBeginTransaction38Request();

      case OChannelBinaryProtocol.REQUEST_TX_COMMIT:
        return new OCommit38Request();

      default:
        return createRequest37(requestType);
    }
  }
}
