package com.orientechnologies.orient.core.config;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.storage.OChecksumMode;

public interface OConfiguration {
  default boolean environmentDumpCfgAtStartup() {
    return getValueAsBoolean(OGlobalConfiguration.ENVIRONMENT_DUMP_CFG_AT_STARTUP);
  }

  default int environmentLockManagerConcurrencyLevel() {
    return getValueAsInteger(OGlobalConfiguration.ENVIRONMENT_LOCK_MANAGER_CONCURRENCY_LEVEL);
  }

  default boolean environmentAllowJvmShutdown() {
    return getValueAsBoolean(OGlobalConfiguration.ENVIRONMENT_ALLOW_JVM_SHUTDOWN);
  }

  default int scriptPool() {
    return getValueAsInteger(OGlobalConfiguration.SCRIPT_POOL);
  }

  default boolean memoryUseUnsafe() {
    return getValueAsBoolean(OGlobalConfiguration.MEMORY_USE_UNSAFE);
  }

  default boolean memoryProfiling() {
    return getValueAsBoolean(OGlobalConfiguration.MEMORY_PROFILING);
  }

  default int memoryProfilingReportInterval() {
    return getValueAsInteger(OGlobalConfiguration.MEMORY_PROFILING_REPORT_INTERVAL);
  }

  default String memoryLeftToOs() {
    return getValueAsString(OGlobalConfiguration.MEMORY_LEFT_TO_OS);
  }

  default String memoryLeftToContainer() {
    return getValueAsString(OGlobalConfiguration.MEMORY_LEFT_TO_CONTAINER);
  }

  default int directMemoryPoolLimit() {
    return getValueAsInteger(OGlobalConfiguration.DIRECT_MEMORY_POOL_LIMIT);
  }

  default boolean directMemoryPreallocate() {
    return getValueAsBoolean(OGlobalConfiguration.DIRECT_MEMORY_PREALLOCATE);
  }

  default boolean directMemoryTrackMode() {
    return getValueAsBoolean(OGlobalConfiguration.DIRECT_MEMORY_TRACK_MODE);
  }

  default boolean directMemoryOnlyAlignedAccess() {
    return getValueAsBoolean(OGlobalConfiguration.DIRECT_MEMORY_ONLY_ALIGNED_ACCESS);
  }

  default int openFilesLimit() {
    return getValueAsInteger(OGlobalConfiguration.OPEN_FILES_LIMIT);
  }

  default int componentsLockCache() {
    return getValueAsInteger(OGlobalConfiguration.COMPONENTS_LOCK_CACHE);
  }

  default long diskCacheSize() {
    return getValueAsInteger(OGlobalConfiguration.DISK_CACHE_SIZE);
  }

  default long diskWriteCachePart() {
    return getValueAsInteger(OGlobalConfiguration.DISK_WRITE_CACHE_PART);
  }

  default long diskWriteCachePageFlushInterval() {
    return getValueAsLong(OGlobalConfiguration.DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL);
  }

  default OChecksumMode storageChecksumMode() {
    return getValueAsEnum(OGlobalConfiguration.STORAGE_CHECKSUM_MODE, OChecksumMode.class);
  }

  default String storageCompressionMethod() {
    return getValueAsString(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD);
  }

  default String storageEncryptionMethod() {
    return getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD);
  }

  default String storageEncryptionKey() {
    return getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
  }

  default int storageAtomicOperationsTableCompactionLimit() {
    return getValueAsInteger(OGlobalConfiguration.STORAGE_ATOMIC_OPERATIONS_TABLE_COMPACTION_LIMIT);
  }

  default boolean storageCallFsync() {
    return getValueAsBoolean(OGlobalConfiguration.STORAGE_CALL_FSYNC);
  }

  default boolean storageUseDoubleWriteLog() {
    return getValueAsBoolean(OGlobalConfiguration.STORAGE_USE_DOUBLE_WRITE_LOG);
  }

  default long storageDoubleWriteLogMaxSegSize() {
    return getValueAsInteger(OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE);
  }

  default long storageDoubleWriteLogMaxSegSizePercent() {
    return getValueAsInteger(OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE_PERCENT);
  }

  default long storageDoubleWriteLogMinSegSize() {
    return getValueAsInteger(OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MIN_SEG_SIZE);
  }

  default int storageClusterVersion() {
    return getValueAsInteger(OGlobalConfiguration.STORAGE_CLUSTER_VERSION);
  }

  default boolean storagePrintWalPerformanceStatistics() {
    return getValueAsBoolean(OGlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_STATISTICS);
  }

  default int storagePrintWalPerformanceInterval() {
    return getValueAsInteger(OGlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_INTERVAL);
  }

  default String storagePessimisticLocking() {
    return getValueAsString(OGlobalConfiguration.STORAGE_PESSIMISTIC_LOCKING);
  }

  default int walCacheSize() {
    return getValueAsInteger(OGlobalConfiguration.WAL_CACHE_SIZE);
  }

  default int walBufferSize() {
    return getValueAsInteger(OGlobalConfiguration.WAL_BUFFER_SIZE);
  }

  default long walSegmentsInterval() {
    return getValueAsInteger(OGlobalConfiguration.WAL_SEGMENTS_INTERVAL);
  }

  default long walMaxSegmentSize() {
    return getValueAsInteger(OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE);
  }

  default long walMaxSegmentSizePercent() {
    return getValueAsInteger(OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE_PERCENT);
  }

  default long walMinSegSize() {
    return getValueAsInteger(OGlobalConfiguration.WAL_MIN_SEG_SIZE);
  }

  default int walMinCompressedRecordSize() {
    return getValueAsInteger(OGlobalConfiguration.WAL_MIN_COMPRESSED_RECORD_SIZE);
  }

  default long walMaxSize() {
    return getValueAsInteger(OGlobalConfiguration.WAL_MAX_SIZE);
  }

  default boolean walKeepSingleSegment() {
    return getValueAsBoolean(OGlobalConfiguration.WAL_KEEP_SINGLE_SEGMENT);
  }

  default int walCommitTimeout() {
    return getValueAsInteger(OGlobalConfiguration.WAL_COMMIT_TIMEOUT);
  }

  default int walShutdownTimeout() {
    return getValueAsInteger(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT);
  }

  default long walFuzzyCheckpointInterval() {
    return getValueAsInteger(OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL);
  }

  default int walReportAfterOperationsDuringRestore() {
    return getValueAsInteger(OGlobalConfiguration.WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE);
  }

  default int walRestoreBatchSize() {
    return getValueAsInteger(OGlobalConfiguration.WAL_RESTORE_BATCH_SIZE);
  }

  default String walLocation() {
    return getValueAsString(OGlobalConfiguration.WAL_LOCATION);
  }

  default int diskCachePageSize() {
    return getValueAsInteger(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE);
  }

  default long diskCacheFreeSpaceLimit() {
    return getValueAsLong(OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT);
  }

  default boolean recordDownsizingEnabled() {
    return getValueAsBoolean(OGlobalConfiguration.RECORD_DOWNSIZING_ENABLED);
  }

  default int documentBinaryMapping() {
    return getValueAsInteger(OGlobalConfiguration.DOCUMENT_BINARY_MAPPING);
  }

  default int dbPoolMin() {
    return getValueAsInteger(OGlobalConfiguration.DB_POOL_MIN);
  }

  default int dbPoolMax() {
    return getValueAsInteger(OGlobalConfiguration.DB_POOL_MAX);
  }

  default int dbCachedPoolCapacity() {
    return getValueAsInteger(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY);
  }

  default int dbStringCacheSize() {
    return getValueAsInteger(OGlobalConfiguration.DB_STRING_CACHE_SIZE);
  }

  default long dbCachedPoolCleanUpTimeout() {
    return getValueAsLong(OGlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT);
  }

  default int dbPoolAcquireTimeout() {
    return getValueAsInteger(OGlobalConfiguration.DB_POOL_ACQUIRE_TIMEOUT);
  }

  default boolean dbValidation() {
    return getValueAsBoolean(OGlobalConfiguration.DB_VALIDATION);
  }

  default boolean dbCustomSupport() {
    return getValueAsBoolean(OGlobalConfiguration.DB_CUSTOM_SUPPORT);
  }

  default int indexEmbeddedToSbtreebonsaiThreshold() {
    return getValueAsInteger(OGlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD);
  }

  default int indexSbtreebonsaiToEmbeddedThreshold() {
    return getValueAsInteger(OGlobalConfiguration.INDEX_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD);
  }

  default boolean indexSynchronousAutoRebuild() {
    return getValueAsBoolean(OGlobalConfiguration.INDEX_SYNCHRONOUS_AUTO_REBUILD);
  }

  default boolean indexAllowManualIndexes() {
    return getValueAsBoolean(OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES);
  }

  default boolean indexAllowManualIndexesWarning() {
    return getValueAsBoolean(OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES_WARNING);
  }

  default boolean indexIgnoreNullValuesDefault() {
    return getValueAsBoolean(OGlobalConfiguration.INDEX_IGNORE_NULL_VALUES_DEFAULT);
  }

  default int indexCursorPrefetchSize() {
    return getValueAsInteger(OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE);
  }

  default int sbtreeMaxDepth() {
    return getValueAsInteger(OGlobalConfiguration.SBTREE_MAX_DEPTH);
  }

  default int sbtreeMaxKeySize() {
    return getValueAsInteger(OGlobalConfiguration.SBTREE_MAX_KEY_SIZE);
  }

  default int sbtreeMaxEmbeddedValueSize() {
    return getValueAsInteger(OGlobalConfiguration.SBTREE_MAX_EMBEDDED_VALUE_SIZE);
  }

  default int sbtreebonsaiBucketSize() {
    return getValueAsInteger(OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE);
  }

  default int ridBagEmbeddedDefaultSize() {
    return getValueAsInteger(OGlobalConfiguration.RID_BAG_EMBEDDED_DEFAULT_SIZE);
  }

  default int ridBagEmbeddedToSbtreebonsaiThreshold() {
    return getValueAsInteger(OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD);
  }

  default int ridBagSbtreebonsaiToEmbeddedThreshold() {
    return getValueAsInteger(OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD);
  }

  default boolean fileLock() {
    return getValueAsBoolean(OGlobalConfiguration.FILE_LOCK);
  }

  default int fileDeleteDelay() {
    return getValueAsInteger(OGlobalConfiguration.FILE_DELETE_DELAY);
  }

  default int fileDeleteRetry() {
    return getValueAsInteger(OGlobalConfiguration.FILE_DELETE_RETRY);
  }

  default int securityUserPasswordSaltIterations() {
    return getValueAsInteger(OGlobalConfiguration.SECURITY_USER_PASSWORD_SALT_ITERATIONS);
  }

  default int securityUserPasswordSaltCacheSize() {
    return getValueAsInteger(OGlobalConfiguration.SECURITY_USER_PASSWORD_SALT_CACHE_SIZE);
  }

  default String securityUserPasswordDefaultAlgorithm() {
    return getValueAsString(OGlobalConfiguration.SECURITY_USER_PASSWORD_DEFAULT_ALGORITHM);
  }

  default int networkMaxConcurrentSessions() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS);
  }

  default int networkSocketBufferSize() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_BUFFER_SIZE);
  }

  default int networkLockTimeout() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_LOCK_TIMEOUT);
  }

  default int networkSocketTimeout() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT);
  }

  default int networkRequestTimeout() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT);
  }

  default int networkSocketRetry() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY);
  }

  default int networkSocketRetryDelay() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY_DELAY);
  }

  default boolean networkBinaryDnsLoadbalancingEnabled() {
    return getValueAsBoolean(OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_ENABLED);
  }

  default int networkBinaryDnsLoadbalancingTimeout() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT);
  }

  default int networkBinaryMaxContentLength() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_BINARY_MAX_CONTENT_LENGTH);
  }

  default int networkBinaryMinProtocolVersion() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_BINARY_MIN_PROTOCOL_VERSION);
  }

  default boolean networkBinaryDebug() {
    return getValueAsBoolean(OGlobalConfiguration.NETWORK_BINARY_DEBUG);
  }

  default boolean networkBinaryAllowNoToken() {
    return getValueAsBoolean(OGlobalConfiguration.NETWORK_BINARY_ALLOW_NO_TOKEN);
  }

  default boolean networkHttpInstallDefaultCommands() {
    return getValueAsBoolean(OGlobalConfiguration.NETWORK_HTTP_INSTALL_DEFAULT_COMMANDS);
  }

  default String networkHttpServerInfo() {
    return getValueAsString(OGlobalConfiguration.NETWORK_HTTP_SERVER_INFO);
  }

  default int networkHttpMaxContentLength() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_HTTP_MAX_CONTENT_LENGTH);
  }

  default boolean networkHttpStreaming() {
    return getValueAsBoolean(OGlobalConfiguration.NETWORK_HTTP_STREAMING);
  }

  default String networkHttpContentCharset() {
    return getValueAsString(OGlobalConfiguration.NETWORK_HTTP_CONTENT_CHARSET);
  }

  default boolean networkHttpJsonResponseError() {
    return getValueAsBoolean(OGlobalConfiguration.NETWORK_HTTP_JSON_RESPONSE_ERROR);
  }

  default boolean networkHttpJsonpEnabled() {
    return getValueAsBoolean(OGlobalConfiguration.NETWORK_HTTP_JSONP_ENABLED);
  }

  default int networkHttpSessionExpireTimeout() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_HTTP_SESSION_EXPIRE_TIMEOUT);
  }

  default boolean networkHttpSessionCookieSameSite() {
    return getValueAsBoolean(OGlobalConfiguration.NETWORK_HTTP_SESSION_COOKIE_SAME_SITE);
  }

  default boolean networkHttpUseToken() {
    return getValueAsBoolean(OGlobalConfiguration.NETWORK_HTTP_USE_TOKEN);
  }

  default String networkTokenSecretkey() {
    return getValueAsString(OGlobalConfiguration.NETWORK_TOKEN_SECRETKEY);
  }

  default String networkTokenEncryptionAlgorithm() {
    return getValueAsString(OGlobalConfiguration.NETWORK_TOKEN_ENCRYPTION_ALGORITHM);
  }

  default int networkTokenExpireTimeout() {
    return getValueAsInteger(OGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT);
  }

  default boolean initInServletContextListener() {
    return getValueAsBoolean(OGlobalConfiguration.INIT_IN_SERVLET_CONTEXT_LISTENER);
  }

  default boolean profilerEnabled() {
    return getValueAsBoolean(OGlobalConfiguration.PROFILER_ENABLED);
  }

  default int profilerAutodumpInterval() {
    return getValueAsInteger(OGlobalConfiguration.PROFILER_AUTODUMP_INTERVAL);
  }

  default String profilerAutodumpType() {
    return getValueAsString(OGlobalConfiguration.PROFILER_AUTODUMP_TYPE);
  }

  default int profilerMaxvalues() {
    return getValueAsInteger(OGlobalConfiguration.PROFILER_MAXVALUES);
  }

  default long profilerMemorycheckInterval() {
    return getValueAsLong(OGlobalConfiguration.PROFILER_MEMORYCHECK_INTERVAL);
  }

  default int sequenceMaxRetry() {
    return getValueAsInteger(OGlobalConfiguration.SEQUENCE_MAX_RETRY);
  }

  default int sequenceRetryDelay() {
    return getValueAsInteger(OGlobalConfiguration.SEQUENCE_RETRY_DELAY);
  }

  default int classMinimumClusters() {
    return getValueAsInteger(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS);
  }

  default String logSupportsAnsi() {
    return getValueAsString(OGlobalConfiguration.LOG_SUPPORTS_ANSI);
  }

  default String cacheLocalImpl() {
    return getValueAsString(OGlobalConfiguration.CACHE_LOCAL_IMPL);
  }

  default long commandTimeout() {
    return getValueAsLong(OGlobalConfiguration.COMMAND_TIMEOUT);
  }

  default int queryRemoteResultsetPageSize() {
    return getValueAsInteger(OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE);
  }

  default boolean queryRemoteSendExecutionPlan() {
    return getValueAsBoolean(OGlobalConfiguration.QUERY_REMOTE_SEND_EXECUTION_PLAN);
  }

  default boolean queryParallelAuto() {
    return getValueAsBoolean(OGlobalConfiguration.QUERY_PARALLEL_AUTO);
  }

  default long queryParallelMinimumRecords() {
    return getValueAsLong(OGlobalConfiguration.QUERY_PARALLEL_MINIMUM_RECORDS);
  }

  default int queryParallelResultQueueSize() {
    return getValueAsInteger(OGlobalConfiguration.QUERY_PARALLEL_RESULT_QUEUE_SIZE);
  }

  default long queryScanThresholdTip() {
    return getValueAsLong(OGlobalConfiguration.QUERY_SCAN_THRESHOLD_TIP);
  }

  default long queryLimitThresholdTip() {
    return getValueAsLong(OGlobalConfiguration.QUERY_LIMIT_THRESHOLD_TIP);
  }

  default long queryMaxHeapElementsAllowedPerOp() {
    return getValueAsLong(OGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
  }

  default boolean queryLiveSupport() {
    return getValueAsBoolean(OGlobalConfiguration.QUERY_LIVE_SUPPORT);
  }

  default int statementCacheSize() {
    return getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE);
  }

  default String sqlGraphConsistencyMode() {
    return getValueAsString(OGlobalConfiguration.SQL_GRAPH_CONSISTENCY_MODE);
  }

  default int clientChannelMaxPool() {
    return getValueAsInteger(OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL);
  }

  default int clientConnectPoolWaitTimeout() {
    return getValueAsInteger(OGlobalConfiguration.CLIENT_CONNECT_POOL_WAIT_TIMEOUT);
  }

  default int clientDbReleaseWaitTimeout() {
    return getValueAsInteger(OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT);
  }

  default boolean clientUseSsl() {
    return getValueAsBoolean(OGlobalConfiguration.CLIENT_USE_SSL);
  }

  default String clientSslKeystore() {
    return getValueAsString(OGlobalConfiguration.CLIENT_SSL_KEYSTORE);
  }

  default String clientSslKeystorePassword() {
    return getValueAsString(OGlobalConfiguration.CLIENT_SSL_KEYSTORE_PASSWORD);
  }

  default String clientSslTruststore() {
    return getValueAsString(OGlobalConfiguration.CLIENT_SSL_TRUSTSTORE);
  }

  default String clientSslTruststorePassword() {
    return getValueAsString(OGlobalConfiguration.CLIENT_SSL_TRUSTSTORE_PASSWORD);
  }

  default boolean serverOpenAllDatabasesAtStartup() {
    return getValueAsBoolean(OGlobalConfiguration.SERVER_OPEN_ALL_DATABASES_AT_STARTUP);
  }

  default String serverDatabasePath() {
    return getValueAsString(OGlobalConfiguration.SERVER_DATABASE_PATH);
  }

  default int serverChannelCleanDelay() {
    return getValueAsInteger(OGlobalConfiguration.SERVER_CHANNEL_CLEAN_DELAY);
  }

  default boolean serverCacheFileStatic() {
    return getValueAsBoolean(OGlobalConfiguration.SERVER_CACHE_FILE_STATIC);
  }

  default String serverLogDumpClientExceptionLevel() {
    return getValueAsString(OGlobalConfiguration.SERVER_LOG_DUMP_CLIENT_EXCEPTION_LEVEL);
  }

  default boolean serverLogDumpClientExceptionFullstacktrace() {
    return getValueAsBoolean(OGlobalConfiguration.SERVER_LOG_DUMP_CLIENT_EXCEPTION_FULLSTACKTRACE);
  }

  default boolean serverBackwardCompatibility() {
    return getValueAsBoolean(OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY);
  }

  default long distributedDumpStatsEvery() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_DUMP_STATS_EVERY);
  }

  default long distributedCrudTaskSynchTimeout() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT);
  }

  default long distributedMaxStartupDelay() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_MAX_STARTUP_DELAY);
  }

  default long distributedCommandTaskSynchTimeout() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT);
  }

  default long distributedCommandQuickTaskSynchTimeout() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT);
  }

  default long distributedCommandLongTaskSynchTimeout() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_COMMAND_LONG_TASK_SYNCH_TIMEOUT);
  }

  default long distributedDeploydbTaskSynchTimeout() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT);
  }

  default long distributedDeploychunkTaskSynchTimeout() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_DEPLOYCHUNK_TASK_SYNCH_TIMEOUT);
  }

  default int distributedDeploydbTaskCompression() {
    return getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION);
  }

  default long distributedAsynchResponsesTimeout() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT);
  }

  default long distributedTxExpireTimeout() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT);
  }

  default int distributedRequestChannels() {
    return getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_REQUEST_CHANNELS);
  }

  default int distributedResponseChannels() {
    return getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_RESPONSE_CHANNELS);
  }

  default long distributedHeartbeatTimeout() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_HEARTBEAT_TIMEOUT);
  }

  default boolean distributedCheckHealthCanOfflineServer() {
    return getValueAsBoolean(OGlobalConfiguration.DISTRIBUTED_CHECK_HEALTH_CAN_OFFLINE_SERVER);
  }

  default long distributedCheckHealthEvery() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_CHECK_HEALTH_EVERY);
  }

  default long distributedAutoRemoveOfflineServers() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_AUTO_REMOVE_OFFLINE_SERVERS);
  }

  default long distributedPublishNodeStatusEvery() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_PUBLISH_NODE_STATUS_EVERY);
  }

  default int distributedReplicationProtocolVersion() {
    return getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_REPLICATION_PROTOCOL_VERSION);
  }

  default int distributedDbWorkerthreads() {
    return getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_DB_WORKERTHREADS);
  }

  default String distributedBackupDirectory() {
    return getValueAsString(OGlobalConfiguration.DISTRIBUTED_BACKUP_DIRECTORY);
  }

  default int distributedConcurrentTxMaxAutoretry() {
    return getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
  }

  default int distributedConcurrentTxAutoretryDelay() {
    return getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);
  }

  default int distributedTransactionSequenceSetSize() {
    return getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_TRANSACTION_SEQUENCE_SET_SIZE);
  }

  default long distributedDatabaseOnlineGracePeriod() {
    return getValueAsLong(OGlobalConfiguration.DISTRIBUTED_DATABASE_ONLINE_GRACE_PERIOD);
  }

  default String dbDocumentSerializer() {
    return getValueAsString(OGlobalConfiguration.DB_DOCUMENT_SERIALIZER);
  }

  default String clientKrb5Config() {
    return getValueAsString(OGlobalConfiguration.CLIENT_KRB5_CONFIG);
  }

  default String clientKrb5Ccname() {
    return getValueAsString(OGlobalConfiguration.CLIENT_KRB5_CCNAME);
  }

  default String clientKrb5Ktname() {
    return getValueAsString(OGlobalConfiguration.CLIENT_KRB5_KTNAME);
  }

  default String clientConnectionStrategy() {
    return getValueAsString(OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY);
  }

  default boolean clientConnectionFetchHostList() {
    return getValueAsBoolean(OGlobalConfiguration.CLIENT_CONNECTION_FETCH_HOST_LIST);
  }

  default String clientCredentialInterceptor() {
    return getValueAsString(OGlobalConfiguration.CLIENT_CREDENTIAL_INTERCEPTOR);
  }

  default String clientCiKeyalgorithm() {
    return getValueAsString(OGlobalConfiguration.CLIENT_CI_KEYALGORITHM);
  }

  default String clientCiCiphertransform() {
    return getValueAsString(OGlobalConfiguration.CLIENT_CI_CIPHERTRANSFORM);
  }

  default String clientCiKeystoreFile() {
    return getValueAsString(OGlobalConfiguration.CLIENT_CI_KEYSTORE_FILE);
  }

  default String clientCiKeystorePassword() {
    return getValueAsString(OGlobalConfiguration.CLIENT_CI_KEYSTORE_PASSWORD);
  }

  default boolean createDefaultUsers() {
    return getValueAsBoolean(OGlobalConfiguration.CREATE_DEFAULT_USERS);
  }

  default boolean warningDefaultUsers() {
    return getValueAsBoolean(OGlobalConfiguration.WARNING_DEFAULT_USERS);
  }

  default String serverSecurityFile() {
    return getValueAsString(OGlobalConfiguration.SERVER_SECURITY_FILE);
  }

  default boolean spatialEnableDirectWktReader() {
    return getValueAsBoolean(OGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER);
  }

  @Deprecated
  default String oauth2Secretkey() {
    return getValueAsString(OGlobalConfiguration.OAUTH2_SECRETKEY);
  }

  default boolean autoCloseAfterDelay() {
    return getValueAsBoolean(OGlobalConfiguration.AUTO_CLOSE_AFTER_DELAY);
  }

  default int autoCloseDelay() {
    return getValueAsInteger(OGlobalConfiguration.AUTO_CLOSE_DELAY);
  }

  default boolean distributed() {
    return getValueAsBoolean(OGlobalConfiguration.DISTRIBUTED);
  }

  default String distributedNodeName() {
    return getValueAsString(OGlobalConfiguration.DISTRIBUTED_NODE_NAME);
  }

  default boolean clientChannelIdleClose() {
    return getValueAsBoolean(OGlobalConfiguration.CLIENT_CHANNEL_IDLE_CLOSE);
  }

  default int clientChannelIdleTimeout() {
    return getValueAsInteger(OGlobalConfiguration.CLIENT_CHANNEL_IDLE_TIMEOUT);
  }

  default boolean distributedAutoCreateClusters() {
    return getValueAsBoolean(OGlobalConfiguration.DISTRIBUTED_AUTO_CREATE_CLUSTERS);
  }

  default int enterpriseMetricsMax() {
    return getValueAsInteger(OGlobalConfiguration.ENTERPRISE_METRICS_MAX);
  }

  default boolean executorDebugTraceSource() {
    return getValueAsBoolean(OGlobalConfiguration.EXECUTOR_DEBUG_TRACE_SOURCE);
  }

  default int executorPoolMaxSize() {
    return getValueAsInteger(OGlobalConfiguration.EXECUTOR_POOL_MAX_SIZE);
  }

  default int executorPoolIoMaxSize() {
    return getValueAsInteger(OGlobalConfiguration.EXECUTOR_POOL_IO_MAX_SIZE);
  }

  default boolean executorPoolIoEnabled() {
    return getValueAsBoolean(OGlobalConfiguration.EXECUTOR_POOL_IO_ENABLED);
  }

  Object getValue(OGlobalConfiguration gc);

  default boolean getValueAsBoolean(OGlobalConfiguration gc) {
    Object v = getValue(gc);
    return v instanceof Boolean ? (Boolean) v : Boolean.parseBoolean(v.toString());
  }

  default String getValueAsString(OGlobalConfiguration gc) {
    Object v = getValue(gc);
    return v != null ? v.toString() : null;
  }

  default int getValueAsInteger(OGlobalConfiguration gc) {
    Object v = getValue(gc);
    return (int)
        (v instanceof Number ? ((Number) v).intValue() : OFileUtils.getSizeAsNumber(v.toString()));
  }

  default long getValueAsLong(OGlobalConfiguration gc) {
    Object v = getValue(gc);
    return v instanceof Number
        ? ((Number) v).longValue()
        : OFileUtils.getSizeAsNumber(v.toString());
  }

  default float getValueAsFloat(OGlobalConfiguration gc) {
    Object v = getValue(gc);
    return v instanceof Float ? (Float) v : Float.parseFloat(v.toString());
  }

  public default <T extends Enum<T>> T getValueAsEnum(
      final OGlobalConfiguration gc, Class<T> enumType) {
    final Object value = getValue(gc);

    if (value == null) return null;

    if (enumType.isAssignableFrom(value.getClass())) {
      return enumType.cast(value);
    } else if (value instanceof String) {
      final String presentation = value.toString();
      return Enum.valueOf(enumType, presentation);
    } else {
      throw new ClassCastException(
          "Value " + value + " can not be cast to enumeration " + enumType.getSimpleName());
    }
  }
}
