package com.orientechnologies.orient.unit;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.orientechnologies.orient.unit.cluster.AbstractOrientDBCluster;
import com.orientechnologies.orient.unit.cluster.ClusterFactory;
import org.slf4j.LoggerFactory;

public class EmbeddedOrientDBRule extends AbstractOrientDBRule<EmbeddedOrientDBRule.EmbeddedRuleBuilder.EmbeddedRuleBuildInfo> {
    private static final String DEFAULT_ORIENTDB_SERVER_CONFIG_RESOURCE_PATH = "default-orientdb-test-server-config.xml";

    private AbstractOrientDBCluster cluster;

    protected EmbeddedOrientDBRule(EmbeddedRuleBuilder.EmbeddedRuleBuildInfo buildInfo)
    {
        super(buildInfo);
        logger = LoggerFactory.getLogger(EmbeddedOrientDBRule.class);
    }

    @Override
    public void before() throws Throwable {
        cluster = ClusterFactory.createCluster(buildInfo);

        if(cluster.getAdminName() == null){
            throw new IllegalStateException("Database user did not set and hasn't been determined automatically.");
        }

        super.before();
    }

    @Override
    public void recreateDB(final String dbtype) throws Exception {
        synchronized (cluster) {

            if( !AbstractOrientDBCluster.Status.OFFLINE.equals(cluster.getStatus())){
                cluster.stop();
            }

            final boolean isEmbedded = buildInfo.isEmbedded();

            if(!isEmbedded) {
                cluster = ClusterFactory.rebuildCluster(cluster);
            }
            cluster.start();
            super.recreateDB(dbtype);

            if(!isEmbedded) {
                // wait while the DB will be available on all nodes;
                final OServer node = cluster.getNode(0);
                for (boolean dbAvailable = false; !dbAvailable; ) {
                    ODistributedServerManager dm = node.getDistributedManager();

                    if (((OHazelcastPlugin) dm).getAvailableNodes(buildInfo.database) >= cluster.size()) {
                        dbAvailable = true;
                    } else {
                        Thread.sleep(500);
                    }
                    final ODocument doc = dm.getClusterConfiguration();
                    assert doc != null;
                }
            }
        }
    }

    @Override
    public void after() {
        if(cluster != null){
            try {
                cluster.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        super.after();
    }

    public final AbstractOrientDBCluster getCluster(){
        return cluster;
    }

    @Override
    public IDatabaseConnectionInfo getConnectionInfo(int nodeIndex){
        final IDatabaseConnectionInfo ci = getConnectionInfo();
        return new DBConnectionInfo(ci.getDatabase(), ci.getDatabaseType(), ci.getStorageMode(),
                cluster.getAdminName(nodeIndex), cluster.getAdminPassword(nodeIndex), ci.getAddress(),
                buildInfo.getBinaryPorts()[nodeIndex]);
    }

    public static EmbeddedRuleBuilder build(){
        return new EmbeddedRuleBuilder();
    }

    public static class EmbeddedRuleBuilder extends ClusterFactory.ClusterBuildInfoBuilder<EmbeddedRuleBuilder, EmbeddedRuleBuilder.EmbeddedRuleBuildInfo> {

        protected static class EmbeddedRuleBuildInfo extends ClusterFactory.ClusterBuildInfo implements AbstractOrientDBRule.IDatabaseConnectionInfo{
            protected String database = DEFAULT_DATABASE;
            protected String databaseType = DEFAULT_DATABASE_TYPE;
            protected String storageMode = DEFAULT_STORAGE_MODE;

            @Override
            public String getDatabase() {
                return database;
            }

            @Override
            public String getDatabaseType() {
                return databaseType;
            }

            @Override
            public String getStorageMode() {
                return storageMode;
            }

            @Override
            public int getBinaryPort() {
                return getBinaryPorts()[0];
            }
        }

        public EmbeddedRuleBuilder() {
            super(new EmbeddedRuleBuildInfo());
        }

        private EmbeddedOrientDBRule createRuleAndApplyConfig(String databaseType){
            try {
                buildInfo.databaseType = databaseType;

                if(!buildInfo.isServerConfigProvided()){
                    LoggerFactory.getLogger(EmbeddedRuleBuilder.class).warn("Server Config has not been specified, therefore Default Server Config will be used.");
                    useServerConfigResource(DEFAULT_ORIENTDB_SERVER_CONFIG_RESOURCE_PATH);
                }

                return new EmbeddedOrientDBRule(buildInfo);
            } catch (Exception e) {
                throw new RuntimeException("Can't instantiate corresponding rule ('" + EmbeddedOrientDBRule.class.getName() + "')", e );
            }
        }

        public EmbeddedOrientDBRule createGraphDB() {
            return createRuleAndApplyConfig("graph");
        }

        public EmbeddedOrientDBRule createDocumentDB() {
            return createRuleAndApplyConfig("document");
        }

        public EmbeddedOrientDBRule withoutDatabaseCreation() {
            return createRuleAndApplyConfig(null);
        }
    }

    private static class DBConnectionInfo implements IDatabaseConnectionInfo{
        private final String database;
        private final String databaseType;
        private final String storageMode;
        private final String login;
        private final String password;
        private final String address;
        private final int binaryPort;

        private DBConnectionInfo(String database, String databaseType, String storageMode, String login, String password, String address, int binaryPort) {
            this.database = database;
            this.databaseType = databaseType;
            this.storageMode = storageMode;
            this.login = login;
            this.password = password;
            this.address = address;
            this.binaryPort = binaryPort;
        }

        @Override
        public String getDatabase() {
            return database;
        }

        @Override
        public String getDatabaseType() {
            return databaseType;
        }

        @Override
        public String getStorageMode() {
            return storageMode;
        }

        @Override
        public String getLogin() {
            return login;
        }

        @Override
        public String getPassword() {
            return password;
        }

        @Override
        public String getAddress() {
            return address;
        }

        @Override
        public int getBinaryPort() {
            return binaryPort;
        }
    }
}