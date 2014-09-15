package com.orientechnologies.orient.unit;

import org.slf4j.LoggerFactory;


public class RemoteOrientDBRule extends AbstractOrientDBRule<RemoteOrientDBRule.RemoteRuleBuilder.RemoteRuleBuildInfo> {
    protected RemoteOrientDBRule(RemoteRuleBuilder.RemoteRuleBuildInfo buildInfo) {
        super(buildInfo);
        logger = LoggerFactory.getLogger(RemoteOrientDBRule.class);
    }

    // TODO: Needs to be implemented
    @Override
    public IDatabaseConnectionInfo getConnectionInfo(int idx) {
        throw new UnsupportedOperationException("Is not implemented yet");
    }


    public static RemoteRuleBuilder build(){
        return new RemoteRuleBuilder();
    }

    public static class RemoteRuleBuilder extends ConnectionInfo.ConnectionInfoBuilder<RemoteRuleBuilder, RemoteRuleBuilder.RemoteRuleBuildInfo> {

        public static class RemoteRuleBuildInfo extends ConnectionInfo implements AbstractOrientDBRule.IDatabaseConnectionInfo{
            private String database = DEFAULT_DATABASE;
            private String databaseType = DEFAULT_DATABASE_TYPE;
            private String storageMode = DEFAULT_STORAGE_MODE;

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

        public RemoteRuleBuilder() {
            super(new RemoteRuleBuildInfo());
        }

        protected RemoteRuleBuilder(RemoteRuleBuildInfo buildInfo) {
            super(buildInfo);
        }

        private RemoteOrientDBRule createRuleAndApplyConfig(String databaseType){
            try {
                buildInfo.databaseType = databaseType;
                return new RemoteOrientDBRule(buildInfo);
            } catch (Exception e) {
                throw new RuntimeException("Can't instantiate corresponding rule ('" + RemoteOrientDBRule.class.getName() + "')", e );
            }
        }

        public RemoteOrientDBRule createGraphDB(){
            return createRuleAndApplyConfig("graph");
        }

        public RemoteOrientDBRule createDocumentDB(){
            return createRuleAndApplyConfig("document");
        }

        public RemoteOrientDBRule withoutDatabaseCreation(){
            return createRuleAndApplyConfig(null);
        }
    }
}