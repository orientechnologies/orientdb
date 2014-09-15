package com.orientechnologies.orient.unit;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

abstract class AbstractOrientDBRule<BI extends AbstractOrientDBRule.IDatabaseConnectionInfo> extends ExternalResource {

    public static interface IDatabaseConnectionInfo {
        static final String DEFAULT_DATABASE = "testdb";
        static final String DEFAULT_DATABASE_TYPE = "graph";
        static final String DEFAULT_STORAGE_MODE = "plocal";

        String getDatabase();
        String getDatabaseType();
        String getStorageMode();
        String getLogin();
        String getPassword();
        String getAddress();
        int getBinaryPort();
    }

    protected Logger logger = LoggerFactory.getLogger(AbstractOrientDBRule.class);
    protected final BI buildInfo;

    protected AbstractOrientDBRule(BI buildInfo) {
        this.buildInfo = buildInfo;
    }

    @Override
    public void before() throws Throwable {
        super.before();

        final boolean need2recreateDB = buildInfo.getDatabaseType() != null && !buildInfo.getDatabaseType().isEmpty();
        if (need2recreateDB) {
            recreateDB();
        }

        if(logger.isInfoEnabled()) {
            final IDatabaseConnectionInfo ci = getConnectionInfo();
            final String connection = String.format("connect remote:%s:%s/%s %s %s", ci.getAddress(), ci.getBinaryPort(),
                        ci.getDatabase(), ci.getLogin(), ci.getPassword()
                    );

            logger.info(need2recreateDB ? "\nThe database '{}' has been created (on behalf of user '{}') and ready to use.\n" +
                            "The following command might be used to establish connection: {}"
                            : "\nExisted database '{}' and user '{}'.\n The following command " +
                            "might be used to establish connection: {}"
                    ,
                    buildInfo.getDatabase(),
                    buildInfo.getLogin(),
                    connection
            );
        }
        assert checkIfDbExists(buildInfo.getStorageMode());
    }

    public IDatabaseConnectionInfo getConnectionInfo(){
        return buildInfo;
    }

    public abstract IDatabaseConnectionInfo getConnectionInfo(int idx);


    protected static final OServerAdmin createAndConnectServerAdmin(IDatabaseConnectionInfo ci) throws IOException {
        final OServerAdmin serverAdmin = new OServerAdmin( ODBTestSupportRule.connectionString("remote", ci) );
        try {
            serverAdmin.connect(ci.getLogin(), ci.getPassword());
        }catch (IOException e){
            serverAdmin.close();
            throw e;
        }
        return serverAdmin;
    }

    public void recreateDB(String dbtype) throws Exception {
        OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
        final IDatabaseConnectionInfo ci = getConnectionInfo();

        logger.debug("User '{}'  will be used for a database creation.", ci.getLogin());

        final OServerAdmin sa = new OServerAdmin( String.format("%s:%s/%s", ci.getAddress(), ci.getBinaryPort(), ci.getDatabase()) );
        sa.connect(ci.getLogin(), ci.getPassword());
        try {
            if (sa.existsDatabase(ci.getStorageMode())) {
                logger.warn("Database '{}' already exists, so it will be dropped", ci.getDatabase());
                sa.dropDatabase(ci.getStorageMode());
            }

//            sa.createDatabase(ci.getDatabaseType(), ci.getStorageMode());
        } finally {
            sa.close();
        }

        final OServerAdmin sa2 = new OServerAdmin( String.format("remote:%s:%s/%s", ci.getAddress(), ci.getBinaryPort(), ci.getDatabase()) );
        sa.connect(ci.getLogin(), ci.getPassword());
        try {
            sa2.createDatabase(ci.getDatabaseType(), ci.getStorageMode());
        } finally {
            sa2.close();
        }
    }

    public void recreateDB() throws Exception {
        if (buildInfo.getDatabaseType() == null || buildInfo.getDatabaseType().isEmpty()) {
            throw new IllegalArgumentException("Need to set database type to the Rule");
        }
        recreateDB(buildInfo.getDatabaseType());
    }

    public boolean checkIfDbExists(String storageType) throws IOException {
        final OServerAdmin serverAdmin = createAndConnectServerAdmin(getConnectionInfo());
        try {
            return serverAdmin.existsDatabase(storageType);
        } finally {
            serverAdmin.close();
        }
    }

    @Override
    public void after() {
        super.after();
    }

    public String getDatabase(){
        return buildInfo.getDatabase();
    }

    public String getDatabaseType(){
        return buildInfo.getDatabaseType();
    }

    public String getStorageMode(){
        return buildInfo.getStorageMode();
    }

    public String getUser(){
        return buildInfo.getLogin();
    }

    public String getPassword(){
        return buildInfo.getPassword();
    }
}
