package com.orientechnologies.orient.unit;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.rules.ExternalResource;

public class ODBTestSupportRule extends ExternalResource {
    private final AbstractOrientDBRule orientDBRule;

    public ODBTestSupportRule(AbstractOrientDBRule orientDBRule) {
        this.orientDBRule = orientDBRule;
    }

    @Override
    protected void before() throws Throwable {
        orientDBRule().before();
        super.before();
    }

    @Override
    protected void after() {
        super.after();
        orientDBRule().after();
    }

    protected final AbstractOrientDBRule orientDBRule(){
        assert orientDBRule != null;
        return orientDBRule;
    }

    // TODO: remove cast
    public int nodesCount(){
        return ((EmbeddedOrientDBRule)orientDBRule()).getCluster().size();
    }

    public AbstractOrientDBRule.IDatabaseConnectionInfo getConnectionInfo(){
        return orientDBRule().getConnectionInfo();
    }

    public AbstractOrientDBRule.IDatabaseConnectionInfo getConnectionInfo(int idxNode){
        return orientDBRule().getConnectionInfo(idxNode);
    }

    public String remoteConnectionString(){
        return connectionString("remote", getConnectionInfo());
    }

    public String localConnectionString(){
        return connectionString("plocal", getConnectionInfo());
    }

    static protected final String connectionString(String prefix, AbstractOrientDBRule.IDatabaseConnectionInfo ci){
        return String.format("%s:%s:%s/%s", prefix, ci.getAddress(), ci.getBinaryPort(), ci.getDatabase());
    }

    protected ODatabaseDocumentTx remoteDocumentDBFromGPool(AbstractOrientDBRule.IDatabaseConnectionInfo ci){
        final ODatabaseDocumentPool pool = ODatabaseDocumentPool.global();
        final ODatabaseDocumentTx retVal = pool.acquire(connectionString("remote", ci), ci.getLogin(), ci.getPassword());
        return retVal;
    }

    public ODatabaseDocumentTx remoteDocumentDBFromGPool(){
        final AbstractOrientDBRule.IDatabaseConnectionInfo ci = getConnectionInfo();
        return remoteDocumentDBFromGPool(ci);
    }

    public ODatabaseDocumentTx remoteDocumentDBFromGPool(int idxNode){
        final AbstractOrientDBRule.IDatabaseConnectionInfo ci = orientDBRule.getConnectionInfo(idxNode);
        return remoteDocumentDBFromGPool(ci);
    }

    public ODatabaseFlat localDatabaseFlat(){
        final AbstractOrientDBRule.IDatabaseConnectionInfo ci = getConnectionInfo();
        final ODatabaseFlat db = new ODatabaseFlat(localConnectionString()).open(ci.getLogin(), ci.getPassword());
        return db;
    }

    public <RET> RET executeSQLCommandWithParams(String query, Object...params) throws Exception{
        final ODatabaseDocumentTx db = remoteDocumentDBFromGPool();
        try {
            final String statement = String.format(query);
            return db.command(new OCommandSQL(statement)).execute(params);
        }finally{
            db.close();
        }
    }

    public <RET> RET executeSQLCommandWithParams(ODatabaseDocumentTx db, String query, Object...args) throws Exception{
        final String statement = String.format(query, args);
        return db.command(new OCommandSQL(statement)).execute(args);
    }

    public static ODBTestSupportRule create(AbstractOrientDBRule odbRule) {
        return new ODBTestSupportRule(odbRule);
    }
}
