package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

public class OPartitionedReCreatableDatabasePool {
    private final OrientDB orientdb;
    private ODatabasePool pool;
    private final String dbName;
    private final String userName;
    private final String password;
    private final int maxSize;

    public OPartitionedReCreatableDatabasePool(OrientDB orientdb, String dbName, String userName, String password, int maxSize) {
        this.orientdb = orientdb;
        this.dbName = dbName;
        this.userName = userName;
        this.password = password;
        this.maxSize = maxSize;
        reCreatePool();

    }

    public void reCreatePool() {
        close();
        this.pool = new ODatabasePool(this.orientdb, this.dbName, this.userName, this.password);
    }

    public void close() {
        if (this.pool != null)
            this.pool.close();

        this.pool = null;
    }

    public ODatabaseDocument acquire() {
        if (this.pool != null)
            return this.pool.acquire();
        return null;
    }
}
