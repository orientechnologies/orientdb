package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

public class OPartitionedReCreatableDatabasePool {
    private OPartitionedDatabasePool pool;
    public final String url;
    public final String userName;
    public final String password;
    public final int maxSize;
    public final boolean autoCreate;

    public OPartitionedReCreatableDatabasePool(String url, String userName, String password, int maxSize, boolean autoCreate) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.maxSize = maxSize;
        this.autoCreate = autoCreate;
        reCreatePool();
    }

    public void reCreatePool() {
        close();
        this.pool = new OPartitionedDatabasePool(this.url, this.userName, this.password, this.maxSize).setAutoCreate(this.autoCreate);
    }

    public void close() {
        if (this.pool != null)
            this.pool.close();

        this.pool = null;
    }

    public ODatabaseDocumentTx acquire() {
        if (this.pool != null)
            return this.pool.acquire();

        return null;
    }
}
