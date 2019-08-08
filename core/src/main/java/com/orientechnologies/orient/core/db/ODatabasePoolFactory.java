package com.orientechnologies.orient.core.db;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.util.Objects;

/**
 * Implementation of database pool factory which allows store database pools associated with users
 * Works like LRU cache, using {@link ConcurrentLinkedHashMap<PoolIdentity, ODatabasePool>} as store for databases pools.
 *
 *
 * How it works:
 * 1. Pool store capacity is 100
 * 2. We have 100 pools in store
 * 3. We want get 101 pool
 * 4. First we will remove pool which used long time ago from pool store
 * 5. Then we add new pool from point 3 to pool store
 *
 * @author Vitalii Honchar (weaxme@gmail.com)
 */
public class ODatabasePoolFactory extends OOrientListenerAbstract {

    /**
     * Max size of connections which one pool can contains
     */
    private volatile int maxPoolSize = 64;

    private boolean closed;
    private final ConcurrentLinkedHashMap<PoolIdentity, ODatabasePool> poolStore;
    private final OrientDB orientDB;

    /**
     * @param orientDB instance of {@link OrientDB} which will be used for create new database pools {@link ODatabasePool}
     */
    public ODatabasePoolFactory(OrientDB orientDB) {
        this(orientDB, 100);
    }

    /**
     * @param orientDB instance of {@link OrientDB} which will be used for create new database pools {@link ODatabasePool}
     * @param capacity capacity of pool store, by default is 100
     */
    public ODatabasePoolFactory(OrientDB orientDB, int capacity) {
        poolStore = new ConcurrentLinkedHashMap.Builder<PoolIdentity, ODatabasePool>()
                .maximumWeightedCapacity(capacity)
                .listener((identity, databasePool) -> databasePool.close())
                .build();
        this.orientDB = orientDB;

        Orient.instance().registerWeakOrientStartupListener(this);
        Orient.instance().registerWeakOrientShutdownListener(this);
    }

    /**
     * Get or create database pool instance for given user
     * @param database name of database
     * @param username name of user which need access to database
     * @param password user password
     * @return {@link ODatabasePool} which is new instance of pool or instance from pool storage
     */
    public ODatabasePool get(String database, String username, String password) {
        checkForClose();

        PoolIdentity identity = new PoolIdentity(database, username, password);
        ODatabasePool pool = poolStore.get(identity);
        if (pool != null) {
            return pool;
        }

        return poolStore.computeIfAbsent(identity, indent -> {
            OrientDBConfigBuilder builder = OrientDBConfig.builder();
            builder.addConfig(OGlobalConfiguration.DB_POOL_MAX, maxPoolSize);
            return new ODatabasePool(orientDB, identity.database, identity.username, identity.password, builder.build());
        });
    }

    /**
     * Close all open pools and clear pool storage
     */
    public void reset() {
        poolStore.forEach((identity, pool) -> pool.close());
        poolStore.clear();
    }

    /**
     * Close all open pools and clear pool storage. Set flag closed to true, so this instance can't be used again.
     * Need create new instance of {@link ODatabasePoolFactory} after close one of factories.
     */
    public void close() {
        if (!isClosed()) {
            closed = true;
            reset();
        }
    }

    /**
     * @return true if factory is closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * @return max pool size. Default is 64
     */
    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    /**
     * Set max pool connections size which will be used for create new {@link ODatabasePool}
     * @param maxPoolSize max pool connections size
     * @return this instance
     */
    public ODatabasePoolFactory setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    /**
     * OrientDB callback hook which invokes when OrientDB shutdowns. Close this pool factory
     */
    @Override
    public void onShutdown() {
        close();
    }

    /**
     * @throws IllegalStateException if pool factory is closed
     */
    private void checkForClose() {
        if (closed) {
            throw new IllegalStateException("Pool factory is closed");
        }
    }

    /**
     * Identity which will be used as key in {@link ODatabasePoolFactory#poolStore}
     */
    private static class PoolIdentity {
        private final String database;
        private final String username;
        private final String password;

        /**
         * @param database database name
         * @param username user name
         * @param password user password
         */
        public PoolIdentity(String database, String username, String password) {
            this.database = database;
            this.username = username;
            this.password = password;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PoolIdentity that = (PoolIdentity) o;
            return Objects.equals(database, that.database) &&
                    Objects.equals(username, that.username) &&
                    Objects.equals(password, that.password);
        }

        @Override
        public int hashCode() {
            return Objects.hash(database, username, password);
        }
    }
}