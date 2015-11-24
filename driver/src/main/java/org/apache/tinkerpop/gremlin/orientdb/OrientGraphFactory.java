package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseFactory;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;


public final class OrientGraphFactory {
    public static String ADMIN = "admin";
    protected final String url;
    protected final String user;
    protected final String password;
    protected volatile OPartitionedDatabasePool pool;

    public OrientGraphFactory(String url) {
        this.url = url;
        this.user = ADMIN;
        this.password = ADMIN;
    }

    public OrientGraphFactory(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    /**
     * Gets transactional graph with the database from pool if pool is configured. Otherwise creates a graph with new db instance. The
     * Graph instance inherits the factory's configuration.
     *
     * @param create
     *          if true automatically creates database if database with given URL does not exist
     * @param open
     *          if true automatically opens the database
     */
    //TODO: allow to open with these properties
    public OrientGraph getNoTx(boolean create, boolean open) {
        return getGraph(create, open, false);
    }

    public OrientGraph getNoTx() {
        return getNoTx(true, true);
    }

    public OrientGraph getTx(boolean create, boolean open) {
        return getGraph(create, open, true);
    }

    public OrientGraph getTx() {
        return getTx(true, true);
    }

    protected OrientGraph getGraph(boolean create, boolean open, boolean transactional) {
        final OrientGraph g;
        final Configuration config = getConfiguration(create, open, transactional);
        if (pool != null) {
            g = new OrientGraph(pool, config);
        } else {
            g = new OrientGraph(getDatabase(create, open), config);
        }
        return g;
    }

    protected Configuration getConfiguration(boolean create, boolean open, boolean transactional) {
        return new BaseConfiguration() {{
            setProperty(Graph.GRAPH, OrientGraph.class.getName());
            setProperty(OrientGraph.CONFIG_URL, url);
            setProperty(OrientGraph.CONFIG_USER, user);
            setProperty(OrientGraph.CONFIG_PASS, password);
            setProperty(OrientGraph.CONFIG_CREATE, create);
            setProperty(OrientGraph.CONFIG_OPEN, open);
            setProperty(OrientGraph.CONFIG_TRANSACTIONAL, transactional);
        }};
    }

    /**
     * @param create if true automatically creates database if database with given URL does not exist
     * @param open   if true automatically opens the database
     */
    protected ODatabaseDocumentTx getDatabase(boolean create, boolean open) {
        final ODatabaseDocumentTx db = new ODatabaseFactory().createDatabase("graph", url);
        if (!db.getURL().startsWith("remote:") && !db.exists()) {
            if (create) db.create();
            else if (open) throw new ODatabaseException("Database '" + url + "' not found");
        } else if (open) db.open(user, password);

        return db;
    }
}
