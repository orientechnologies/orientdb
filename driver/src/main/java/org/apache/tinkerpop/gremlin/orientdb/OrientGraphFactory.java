package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;

public final class OrientGraphFactory {
    public static String ADMIN = "admin";
    protected final String url;
    protected final String user;
    protected final String password;
    protected Configuration configuration;
    protected volatile OPartitionedReCreatableDatabasePool pool;
    protected boolean labelAsClassName;

    public OrientGraphFactory(String url) {
        this(url, ADMIN, ADMIN);
    }

    public OrientGraphFactory(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.labelAsClassName = true;
    }

    public OrientGraphFactory(Configuration config) {
        this(config.getString(OrientGraph.CONFIG_URL, "memory:test-" + Math.random()));
        this.configuration = config;
    }

    /**
     * Gets transactional graph with the database from pool if pool is
     * configured. Otherwise creates a graph with new db instance. The Graph
     * instance inherits the factory's configuration.
     *
     * @param create
     *            if true automatically creates database if database with given
     *            URL does not exist
     * @param open
     *            if true automatically opens the database
     */
    // TODO: allow to open with these properties
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
            g = new OrientGraph(getDatabase(create, open), config, user, password);
        }
        initGraph(g);
        return g;
    }

    protected void initGraph(OrientGraph g) {
        final ODatabaseDocumentTx db = g.getRawDatabase();
        boolean txActive = db.getTransaction().isActive();

        if (txActive)
            // COMMIT TX BEFORE ANY SCHEMA CHANGES
            db.commit();

        OSchema schema = db.getMetadata().getSchema();
        if (!schema.existsClass(OClass.VERTEX_CLASS_NAME))
            schema.createClass(OClass.VERTEX_CLASS_NAME).setOverSize(2);
        if (!schema.existsClass(OClass.EDGE_CLASS_NAME))
            schema.createClass(OClass.EDGE_CLASS_NAME);

        if (txActive) {
            // REOPEN IT AGAIN
            db.begin();
        }
    }

    protected Configuration getConfiguration(boolean create, boolean open, boolean transactional) {
        if (configuration != null)
            return configuration;
        else
            return new BaseConfiguration() {
                {
                    setProperty(Graph.GRAPH, OrientGraph.class.getName());
                    setProperty(OrientGraph.CONFIG_URL, url);
                    setProperty(OrientGraph.CONFIG_USER, user);
                    setProperty(OrientGraph.CONFIG_PASS, password);
                    setProperty(OrientGraph.CONFIG_CREATE, create);
                    setProperty(OrientGraph.CONFIG_OPEN, open);
                    setProperty(OrientGraph.CONFIG_TRANSACTIONAL, transactional);
                    setProperty(OrientGraph.CONFIG_LABEL_AS_CLASSNAME, labelAsClassName);
                }
            };
    }

    /**
     * @param create
     *            if true automatically creates database if database with given
     *            URL does not exist
     * @param open
     *            if true automatically opens the database
     */
    protected ODatabaseDocumentTx getDatabase(boolean create, boolean open) {
        final ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
        if (!db.getURL().startsWith("remote:") && !db.exists()) {
            if (create)
                db.create();
            else if (open) throw new ODatabaseException("Database '" + url + "' not found");
        } else if (open) db.open(user, password);

        return db;
    }

    /**
     * Enable or disable the prefixing of class names with V_&lt;label&gt; for
     * vertices or E_&lt;label&gt; for edges.
     * 
     * @param is
     *            if true classname equals label, if false classname is prefixed
     *            with V_ or E_ (default)
     */
    public OrientGraphFactory setLabelAsClassName(boolean is) {
        this.labelAsClassName = is;
        return this;
    }

    /**
     * Setting up the factory to use database pool instead of creation a new
     * instance of database connection each time.
     */
    @Deprecated
    public OrientGraphFactory setupPool(final int max) {
        pool = new OPartitionedReCreatableDatabasePool(url, user, password, max, true);
        return this;
    }

    public OrientGraphFactory setupPool(final int maxPartitionSize, final int max) {
        pool = new OPartitionedReCreatableDatabasePool(url, user, password, maxPartitionSize, max, true);
        return this;
    }

    public OPartitionedReCreatableDatabasePool pool() {
        return pool;
    }

    /**
     * Closes all pooled databases and clear the pool.
     */
    public void close() {
        if (pool != null)
            pool.close();

        pool = null;
    }

}
