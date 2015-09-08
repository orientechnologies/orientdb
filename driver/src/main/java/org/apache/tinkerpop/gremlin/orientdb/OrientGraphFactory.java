package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;


public final class OrientGraphFactory {
    public static String ADMIN = "admin";
    protected final String url;
    protected final String user;
    protected final String password;

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
     * @param create
     *          if true automatically creates database if database with given URL does not exist
     * @param open
     *          if true automatically opens the database
     */
    //TODO: allow to open with these properties
    public OrientGraph getNoTx(boolean create, boolean open) {
        return OrientGraph.open(getConfiguration(create, open));
    }

    public OrientGraph getNoTx() {
        return getNoTx(true, true);
    }

    protected Configuration getConfiguration(boolean create, boolean open) {
        return new BaseConfiguration() {{
            setProperty(Graph.GRAPH, OrientGraph.class.getName());
            setProperty(OrientGraph.CONFIG_URL, url);
            setProperty(OrientGraph.CONFIG_USER, user);
            setProperty(OrientGraph.CONFIG_PASS, password);
            setProperty(OrientGraph.CONFIG_CREATE, create);
            setProperty(OrientGraph.CONFIG_OPEN, open);
        }};
    }

}
