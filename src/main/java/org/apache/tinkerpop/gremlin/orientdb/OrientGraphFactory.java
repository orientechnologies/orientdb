package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;


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

    public OrientGraph getTx() {
        return new OrientGraph(getDatabase(true, true));
    }

    /**
     * @param create
     *          if true automatically creates database if database with given URL does not exist
     * @param open
     *          if true automatically opens the database
     */
    public OrientGraph getTx(boolean create, boolean open) {
        return new OrientGraph(getDatabase(create, open));
    }

    /**
     * @param create
     *          if true automatically creates database if database with given URL does not exist
     * @param open
     *          if true automatically opens the database
     */
    protected ODatabaseDocumentTx getDatabase(boolean create, boolean open) {
        final ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
        if (!db.getURL().startsWith("remote:") && !db.exists()) {
            if (create) db.create();
            else if (open) throw new ODatabaseException("Database '" + url + "' not found");
        } else if (open) db.open(user, password);

        return db;
    }
}
