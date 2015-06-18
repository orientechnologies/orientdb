package org.apache.tinkerpop.gremlin.orientdb.structure;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;


public final class OrientGraphFactory {
    public OrientGraph open(String url, String user, String password) {
        return new OrientGraph(getDatabase(url, user, password));
    }

    protected ODatabaseDocumentTx getDatabase(String url, String user, String password) {
        final ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
        db.open(user, password);

        return db;
    }
}
