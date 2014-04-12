package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com)
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientGraphFactory {
  protected final String          url;
  protected final String          user;
  protected final String          password;

  protected ODatabaseDocumentPool pool;
  protected boolean               transactional = true;

  public OrientGraphFactory(final String iURL) {
    this(iURL, "admin", "admin");
  }

  public OrientGraphFactory(final String iURL, final String iUser, final String iPassword) {
    url = iURL;
    user = iUser;
    password = iPassword;
  }

  public void close() {
    if (pool != null) {
      pool.close();
      pool = null;
    }
  }

  public void drop() {
    getDatabase(false).drop();
  }

  public OrientBaseGraph get() {
    return transactional ? getTx() : getNoTx();
  }

  public OrientGraph getTx() {
    if (pool == null) {
      return new OrientGraph(getDatabase());
    } else {
      return new OrientGraph(pool);
    }
  }

  public OrientGraphNoTx getNoTx() {
    if (pool == null) {
      return new OrientGraphNoTx(getDatabase());
    } else {
      return new OrientGraphNoTx(pool);
    }
  }

  public ODatabaseDocumentTx getDatabase() {
    return getDatabase(true);
  }

  public ODatabaseDocumentTx getDatabase(final boolean iCreate) {
    if (pool != null)
      // USE THE POOL
      return pool.acquire();

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    if (!db.getURL().startsWith("remote:") && !db.exists()) {
      if (iCreate)
        db.create();
      else
        throw new ODatabaseException("Database '" + url + "' not found");
    } else
      db.open(user, password);

    return db;
  }

  public boolean exists() {
    final ODatabaseDocumentTx db = getDatabase();
    try {
      return db.exists();
    } finally {
      db.close();
    }
  }

  public OrientGraphFactory setupPool(final int iMin, final int iMax) {
    // ASSURE THE DB IS CREATED THE FIRST TIME
    final ODatabaseDocumentTx db = getDatabase();
    db.close();

    if (pool != null) {
      pool.close();
    }

    pool = new ODatabaseDocumentPool(url, user, password);
    pool.setup(iMin, iMax);
    return this;
  }

  public boolean isTransactional() {
    return transactional;
  }

  public OrientGraphFactory setTransactional(boolean transactional) {
    this.transactional = transactional;
    return this;
  }

  @Override
  protected void finalize() throws Throwable {
    close();
  }

}
