package com.orientechnologies.orient.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OToken;

public class OTockenClientConnectionManager {

  private Map<String, AtomicInteger> counter = new HashMap<String, AtomicInteger>();

  public ODatabaseDocument open(OToken token) {
    String dbName = token.getDatabase();
    String type = "";
    String url = type + ":" + dbName;
    ODatabaseDocument db = new ODatabaseDocumentTx(url);
    db.open(token);
    AtomicInteger cur = counter.get(dbName);
    if (cur == null) {
      synchronized (counter) {
        cur = counter.get(dbName);
        if (cur == null) {
          cur = new AtomicInteger();
          counter.put(dbName, cur);
        }
      }
    }
    cur.incrementAndGet();
    return db;
  }

  public void close(ODatabaseDocument db) {
    AtomicInteger cur = counter.get(db.getName());
    assert cur != null : " counter for db " + db.getName() + " is missed, wrong api call";
    int val = cur.decrementAndGet();
    if (val == 0) {
      // TODO: run close task.
    } else
      db.close();
  }
}
