package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.List;

public class ODatabasesTopologyStore {

  private final List<ODatabaseTopologyStore> databases;

  public ODatabasesTopologyStore(List<ODatabaseTopologyStore> databases) {
    this.databases = databases;
  }

  public List<ODatabaseTopologyStore> getDatabases() {
    return databases;
  }

  public static ODatabasesTopologyStore fromResult(OResult res) {
    assert (int) res.getProperty("serializationVersion") == 1;
    List<OResult> dbs = res.getProperty("databases");
    var databases = dbs.stream().map((x) -> ODatabaseTopologyStore.fromResult(x)).toList();
    return new ODatabasesTopologyStore(databases);
  }

  public void toElement(OElement el) {
    el.setProperty("serializationVersion", 1);
    var dbs = databases.stream().map((x) -> x.toDocument()).toList();
    el.setProperty("databases", dbs, OType.EMBEDDEDLIST);
  }
}
