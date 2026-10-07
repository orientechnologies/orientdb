package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.OrientDBLoader;

public class OrientDBDistributedLoader implements OrientDBLoader {

  @Override
  public String getScheme() {
    return "distributed";
  }

  @Override
  public OrientDBInternal load(String url, OrientDBConfig config, Orient orient) {
    return new OrientDBDistributed(url, config, orient);
  }
}
