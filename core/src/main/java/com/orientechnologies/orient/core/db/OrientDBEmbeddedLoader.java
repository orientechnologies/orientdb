package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.Orient;

public class OrientDBEmbeddedLoader implements OrientDBLoader {

  @Override
  public String getScheme() {
    return "embedded";
  }

  @Override
  public OrientDBInternal load(String url, OrientDBConfig config, Orient orient) {
    return new OrientDBEmbedded(url, config, orient);
  }
}
