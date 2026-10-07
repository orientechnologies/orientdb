package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.OrientDBLoader;

public class OrientDBRemoteLoader implements OrientDBLoader {

  @Override
  public String getScheme() {
    return "remote";
  }

  @Override
  public OrientDBInternal load(String url, OrientDBConfig config, Orient orient) {
    return new OrientDBRemote(url.split("[,;]"), config, orient);
  }
}
