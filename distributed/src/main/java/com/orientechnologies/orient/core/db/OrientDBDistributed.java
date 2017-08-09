package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.Orient;

/**
 * Created by tglman on 08/08/17.
 */
public class OrientDBDistributed extends OrientDBEmbedded {

  public OrientDBDistributed(String directoryPath, OrientDBConfig config, Orient instance) {
    super(directoryPath, config, instance);
  }
}
