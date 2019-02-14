package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.server.OSystemDatabase;

import java.util.ArrayList;
import java.util.List;

public class OStructuralConfiguration {

  private OSystemDatabase                    systemDatabase;
  private List<OStructuralNodeConfiguration> nodes;

  public OStructuralConfiguration(OrientDBDistributed context) {
    systemDatabase = context.getServer().getSystemDatabase();
    load();
  }

  private void load() {
  }

  private void updatePersistent() {
    
  }

  public List<String> listDatabases() {
    return new ArrayList<>();
  }

  public OStructuralNodeConfiguration getConfiguration(String nodeId) {
    return null;
  }

}
