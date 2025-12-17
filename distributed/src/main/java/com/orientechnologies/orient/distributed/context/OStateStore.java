package com.orientechnologies.orient.distributed.context;

public interface OStateStore {

  void save(ONodeStateStore store);

  ONodeStateStore load();
}
