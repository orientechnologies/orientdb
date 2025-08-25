package com.orientechnologies.orient.distributed.context;

import java.util.Optional;

public interface OStateStore {

  Optional<ONodeStateStore> loadState();

  Optional<byte[]> loadSequence();

  void saveState(ONodeStateStore store);
}
