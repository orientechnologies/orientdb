package com.orientechnologies.orient.distributed.context;

import java.util.Optional;

public interface OStateStore {

  Optional<ONodeStateStore> loadState();

  Optional<byte[]> loadSequence();

  void saveSequence(byte[] seq);

  void saveState(ONodeStateStore store);
}
