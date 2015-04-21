package com.orientechnologies.orient.core.storage.cache;

import java.security.SecureRandom;

public class OSnowFlakeIdGen implements OWriteCacheIdGen {
  private long               lastTs;
  private final SecureRandom rnd;
  private int                sequenceCounter;
  private byte[]             rndBytes = new byte[2];

  public OSnowFlakeIdGen() {
    rnd = new SecureRandom();

    init();
  }

  @Override
  public synchronized int nextId() {
    if (sequenceCounter < 16)
      sequenceCounter++;
    else {
      init();
    }

    return composeId();
  }

  private void init() {
    sequenceCounter = 0;
    lastTs = System.currentTimeMillis();
    rnd.nextBytes(rndBytes);
  }

  private int composeId() {
    return ((rndBytes[0] & 0xFF) << 24) | ((rndBytes[1] & 0xFF) << 16) | (((int) (lastTs & 0xFFF)) << 4) | (sequenceCounter & 0xF);
  }
}
