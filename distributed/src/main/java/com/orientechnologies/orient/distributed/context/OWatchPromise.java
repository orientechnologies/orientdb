package com.orientechnologies.orient.distributed.context;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class OWatchPromise {

  // TODO: timeout based cleanup
  private Map<Long, CountDownLatch> map = new HashMap<>();

  public CountDownLatch watch(long sequence) {
    CountDownLatch latch = map.computeIfAbsent(sequence, (key) -> new CountDownLatch(1));
    return latch;
  }

  public void complete(long sequence) {
    CountDownLatch latch = this.map.remove(sequence);
    latch.countDown();
  }
}
