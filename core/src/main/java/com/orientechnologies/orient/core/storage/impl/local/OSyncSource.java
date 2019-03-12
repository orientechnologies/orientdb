package com.orientechnologies.orient.core.storage.impl.local;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;

public interface OSyncSource {
  boolean getIncremental();

  InputStream getInputStream();

  CountDownLatch getFinished();

  void invalidate();

  boolean isValid();
}
