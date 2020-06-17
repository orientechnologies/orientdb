package com.orientechnologies.common.log;

import java.util.logging.LogManager;

/**
 * Inhibits the logs reset request which is typically done on shutdown. This allows to use JDK
 * logging from shutdown hooks.
 * -Djava.util.logging.manager=com.orientechnologies.common.log.ShutdownLogManager must be passed to
 * the JVM, to activate this log manager.
 */
public class ShutdownLogManager extends LogManager {

  @Override
  public void reset() {
    // do nothing
  }

  protected void shutdown() {
    super.reset();
  }
}
