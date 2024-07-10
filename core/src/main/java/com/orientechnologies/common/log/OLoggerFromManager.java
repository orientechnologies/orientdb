package com.orientechnologies.common.log;

public class OLoggerFromManager implements OLogger {

  private Class<?> cl;
  private OLogManager manager;

  public OLoggerFromManager(Class<?> cl, OLogManager manager) {
    this.cl = cl;
    this.manager = manager;
  }

  @Override
  public void log(
      Level level,
      String message,
      Throwable exception,
      boolean extractDatabase,
      Object... additionalArgs) {
    manager.log(
        this.cl, translateLevel(level), message, exception, extractDatabase, additionalArgs);
  }

  @Override
  public boolean isDebugEnabled() {
    return manager.isDebugEnabled();
  }

  @Override
  public boolean isWarnEnabled() {
    return manager.isWarnEnabled();
  }

  private java.util.logging.Level translateLevel(Level level) {
    switch (level) {
      case TRACE:
        return java.util.logging.Level.FINEST;
      case DEBUG:
        return java.util.logging.Level.FINE;
      case INFO:
        return java.util.logging.Level.INFO;
      case WARN:
        return java.util.logging.Level.WARNING;
      case ERROR:
        return java.util.logging.Level.SEVERE;
    }
    return null;
  }
}
