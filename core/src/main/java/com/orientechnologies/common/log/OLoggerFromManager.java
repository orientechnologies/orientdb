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

  private static java.util.logging.Level translateLevel(Level level) {
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

  public static Level translateJavaLogging(java.util.logging.Level level) {
    if (java.util.logging.Level.FINEST.equals(level)) {
      return Level.TRACE;
    } else if (java.util.logging.Level.ALL.equals(level)) {
      return Level.TRACE;
    } else if (java.util.logging.Level.FINER.equals(level)) {
      return Level.TRACE;
    } else if (java.util.logging.Level.OFF.equals(level)) {
      return Level.TRACE;
    } else if (java.util.logging.Level.FINE.equals(level)) {
      return Level.DEBUG;
    } else if (java.util.logging.Level.CONFIG.equals(level)) {
      return Level.INFO;
    } else if (java.util.logging.Level.INFO.equals(level)) {
      return Level.INFO;
    } else if (java.util.logging.Level.WARNING.equals(level)) {
      return Level.WARN;
    } else if (java.util.logging.Level.SEVERE.equals(level)) {
      return Level.ERROR;
    }
    return Level.TRACE;
  }
}
