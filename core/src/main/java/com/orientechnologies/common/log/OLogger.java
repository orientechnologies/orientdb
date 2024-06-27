package com.orientechnologies.common.log;

public interface OLogger {
  public enum Level {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
  }

  default void debug(String message, Object... additionalArgs) {
    log(Level.DEBUG, message, null, true, additionalArgs);
  }

  default void debug(String message, Throwable exception, Object... additionalArgs) {
    log(Level.DEBUG, message, exception, true, additionalArgs);
  }

  default void debugNoDb(String message, Throwable exception, Object... additionalArgs) {
    log(Level.DEBUG, message, exception, false, additionalArgs);
  }

  default void info(String message, Object... additionalArgs) {
    log(Level.INFO, message, null, true, additionalArgs);
  }

  default void infoNoDb(String message, Object... additionalArgs) {
    log(Level.INFO, message, null, false, additionalArgs);
  }

  default void info(String message, Throwable exception, Object... additionalArgs) {
    log(Level.INFO, message, exception, true, additionalArgs);
  }

  default void warn(String message, Object... additionalArgs) {
    log(Level.WARN, message, null, true, additionalArgs);
  }

  default void warnNoDb(String message, Object... additionalArgs) {
    log(Level.WARN, message, null, false, additionalArgs);
  }

  default void warn(String message, Throwable exception, Object... additionalArgs) {
    log(Level.WARN, message, exception, true, null, additionalArgs);
  }

  default void error(String message, Throwable exception, Object... additionalArgs) {
    log(Level.ERROR, message, exception, true, additionalArgs);
  }

  default void errorNoDb(String message, Throwable exception, Object... additionalArgs) {
    log(Level.ERROR, message, exception, false, additionalArgs);
  }

  public void log(
      Level iLevel,
      String message,
      Throwable exception,
      boolean extractDatabase,
      Object... additionalArgs);
}
