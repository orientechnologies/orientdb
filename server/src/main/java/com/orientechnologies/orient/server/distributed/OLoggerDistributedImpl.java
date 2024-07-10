package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;

public class OLoggerDistributedImpl implements OLoggerDistributed {

  private final OLogger logger;

  public OLoggerDistributedImpl(OLogger logger) {
    this.logger = logger;
  }

  public boolean isDebugEnabled() {
    return OLogManager.instance().isDebugEnabled();
  }

  @Override
  public void debugNode(String localNode, String message, Object... additionalArgs) {
    logger.debug(formatMessage(localNode, message), additionalArgs);
  }

  @Override
  public void debugNode(
      String localNode, String message, Throwable exception, Object... additionalArgs) {
    logger.debug(formatMessage(localNode, message), exception, additionalArgs);
  }

  @Override
  public void infoNode(String localNode, String message, Object... additionalArgs) {
    logger.info(formatMessage(localNode, message), additionalArgs);
  }

  @Override
  public void infoNode(
      String localNode, String message, Throwable exception, Object... additionalArgs) {
    logger.info(formatMessage(localNode, message), exception, additionalArgs);
  }

  @Override
  public void warnNode(String localNode, String message, Object... additionalArgs) {
    logger.warn(formatMessage(localNode, message), additionalArgs);
  }

  @Override
  public void warnNode(
      String localNode, String message, Throwable exception, Object... additionalArgs) {
    logger.warn(formatMessage(localNode, message), exception, additionalArgs);
  }

  @Override
  public void errorNode(String localNode, String message, Object... additionalArgs) {
    logger.error(formatMessage(localNode, message), null, additionalArgs);
  }

  @Override
  public void errorNode(
      String localNode, String message, Throwable exception, Object... additionalArgs) {
    logger.error(formatMessage(localNode, message), exception, additionalArgs);
  }

  public void debugOut(
      String localNode, String remoteNode, String message, Object... additionalArgs) {
    logger.debug(formatMessageOut(localNode, remoteNode, message), additionalArgs);
  }

  public void debugOut(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.debug(formatMessageOut(localNode, remoteNode, message), exception, additionalArgs);
  }

  public void infoOut(
      String localNode, String remoteNode, String message, Object... additionalArgs) {
    logger.info(formatMessageOut(localNode, remoteNode, message), additionalArgs);
  }

  public void infoOut(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.info(formatMessageOut(localNode, remoteNode, message), exception, additionalArgs);
  }

  public void warnOut(
      String localNode, String remoteNode, String message, Object... additionalArgs) {
    logger.warn(formatMessageOut(localNode, remoteNode, message), additionalArgs);
  }

  public void warnOut(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.warn(formatMessageOut(localNode, remoteNode, message), exception, additionalArgs);
  }

  public void errorOut(
      String localNode, String remoteNode, String message, Object... additionalArgs) {
    logger.error(formatMessageOut(localNode, remoteNode, message), null, additionalArgs);
  }

  public void errorOut(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.error(formatMessageOut(localNode, remoteNode, message), exception, additionalArgs);
  }

  public void debugIn(
      String localNode, String remoteNode, String message, Object... additionalArgs) {
    logger.debug(formatMessageIn(localNode, remoteNode, message), additionalArgs);
  }

  public void debugIn(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.debug(formatMessageIn(localNode, remoteNode, message), exception, additionalArgs);
  }

  public void infoIn(
      String localNode, String remoteNode, String message, Object... additionalArgs) {
    logger.info(formatMessageIn(localNode, remoteNode, message), additionalArgs);
  }

  public void infoIn(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.info(formatMessageIn(localNode, remoteNode, message), exception, additionalArgs);
  }

  public void warnIn(
      String localNode, String remoteNode, String message, Object... additionalArgs) {
    logger.warn(formatMessageIn(localNode, remoteNode, message), additionalArgs);
  }

  public void warnIn(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.warn(formatMessageIn(localNode, remoteNode, message), exception, additionalArgs);
  }

  public void errorIn(
      String localNode, String remoteNode, String message, Object... additionalArgs) {
    logger.error(formatMessageIn(localNode, remoteNode, message), null, additionalArgs);
  }

  public void errorIn(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.error(formatMessageIn(localNode, remoteNode, message), exception, additionalArgs);
  }

  protected static String formatMessage(final String localNode, final String message) {
    final StringBuilder formatted = new StringBuilder(256);

    if (localNode != null) {
      formatted.append('[');
      formatted.append(localNode);
      formatted.append(']');
    }

    formatted.append(' ');
    formatted.append(message);

    return formatted.toString();
  }

  protected static String formatMessageIn(
      final String localNode, final String remoteNode, final String message) {
    final StringBuilder formatted = new StringBuilder(256);

    if (localNode != null) {
      formatted.append('[');
      formatted.append(localNode);
      formatted.append(']');
    }

    if (remoteNode != null && !remoteNode.equals(localNode)) {
      formatted.append("<-");
      formatted.append('[');
      formatted.append(remoteNode);
      formatted.append(']');
    }

    formatted.append(' ');
    formatted.append(message);

    return formatted.toString();
  }

  protected static String formatMessageOut(
      final String localNode, final String remoteNode, final String message) {
    final StringBuilder formatted = new StringBuilder(256);

    if (localNode != null) {
      formatted.append('[');
      formatted.append(localNode);
      formatted.append(']');
    }

    if (remoteNode != null && !remoteNode.equals(localNode)) {
      formatted.append("->");
      formatted.append('[');
      formatted.append(remoteNode);
      formatted.append(']');
    }

    formatted.append(' ');
    formatted.append(message);

    return formatted.toString();
  }

  @Override
  public void log(
      Level iLevel,
      String message,
      Throwable exception,
      boolean extractDatabase,
      Object... additionalArgs) {
    logger.log(iLevel, message, exception, extractDatabase, additionalArgs);
  }

  @Override
  public boolean isWarnEnabled() {
    return logger.isWarnEnabled();
  }
}
