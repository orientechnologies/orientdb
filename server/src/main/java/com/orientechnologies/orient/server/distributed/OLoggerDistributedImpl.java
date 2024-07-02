package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;

public class OLoggerDistributedImpl implements OLoggerDistributed {

  private final OLogger logger;

  public OLoggerDistributedImpl(OLogger logger) {
    this.logger = logger;
  }

  public boolean isDebugEnabled() {
    return OLogManager.instance().isDebugEnabled();
  }

  public void debug(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Object... additionalArgs) {
    logger.debug(formatMessage(localNode, remoteNode, direction, message), additionalArgs);
  }

  public void debug(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.debug(
        formatMessage(localNode, remoteNode, direction, message), exception, additionalArgs);
  }

  public void info(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Object... additionalArgs) {
    logger.info(formatMessage(localNode, remoteNode, direction, message), additionalArgs);
  }

  public void info(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.info(
        formatMessage(localNode, remoteNode, direction, message), exception, additionalArgs);
  }

  public void warn(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Object... additionalArgs) {
    logger.warn(formatMessage(localNode, remoteNode, direction, message), additionalArgs);
  }

  public void warn(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.warn(
        formatMessage(localNode, remoteNode, direction, message), exception, additionalArgs);
  }

  public void error(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Object... additionalArgs) {
    logger.error(formatMessage(localNode, remoteNode, direction, message), null, additionalArgs);
  }

  public void error(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Throwable exception,
      Object... additionalArgs) {
    logger.error(
        formatMessage(localNode, remoteNode, direction, message), exception, additionalArgs);
  }

  protected static String formatMessage(
      final String localNode,
      final String remoteNode,
      final DIRECTION direction,
      final String message) {
    final StringBuilder formatted = new StringBuilder(256);

    if (localNode != null) {
      formatted.append('[');
      formatted.append(localNode);
      formatted.append(']');
    }

    if (remoteNode != null && !remoteNode.equals(localNode)) {
      switch (direction) {
        case IN:
          formatted.append("<-");
          break;
        case OUT:
          formatted.append("->");
          break;
        case BOTH:
          formatted.append("<>");
          break;
        case NONE:
          formatted.append("--");
          break;
      }

      formatted.append('[');
      formatted.append(remoteNode);
      formatted.append(']');
    }

    formatted.append(' ');
    formatted.append(message);

    return formatted.toString();
  }
}
