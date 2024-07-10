package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;

public interface OLoggerDistributed extends OLogger {

  static OLoggerDistributed from(OLogger logger) {
    return new OLoggerDistributedImpl(logger);
  }

  void debug(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Object... additionalArgs);

  void debug(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void info(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Object... additionalArgs);

  void info(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void warn(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Object... additionalArgs);

  void warn(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void error(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Object... additionalArgs);

  void error(
      String localNode,
      String remoteNode,
      DIRECTION direction,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void debug(String localNode, String message, Object... additionalArgs);

  void debug(String localNode, String message, Throwable exception, Object... additionalArgs);

  void info(String localNode, String message, Object... additionalArgs);

  void info(String localNode, String message, Throwable exception, Object... additionalArgs);

  void warn(String localNode, String message, Object... additionalArgs);

  void warn(String localNode, String message, Throwable exception, Object... additionalArgs);

  void error(String localNode, String message, Object... additionalArgs);

  void error(String localNode, String message, Throwable exception, Object... additionalArgs);
}
