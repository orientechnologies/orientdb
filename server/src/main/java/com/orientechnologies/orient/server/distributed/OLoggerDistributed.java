package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;

public interface OLoggerDistributed extends OLogger {

  static OLoggerDistributed from(OLogger logger) {
    return new OLoggerDistributedImpl(logger);
  }

  void debugOut(String localNode, String remoteNode, String message, Object... additionalArgs);

  void debugOut(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void infoOut(String localNode, String remoteNode, String message, Object... additionalArgs);

  void infoOut(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void warnOut(String localNode, String remoteNode, String message, Object... additionalArgs);

  void warnOut(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void errorOut(String localNode, String remoteNode, String message, Object... additionalArgs);

  void errorOut(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void debugIn(String localNode, String remoteNode, String message, Object... additionalArgs);

  void debugIn(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void infoIn(String localNode, String remoteNode, String message, Object... additionalArgs);

  void infoIn(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void warnIn(String localNode, String remoteNode, String message, Object... additionalArgs);

  void warnIn(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void errorIn(String localNode, String remoteNode, String message, Object... additionalArgs);

  void errorIn(
      String localNode,
      String remoteNode,
      String message,
      Throwable exception,
      Object... additionalArgs);

  void debugNode(String localNode, String message, Object... additionalArgs);

  void debugNode(String localNode, String message, Throwable exception, Object... additionalArgs);

  void infoNode(String localNode, String message, Object... additionalArgs);

  void infoNode(String localNode, String message, Throwable exception, Object... additionalArgs);

  void warnNode(String localNode, String message, Object... additionalArgs);

  void warnNode(String localNode, String message, Throwable exception, Object... additionalArgs);

  void errorNode(String localNode, String message, Object... additionalArgs);

  void errorNode(String localNode, String message, Throwable exception, Object... additionalArgs);

  static OLoggerDistributed logger(Class<?> cl) {
    return from(OLogManager.instance().logger(cl));
  }
}
