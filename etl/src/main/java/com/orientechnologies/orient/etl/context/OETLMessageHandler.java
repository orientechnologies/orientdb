package com.orientechnologies.orient.etl.context;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.output.OOutputStreamManager;
import com.orientechnologies.orient.output.OPluginMessageHandler;
import java.io.PrintStream;

/**
 * Implementation of OPluginMessageHandler for ETL plugin. It receives messages application from the
 * application and just delegates its printing on a stream through the OutputStreamManager.
 *
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */
public class OETLMessageHandler implements OPluginMessageHandler {

  private int outputManagerLevel; // affects OutputStreamManager outputManagerLevel
  private OOutputStreamManager outputManager;

  public OETLMessageHandler(PrintStream outputStream, int level) {
    this.outputManagerLevel = level;
    this.outputManager = new OOutputStreamManager(outputStream, level);
  }

  public OETLMessageHandler(int level) {
    this.outputManagerLevel = level;
    this.outputManager = new OOutputStreamManager(level);
  }

  public OETLMessageHandler(OOutputStreamManager outputStreamManager) {
    this.outputManager = outputStreamManager;
    this.outputManagerLevel = this.outputManager.getLevel();
  }

  public OOutputStreamManager getOutputManager() {
    return this.outputManager;
  }

  public void setOutputManager(OOutputStreamManager outputManager) {
    this.outputManager = outputManager;
  }

  public int getOutputManagerLevel() {
    return this.outputManagerLevel;
  }

  public void setOutputManagerLevel(int outputManagerLevel) {
    this.outputManagerLevel = outputManagerLevel;
    this.updateOutputStreamManagerLevel();
  }

  private synchronized void updateOutputStreamManagerLevel() {
    this.outputManager.setLevel(this.outputManagerLevel);
  }

  @Override
  public synchronized void debug(Object requester, String message) {
    OLogManager.instance().debug(requester, message);
    message += "\n";
    this.outputManager.debug(message);
  }

  @Override
  public synchronized void debug(Object requester, String format, Object... args) {
    OLogManager.instance().debug(requester, format, args);
    format += "\n";
    this.outputManager.debug(format, args);
  }

  @Override
  public synchronized void info(Object requester, String message) {
    OLogManager.instance().info(requester, message);
    message += "\n";
    this.outputManager.info(message);
  }

  @Override
  public synchronized void info(Object requester, String format, Object... args) {
    OLogManager.instance().info(requester, format, args);
    format += "\n";
    this.outputManager.info(format, args);
  }

  @Override
  public synchronized void warn(Object requester, String message) {
    OLogManager.instance().warn(requester, message);
    message += "\n";
    this.outputManager.warn(message);
  }

  @Override
  public synchronized void warn(Object requester, String format, Object... args) {
    OLogManager.instance().warn(requester, format, args);
    format += "\n";
    this.outputManager.warn(format, args);
  }

  @Override
  public synchronized void error(Object requester, String message) {
    OLogManager.instance().error(requester, message, null);
    message += "\n";
    this.outputManager.error(message);
  }

  @Override
  public synchronized void error(Object requester, String format, Object... args) {
    OLogManager.instance().error(requester, format, null, args);
    format += "\n";
    this.outputManager.error(format, args);
  }
}
