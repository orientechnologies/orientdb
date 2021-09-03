package com.orientechnologies.orient.etl.http;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLPlugin;
import com.orientechnologies.orient.etl.context.OETLContext;
import com.orientechnologies.orient.etl.context.OETLContextWrapper;
import com.orientechnologies.orient.etl.context.OETLMessageHandler;
import com.orientechnologies.orient.etl.util.OMigrationConfigManager;
import com.orientechnologies.orient.output.OPluginMessageHandler;
import com.orientechnologies.orient.server.OServer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

/** Created by gabriele on 27/02/17. */
public class OETLJob implements Runnable {

  private final ODocument cfg;
  private OETLListener listener;
  public Status status;

  public PrintStream stream;
  private ByteArrayOutputStream baos;
  private OPluginMessageHandler messageHandler;

  private OServer currentServerInstance;

  public OETLJob(ODocument cfg, OServer currentServerInstance, OETLListener listener) {
    this.cfg = cfg;
    this.listener = listener;

    this.baos = new ByteArrayOutputStream();
    this.stream = new PrintStream(baos);

    this.currentServerInstance = currentServerInstance;
  }

  @Override
  public void run() {

    final ODocument jsonConfig =
        new ODocument().fromJSON((String) cfg.field("jsonConfig"), "noMap");
    int logLevel = cfg.field("logLevel");
    final String configName = cfg.field("configName");
    final String outDBName = cfg.field("outDBName");

    // fetching the server orientdb home and updating coherently the target database URL
    String outOrientGraphUri = this.currentServerInstance.getDatabaseDirectory() + outDBName;
    String dbURL = "plocal:" + outOrientGraphUri;
    ((ODocument) ((ODocument) jsonConfig.field("loader")).field("orientdb")).field("dbURL", dbURL);

    status = Status.RUNNING;
    this.messageHandler = new OETLMessageHandler(this.stream, logLevel);

    final OETLPlugin etlPlugin = new OETLPlugin(this.currentServerInstance);

    String outDBConfigPath = null;
    try {
      outDBConfigPath =
          OMigrationConfigManager.writeConfigurationInTargetDB(
              jsonConfig, outOrientGraphUri, configName);
    } catch (Exception e) {
      ((OETLContext) OETLContextWrapper.getInstance().getContext())
          .printExceptionMessage(
              e, "Impossible to write etl configuration in the specified path.", "error");
      ((OETLContext) OETLContextWrapper.getInstance().getContext())
          .printExceptionStackTrace(e, "error");
    }

    try {
      String finalJsonConfig = jsonConfig.toJSON("prettyPrint");
      etlPlugin.executeJob(finalJsonConfig, outDBConfigPath, messageHandler);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error during execution of job", e);
    }

    synchronized (listener) {
      status = Status.FINISHED;
      try {
        listener.wait(5000);
        listener.onEnd(this);
      } catch (InterruptedException e) {
      }
    }
  }

  public void validate() {}

  /**
   * Single Job Status
   *
   * @return ODocument
   */
  public ODocument status() {

    synchronized (listener) {
      ODocument status = new ODocument();
      status.field("cfg", cfg);
      status.field("status", this.status);

      String lastBatchLog = "";
      if (this.messageHandler != null) {
        lastBatchLog = extractBatchLog();
      }
      status.field("log", lastBatchLog);

      if (this.status == Status.FINISHED) {
        listener.notifyAll();
      }
      return status;
    }
  }

  private String extractBatchLog() {

    String lastBatchLog = "Current status not correctly loaded.";

    synchronized (this.messageHandler) {

      // filling the last log batch
      int baosInitSize = baos.size();
      try {
        lastBatchLog = baos.toString("UTF-8");
      } catch (UnsupportedEncodingException e) {
        OLogManager.instance().error(this, "UTF-8 encoding is not supported", e);
      }
      int baosFinalSize = baos.size();
      if (baosFinalSize - baosInitSize > 0) {
        OETLContextWrapper.getInstance().getMessageHandler().info(this, "Losing some buffer info.");
      } else {
        baos.reset();
      }
    }
    return lastBatchLog;
  }

  public enum Status {
    STARTED,
    RUNNING,
    FINISHED
  }
}
