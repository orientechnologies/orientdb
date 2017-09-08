package com.orientechnologies.orient.etl.http;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLPlugin;
import com.orientechnologies.orient.output.OPluginMessageHandler;
import com.orientechnologies.orient.server.OServer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Created by gabriele on 27/02/17.
 */
public class OETLJob implements Runnable {

  private final ODocument    cfg;
  private       OETLListener listener;
  public        Status       status;

  public  PrintStream           stream;
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

    final String jsonConfig = cfg.field("jsonConfig");
    int logLevel = Integer.parseInt((String)cfg.field("logLevel"));

    // disabling debug level
    if(logLevel > 0) {
      logLevel++;
    }

    status = Status.RUNNING;
//    this.messageHandler = new OETLMessageHandler(this.stream, logLevel);
    this.messageHandler = null;

    final OETLPlugin etlPlugin = new OETLPlugin();
    String[] args = {jsonConfig};

    try {
      etlPlugin.executeJob(args);
    } catch (Exception e) {
      e.printStackTrace();
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

  public void validate() {

  }

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
      if(this.messageHandler != null) {
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
      } catch (Exception e) {
        e.printStackTrace();
      }
      int baosFinalSize = baos.size();
      if (baosFinalSize - baosInitSize > 0) {
        OLogManager.instance().info(this, "Losing some buffer info.");
      } else {
        baos.reset();
      }
    }
    return lastBatchLog;
  }

  public enum Status {
    STARTED, RUNNING, FINISHED
  }
}
