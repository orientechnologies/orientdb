package com.orientechnologies.orient.etl.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by gabriele on 27/02/17.
 */
public class OETLHandler {

  private ExecutorService pool       = Executors.newFixedThreadPool(1);
  private OETLJob         currentJob = null;

  public OETLHandler() {}

  /**
   * Executes import with configuration;
   *
   * @param cfg
   */
  public void executeImport(ODocument cfg, OServer server) {

    OETLJob job = new OETLJob(cfg, server, new OETLListener() {
      @Override
      public void onEnd(OETLJob etlJob) {
        currentJob = null;
      }
    });

    job.validate();

    currentJob = job;
    pool.execute(job);

  }

  /**
   * Checks If the connection with given parameters is alive
   *
   * @param args
   *
   * @throws Exception
   */
  public void checkConnection(ODocument args) throws Exception {

  }


  /**
   * Status of the Running Jobs
   *
   * @return ODocument
   */
  public ODocument status() {

    ODocument status = new ODocument();

    Collection<ODocument> jobs = new ArrayList<ODocument>();
    if (currentJob != null) {
      jobs.add(currentJob.status());
    }
    status.field("jobs", jobs);
    return status;
  }
}
