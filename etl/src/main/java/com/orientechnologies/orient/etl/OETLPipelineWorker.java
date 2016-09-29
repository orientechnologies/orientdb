package com.orientechnologies.orient.etl;

import java.util.concurrent.BlockingQueue;

/**
 * Created by frank on 14/06/2016.
 */
final class OETLPipelineWorker implements Runnable {

  private final BlockingQueue<OExtractedItem> queue;
  private final OETLPipeline                  pipeline;

  public OETLPipelineWorker(BlockingQueue<OExtractedItem> queue, OETLPipeline pipeline) {
    this.queue = queue;
    this.pipeline = pipeline;
    pipeline.begin();
  }

  @Override
  public void run() {
    try {
      OExtractedItem content;
      while (!(content = queue.take()).finished) {
        pipeline.execute(content);
      }
      pipeline.end();
      //RE-ADD END FLAG FOR OTHER THREADS
      queue.put(content);
    } catch (InterruptedException e) {
    }
  }
}
