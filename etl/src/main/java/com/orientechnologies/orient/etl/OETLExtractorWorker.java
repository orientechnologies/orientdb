package com.orientechnologies.orient.etl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by frank on 14/06/2016.
 */
class OETLExtractorWorker implements Runnable {
  private final BlockingQueue<OExtractedItem> queue;
  private final AtomicLong                    counter;
  private       OETLProcessor                 oetlProcessor;

  public OETLExtractorWorker(OETLProcessor oetlProcessor, BlockingQueue<OExtractedItem> queue, AtomicLong counter) {
    this.oetlProcessor = oetlProcessor;
    this.queue = queue;
    this.counter = counter;
  }


  @Override
  public void run() {
    try {
      oetlProcessor.out(OETLProcessor.LOG_LEVELS.DEBUG, "Start extracting");
      while (oetlProcessor.extractor.hasNext()) {
        // EXTRACTOR
        final OExtractedItem current = oetlProcessor.extractor.next();

        // enqueue for transform and load
        queue.put(current);
        counter.incrementAndGet();
      }
      queue.put(new OExtractedItem(true));
    } catch (InterruptedException e) {

    }
  }
}
