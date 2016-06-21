package com.orientechnologies.orient.etl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by frank on 14/06/2016.
 */
class OETLExtractorWorker implements Callable<Boolean> {
  private       OETLProcessor                 oetlProcessor;
  private final BlockingQueue<OExtractedItem> queue;
  private final AtomicLong                    counter;

  public OETLExtractorWorker(OETLProcessor oetlProcessor, BlockingQueue<OExtractedItem> queue, AtomicLong counter) {
    this.oetlProcessor = oetlProcessor;
    this.queue = queue;
    this.counter = counter;
  }

  @Override
  public Boolean call() throws Exception {
    oetlProcessor.out(OETLProcessor.LOG_LEVELS.DEBUG, "Start extracting");
    while (oetlProcessor.extractor.hasNext()) {
      // EXTRACTOR
      final OExtractedItem current = oetlProcessor.extractor.next();

      // TRANSFORM + LOAD
      queue.put(current);
      counter.incrementAndGet();
    }
    queue.put(new OExtractedItem(true));
    return Boolean.TRUE;
  }
}
