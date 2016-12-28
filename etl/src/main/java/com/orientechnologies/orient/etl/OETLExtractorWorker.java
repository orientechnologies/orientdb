/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl;

import com.orientechnologies.common.log.OLogManager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by frank on 14/06/2016.
 */
class OETLExtractorWorker implements Runnable {
  private final BlockingQueue<OETLExtractedItem> queue;
  private final AtomicLong                       counter;
  private       OETLProcessor                    oetlProcessor;

  public OETLExtractorWorker(OETLProcessor oetlProcessor, BlockingQueue<OETLExtractedItem> queue, AtomicLong counter) {
    this.oetlProcessor = oetlProcessor;
    this.queue = queue;
    this.counter = counter;
  }

  @Override
  public void run() {
    try {
//      oetlProcessor.out(OETLProcessor.LOG_LEVELS.DEBUG, "Start extracting");
      OLogManager.instance().debug(this, "Start extracting");
      while (oetlProcessor.extractor.hasNext()) {
        // EXTRACTOR
        final OETLExtractedItem current = oetlProcessor.extractor.next();

        // enqueue for transform and load
        queue.put(current);
        counter.incrementAndGet();
      }
      queue.put(new OETLExtractedItem(true));
    } catch (InterruptedException e) {

    }
  }
}
