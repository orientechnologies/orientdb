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

import com.orientechnologies.orient.etl.extractor.OETLExtractor;
import java.util.concurrent.BlockingQueue;

/** Created by frank on 14/06/2016. */
class OETLExtractorWorker implements Runnable {
  private final BlockingQueue<OETLExtractedItem> queue;
  private final boolean haltOnError;
  private final OETLExtractor extractor;

  public OETLExtractorWorker(
      OETLExtractor extractor, BlockingQueue<OETLExtractedItem> queue, boolean haltOnError) {
    this.queue = queue;
    this.haltOnError = haltOnError;
    this.extractor = extractor;
  }

  @Override
  public void run() {
    extractor.getProcessor().getContext().getMessageHandler().debug(this, "Start extracting");
    boolean fetch = true;
    while (fetch == true) {

      try {
        if (extractor.hasNext()) {
          // EXTRACTOR
          final OETLExtractedItem current = extractor.next();

          // enqueue for transform and load
          queue.put(current);
        } else {

          queue.put(new OETLExtractedItem(true));
          fetch = false;
        }
      } catch (InterruptedException e) {

      } catch (Exception e) {
        if (haltOnError) {
          try {
            queue.put(new OETLExtractedItem(true));
          } catch (InterruptedException e1) {

          }
          fetch = false;
          throw e;
        }
      }
    }
  }
}
