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

import java.util.concurrent.BlockingQueue;

/** Created by frank on 14/06/2016. */
final class OETLPipelineWorker implements Runnable {

  private final BlockingQueue<OETLExtractedItem> queue;
  private final OETLPipeline pipeline;

  public OETLPipelineWorker(BlockingQueue<OETLExtractedItem> queue, OETLPipeline pipeline) {
    this.queue = queue;
    this.pipeline = pipeline;
    pipeline.begin();
  }

  @Override
  public void run() {
    try {
      OETLExtractedItem content;
      while (!(content = queue.take()).finished) {
        pipeline.execute(content);
      }
      pipeline.end();
      // RE-ADD END FLAG FOR OTHER THREADS
      queue.put(content);
    } catch (InterruptedException e) {
    }
  }
}
