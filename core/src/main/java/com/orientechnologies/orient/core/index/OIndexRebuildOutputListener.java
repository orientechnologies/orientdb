/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;

/**
 * Progress listener for index rebuild.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OIndexRebuildOutputListener implements OProgressListener {
  private final OIndex<?> idx;
  long                    startTime;
  long                    lastDump;
  long                    lastCounter = 0;

  public OIndexRebuildOutputListener(final OIndex<?> idx) {
    this.idx = idx;
  }

  @Override
  public void onBegin(final Object iTask, final long iTotal) {
    startTime = System.currentTimeMillis();
    lastDump = startTime;
    OLogManager.instance().debug(this, "- Building index %s...", idx.getName());
  }

  @Override
  public boolean onProgress(final Object iTask, final long iCounter, final float iPercent) {
    final long now = System.currentTimeMillis();
    if (now - lastDump > 10000) {
      // DUMP EVERY 5 SECONDS FOR LARGE INDEXES
      OLogManager.instance().debug(this, "--> %3.2f%% progress, %,d indexed so far (%,d items/sec)", iPercent, iCounter,
          ((iCounter - lastCounter) / 10));
      lastDump = now;
      lastCounter = iCounter;
    }
    return true;
  }

  @Override
  public void onCompletition(final Object iTask, final boolean iSucceed) {
    OLogManager.instance().debug(this, "--> OK, indexed %,d items in %,d ms", idx.getSize(),
        (System.currentTimeMillis() - startTime));
  }
}