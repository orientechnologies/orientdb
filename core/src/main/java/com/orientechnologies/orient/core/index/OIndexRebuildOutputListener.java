/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
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
  boolean                 rebuild     = false;

  public OIndexRebuildOutputListener(final OIndex<?> idx) {
    this.idx = idx;
  }

  @Override
  public void onBegin(final Object iTask, final long iTotal, final Object iRebuild) {
    startTime = System.currentTimeMillis();
    lastDump = startTime;

    rebuild = (Boolean) iRebuild;
    if (iTotal > 0)
      if (rebuild)
        OLogManager.instance().info(this, "- Rebuilding index %s.%s (estimated %,d items)...", idx.getDatabaseName(), idx.getName(), iTotal);
      else
        OLogManager.instance().debug(this, "- Building index %s.%s (estimated %,d items)...", idx.getDatabaseName(), idx.getName(), iTotal);
  }

  @Override
  public boolean onProgress(final Object iTask, final long iCounter, final float iPercent) {
    final long now = System.currentTimeMillis();
    if (now - lastDump > 10000) {
      // DUMP EVERY 5 SECONDS FOR LARGE INDEXES
      if (rebuild)
        OLogManager.instance().info(this, "--> %3.2f%% progress, %,d indexed so far (%,d items/sec)", iPercent, iCounter,
            ((iCounter - lastCounter) / 10));
      else
        OLogManager.instance().debug(this, "--> %3.2f%% progress, %,d indexed so far (%,d items/sec)", iPercent, iCounter,
            ((iCounter - lastCounter) / 10));
      lastDump = now;
      lastCounter = iCounter;
    }
    return true;
  }

  @Override
  public void onCompletition(final Object iTask, final boolean iSucceed) {
    final long idxSize = idx.getSize();

    if (idxSize > 0)
      if (rebuild)
        OLogManager.instance().info(this, "--> OK, indexed %,d items in %,d ms", idxSize, (System.currentTimeMillis() - startTime));
      else
        OLogManager.instance()
            .debug(this, "--> OK, indexed %,d items in %,d ms", idxSize, (System.currentTimeMillis() - startTime));
  }
}
