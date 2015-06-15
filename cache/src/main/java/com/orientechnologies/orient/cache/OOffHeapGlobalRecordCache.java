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
package com.orientechnologies.orient.cache;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OGlobalRecordCache;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.util.Map;
import java.util.TimerTask;

/**
 * Global cache contained in the resource space of OStorage and shared by all database instances that work on top of that Storage.
 * 
 * @author Luca Garulli
 */
public class OOffHeapGlobalRecordCache implements OGlobalRecordCache {
  private Map<OCachedRID, OCachedRecord> cache;

  private String                         CACHE_HIT;
  private String                         CACHE_MISS;
  private String                         profilerPrefix;
  private String                         profilerMetadataPrefix;

  public OOffHeapGlobalRecordCache() {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();

    profilerPrefix = "db." + db.getName() + ".cache.global.";
    profilerMetadataPrefix = "db.*.cache.global.";

    CACHE_HIT = profilerPrefix + "cache.found";
    CACHE_MISS = profilerPrefix + "cache.notFound";

    cache = ChronicleMapBuilder.of(OCachedRID.class, OCachedRecord.class).entries(1800000)
        .constantKeySizeBySample(new OCachedRID(new ORecordId(0, 0))).create();

    Orient.instance().scheduleTask(new TimerTask() {
      @Override
      public void run() {
        System.out.println(String.format("GLOBAL CACHE (%s) size=%d CACHE_HIT=%d CACHE_MISS=%d", this, getSize(), Orient.instance()
            .getProfiler().getCounter(CACHE_HIT), Orient.instance().getProfiler().getCounter(CACHE_MISS)));
      }
    }, 1000, 1000);
  }

  public int getSize() {
    return cache.size();
  }

  /**
   * Pushes record to cache. Identifier of record used as access key
   *
   * @param record
   *          record that should be cached
   */
  public void updateRecord(final ORecord record) {
    final OCachedRecord cachedRecord;
    if (record instanceof ODocument)
      cachedRecord = new OCachedDocument((ODocument) record);
    else if (record instanceof ORecordBytes)
      cachedRecord = new OCachedRecordBytes((ORecordBytes) record);
    else {
      OLogManager.instance().warn(this, "Unsupported record type: " + record);
      return;
    }

    cache.put(new OCachedRID(cachedRecord.getIdentity()), cachedRecord);
  }

  public ORecord findRecord(final ORID rid) {
    final OCachedRecord record = cache.get(new OCachedRID(rid));

    if (record != null) {
      Orient.instance().getProfiler().updateCounter(CACHE_HIT, "Record found in Level1 Cache", 1L, "db.*.cache.global.cache.found");
      return record.toRecord();
    } else
      Orient.instance().getProfiler()
          .updateCounter(CACHE_MISS, "Record not found in Level1 Cache", 1L, "db.*.cache.global.cache.notFound");

    return null;
  }

  public void deleteRecord(final ORID rid) {
    cache.remove(new OCachedRID(rid));
  }

  public void clear() {
    cache.clear();
  }

  @Override
  public String toString() {
    return "STORAGE level cache records = " + getSize();
  }
}
