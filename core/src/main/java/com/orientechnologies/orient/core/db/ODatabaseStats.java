package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;

public class ODatabaseStats {
  public long loadedRecords;
  public long averageLoadRecordTimeMs;
  public long minLoadRecordTimeMs;
  public long maxLoadRecordTimeMs;

  public long prefetchedRidbagsCount;
  public long ridbagPrefetchTimeMs;
  public long minRidbagPrefetchTimeMs;
  public long maxRidbagPrefetchTimeMs;

  public OResult toResult() {
    OResultInternal result = new OResultInternal();
    result.setProperty("loadedRecords", loadedRecords);
    result.setProperty("averageLoadRecordTimeMs", averageLoadRecordTimeMs);
    result.setProperty("minLoadRecordTimeMs", minLoadRecordTimeMs);
    result.setProperty("maxLoadRecordTimeMs", maxLoadRecordTimeMs);
    result.setProperty("prefetchedRidbagsCount", prefetchedRidbagsCount);
    result.setProperty("ridbagPrefetchTimeMs", ridbagPrefetchTimeMs);
    result.setProperty("minRidbagPrefetchTimeMs", minRidbagPrefetchTimeMs);
    result.setProperty("maxRidbagPrefetchTimeMs", maxRidbagPrefetchTimeMs);

    return result;
  }
}
