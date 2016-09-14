package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.orient.core.cache.OCommandCache;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OConcurrentResultSet;
import com.orientechnologies.orient.core.sql.query.OResultSet;

final class OCommandCacheRemoteResultListener extends OAbstractCommandResultListener {
  private final OCommandCache cmdCache;
  private OResultSet          collector = new OConcurrentResultSet<ORecord>();

  OCommandCacheRemoteResultListener(OCommandResultListener wrappedResultListener, OCommandCache cmdCache) {
    super(wrappedResultListener);
    this.cmdCache = cmdCache;
  }

  @Override
  public boolean isEmpty() {
    return collector != null && collector.isEmpty();
  }

  @Override
  public boolean result(final Object iRecord) {
    if (collector != null) {
      if (collector.currentSize() > cmdCache.getMaxResultsetSize()) {
        // TOO MANY RESULTS: STOP COLLECTING IT BECAUSE THEY WOULD NEVER CACHED
        collector = null;
      } else if (iRecord != null && iRecord instanceof ORecord)
        collector.add(iRecord);
    }
    return true;
  }

  @Override
  public Object getResult() {
    return collector;
  }

  @Override
  public void end() {
    collector.setCompleted();
  }

  @Override
  public void linkdedBySimpleValue(ODocument doc) {
    if (wrappedResultListener instanceof OAbstractCommandResultListener)
      ((OAbstractCommandResultListener) wrappedResultListener).linkdedBySimpleValue(doc);
  }

}