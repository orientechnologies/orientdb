package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.freespacemap;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v2.FreeSpaceMapPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

public final class InitFreeSpaceMapPO extends PageOperationRecord {

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
    page.init();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
  }

  @Override
  public int getId() {
    return WALRecordTypes.FREE_SPACE_MAP_INIT;
  }
}
