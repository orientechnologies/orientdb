package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.metadatapage;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexMetadataPageV2;

public final class LocalHashTableV2MetadataPageInitPO extends PageOperationRecord {
  @Override
  public void redo(final OCacheEntry cacheEntry) {
    final HashIndexMetadataPageV2 page = new HashIndexMetadataPageV2(cacheEntry);
    page.init();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    //do nothing
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_METADATA_PAGE_INIT_PO;
  }
}
