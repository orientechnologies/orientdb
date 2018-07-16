package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

public class OSBTreeBucketAddAllOperation extends OPageOperation {
  public OSBTreeBucketAddAllOperation() {
  }

  public OSBTreeBucketAddAllOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex) {
    super(pageLSN, fileId, pageIndex);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_ADD_ALL_OPERATION;
  }
}
