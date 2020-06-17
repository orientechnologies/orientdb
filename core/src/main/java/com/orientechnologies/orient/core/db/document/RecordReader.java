package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;

/** @Internal */
public interface RecordReader {
  ORawBuffer readRecord(
      OStorage storage,
      ORecordId rid,
      String fetchPlan,
      boolean ignoreCache,
      final int recordVersion)
      throws ORecordNotFoundException;
}
