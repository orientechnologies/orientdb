package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ORecordId;

/**
 * Base interface for the distributed database repairer.
 * 
 * @author Luca Garulli
 */
public interface ODistributedDatabaseRepairer {
  void repairRecord(ORecordId rid);

  void repairCluster(int brokenRecord);

  long getRecordProcessed();

  long getTotalTimeProcessing();

  void shutdown();
}
