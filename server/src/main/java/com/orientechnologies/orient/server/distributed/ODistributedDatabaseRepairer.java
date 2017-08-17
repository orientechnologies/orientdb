package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ORecordId;

import java.util.Collection;

/**
 * Base interface for the distributed database repairer.
 *
 * @author Luca Garulli
 */
public interface ODistributedDatabaseRepairer {
  void repairRecords(Collection<ORecordId> rids);

  void repairRecord(ORecordId rid);

  void enqueueRepairRecords(Collection<ORecordId> involvedRecords);

  void enqueueRepairRecord(ORecordId rid);

  void cancelRepairRecord(ORecordId rid);

  void enqueueRepairCluster(int brokenRecord);

  long getRecordProcessed();

  long getTotalTimeProcessing();

  void shutdown();
}
