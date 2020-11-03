package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.server.distributed.impl.lock.OLockGuard;
import com.orientechnologies.orient.server.distributed.impl.task.OLockKeySource;
import java.util.List;

/**
 * Methods required for scheduling transactions on a distributed database.
 *
 * <p>TODO: This interface is required to make testing easier, and ideally would include all related
 * methods from ODistributedDatabase. Alternatively, these methods could be moved to
 * ODistributedDatabase. However, the large interfaces in the server module and avoiding cyclic
 * dependency between the server and distributed module makes doing this a bit complicated!
 */
public interface OTxDistributedDatabase extends ODistributedDatabase {
  List<OLockGuard> localLock(OLockKeySource keySource);

  void localUnlock(List<OLockGuard> guards);
}
