package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.id.ORID;
import java.util.SortedSet;

public interface OLockKeySource {
  SortedSet<ORID> getRids();

  SortedSet<OPair<String, String>> getUniqueKeys();
}
