package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexInternal;
import java.util.stream.Stream;

public class OBetweenIndexStream implements OIndexStream {
  private OIndexInternal index;
  private Object startKey;
  private boolean includeStart;
  private Object endKey;
  private boolean includeEnd;
  private boolean asc;

  public OBetweenIndexStream(
      OIndexInternal index,
      Object startKey,
      boolean includeStart,
      Object endKey,
      boolean includeEnd,
      boolean asc) {
    super();
    this.index = index;
    this.startKey = startKey;
    this.includeStart = includeStart;
    this.endKey = endKey;
    this.includeEnd = includeEnd;
    this.asc = asc;
  }

  public Stream<ORawPair<Object, ORID>> start(OCommandContext ctx) {
    return index.streamEntriesBetween(startKey, includeStart, endKey, includeEnd, asc);
  }
}
