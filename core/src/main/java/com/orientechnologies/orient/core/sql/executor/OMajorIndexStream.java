package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexInternal;
import java.util.stream.Stream;

public class OMajorIndexStream implements OIndexStream {
  private OIndexInternal index;
  private Object startKey;
  private boolean include;
  private boolean asc;

  public OMajorIndexStream(OIndexInternal index, Object startKey, boolean include, boolean asc) {
    super();
    this.index = index;
    this.startKey = startKey;
    this.include = include;
    this.asc = asc;
  }

  public Stream<ORawPair<Object, ORID>> start(OCommandContext ctx) {
    return index.streamEntriesMajor(startKey, include, asc);
  }
}
