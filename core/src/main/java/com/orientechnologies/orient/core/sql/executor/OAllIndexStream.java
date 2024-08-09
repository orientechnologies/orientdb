package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexInternal;
import java.util.stream.Stream;

class OAllIndexStream implements OIndexStream {
  private OIndexInternal index;
  private boolean asc;

  public OAllIndexStream(OIndexInternal index, boolean asc) {
    super();
    this.index = index;
    this.asc = asc;
  }

  public Stream<ORawPair<Object, ORID>> start(OCommandContext ctx) {
    if (asc) {
      return index.stream();
    } else {
      return index.descStream();
    }
  }
}
