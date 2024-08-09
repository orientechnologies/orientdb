package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexInternal;
import java.util.stream.Stream;

public class ONullIndexStream implements OIndexStream {
  private OIndexInternal index;

  public ONullIndexStream(OIndexInternal index) {
    super();
    this.index = index;
  }

  public Stream<ORawPair<Object, ORID>> start(OCommandContext ctx) {
    final Stream<ORID> stream = index.getRids(null);
    return stream.map((rid) -> new ORawPair<>(null, rid));
  }
}
