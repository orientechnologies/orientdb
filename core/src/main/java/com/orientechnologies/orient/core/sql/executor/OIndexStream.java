package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import java.util.stream.Stream;

public interface OIndexStream {
  public Stream<ORawPair<Object, ORID>> start(OCommandContext ctx);
}
