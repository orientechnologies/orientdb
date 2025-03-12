package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;

public interface OIndexKeySource {

  Object key(OCommandContext ctx);
}
