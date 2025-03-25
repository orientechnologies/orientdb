package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.Collection;

public interface OIndexKeySource {

  Collection<Object> key(OCommandContext ctx, boolean asc);
}
