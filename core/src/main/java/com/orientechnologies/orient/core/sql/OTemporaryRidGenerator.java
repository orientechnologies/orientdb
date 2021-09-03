package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandContext;

public interface OTemporaryRidGenerator {

  int getTemporaryRIDCounter(final OCommandContext iContext);
}
