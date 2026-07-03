package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.Map;

public interface OIdentifierResolver {

  String resolveIdentifierString(OCommandContext ctx);

  void toString(Map<Object, Object> params, StringBuilder builder);

  void toGenericStatement(StringBuilder builder);

  OIdentifierResolver copy();
}
