/* Generated By:JJTree: Do not edit this line. OAlterPropertyStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;

import java.util.Map;

public class OAlterPropertyStatement extends OStatement {

  OIdentifier className;

  OIdentifier propertyName;
  OIdentifier customPropertyName;
  OExpression customPropertyValue;

  OIdentifier settingName;
  public OExpression settingValue;

  public OAlterPropertyStatement(int id) {
    super(id);
  }

  public OAlterPropertyStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public void validate(OrientSql.ValidationStats stats) throws OCommandSQLParsingException {
    super.validate(stats);//TODO
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("ALTER PROPERTY ");
    className.toString(params, builder);
    builder.append(".");
    propertyName.toString(params, builder);
    if (customPropertyName != null) {
      builder.append(" CUSTOM ");
      customPropertyName.toString(params, builder);
      builder.append(" = ");
      customPropertyValue.toString(params, builder);
    } else {
      builder.append(" ");
      settingName.toString(params, builder);
      builder.append(" ");
      settingValue.toString(params, builder);
    }
  }
}
/* JavaCC - OriginalChecksum=2421f6ad3b5f1f8e18149650ff80f1e7 (do not edit this line) */
