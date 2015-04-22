package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.OSecurity;

import java.util.Map;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 4/22/2015
 */
public class OCommandExecutorSQLCreateUser extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_CREATE = "CREATE";
  public static final String KEYWORD_USER = "USER";
  public static final String KEYWORD_IDENTIFIED = "IDENTIFIED";
  public static final String KEYWORD_BY = "BY";
  public static final String KEYWORD_ROLE = "ROLE";
  public static final String SYNTAX = "CREATE USER <user name> IDENTIFIED BY <user password> [ ROLE <role name> ]";

  private static final String USER_CLASS = "OUser";
  private static final String USER_FIELD_NAME = "name";
  private static final String USER_FIELD_PASSWORD = "password";
  private static final String USER_FIELD_STATUS = "status";
  private static final String USER_FIELD_ROLES = "roles";

  private static final String DEFAULT_STATUS = "ACTIVE";
  private static final String DEFAULT_ROLE = "writer";
  private static final String ROLE_CLASS = "ORole";
  private static final String ROLE_FIELD_NAME = "name";

  private String userName;
  private String pass;
  private String role;

  @Override
  public OCommandExecutorSQLCreateUser parse(OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    parserRequiredKeyword(KEYWORD_CREATE);
    parserRequiredKeyword(KEYWORD_USER);
    this.userName = parserRequiredWord(false, "Expected <user name>");

    parserRequiredKeyword(KEYWORD_IDENTIFIED);
    parserRequiredKeyword(KEYWORD_BY);
    this.pass = parserRequiredWord(false, "Expected <user password>");

    String temp;
    while ((temp = parseOptionalWord(true)) != null) {
      if (parserIsEnded()) {
        break;
      }

      if (temp.equals(KEYWORD_ROLE)) {
        this.role = parserRequiredWord(false, "Expected <role name>");
      }
    }

    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs) {
    if (this.userName == null) {
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");
    }

    // Build following command:
    //    INSERT INTO OUser SET name='<name>',password='<pass>',status='ACTIVE'
    // with optinoal:
    //    ,role=(SELECT FROM ORole WHERE name='<role>')

    // INSERT INTO OUser SET
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ");
    sb.append(USER_CLASS);
    sb.append(" SET ");

    // name=<name>
    sb.append(USER_FIELD_NAME);
    sb.append("='");
    sb.append(this.userName);
    sb.append("'");

    // pass=<pass>
    sb.append(',');
    sb.append(USER_FIELD_PASSWORD);
    sb.append("='");
    sb.append(this.pass);
    sb.append("'");

    // status=ACTIVE
    sb.append(',');
    sb.append(USER_FIELD_STATUS);
    sb.append("='");
    sb.append(DEFAULT_STATUS);
    sb.append("'");

    // role=(select from ORole where name = <input_role || 'writer'>)
    if (this.role == null) {
      this.role = DEFAULT_ROLE;
    }

    sb.append(',');
    sb.append(USER_FIELD_ROLES);
    sb.append("=(SELECT FROM ");
    sb.append(ROLE_CLASS);
    sb.append(" WHERE ");
    sb.append(ROLE_FIELD_NAME);
    sb.append("='");
    sb.append(this.role);
    sb.append("')");

    return getDatabase().command(new OCommandSQL(sb.toString())).execute();
  }

  @Override
  public String getSyntax() {
    return SYNTAX;
  }
}
