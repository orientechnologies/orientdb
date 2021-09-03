package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.util.Map;

/**
 * Drops a use.
 *
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 4/22/2015
 */
public class OCommandExecutorSQLDropUser extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_USER = "USER";

  private static final String SYNTAX = "DROP USER <user-name>";
  private static final String USER_CLASS = "OUser";
  private static final String USER_FIELD_NAME = "name";

  private String userName;

  @Override
  public OCommandExecutorSQLDropUser parse(OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    parserRequiredKeyword(KEYWORD_DROP);
    parserRequiredKeyword(KEYWORD_USER);
    this.userName = parserRequiredWord(false, "Expected <user name>");

    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs) {
    if (this.userName == null) {
      throw new OCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    // Build following command:
    // DELETE FROM OUser WHERE name='<name>'

    //
    StringBuilder sb = new StringBuilder();
    sb.append("DELETE FROM ");
    sb.append(USER_CLASS);
    sb.append(" WHERE ");
    sb.append(USER_FIELD_NAME);
    sb.append("='");
    sb.append(this.userName);
    sb.append("'");

    //
    return getDatabase().command(new OCommandSQL(sb.toString())).execute();
  }

  @Override
  public String getSyntax() {
    return SYNTAX;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }
}
