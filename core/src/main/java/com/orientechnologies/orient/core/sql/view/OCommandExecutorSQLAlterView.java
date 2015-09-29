package com.orientechnologies.orient.core.sql.view;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;

import java.util.Map;

/**
 * Interface for a class implementing a view schema object
 *
 * @author Matan Shukry
 * @since 29/9/2015
 */
public class OCommandExecutorSQLAlterView extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_ALTER    = "ALTER";
  public static final String KEYWORD_VIEW     = "VIEW";
  public static final String KEYWORD_NAME     = "NAME";
  public static final String KEYWORD_QUERY    = "QUERY";

  private String              viewName;
  private String              newViewName;
  private String              query;

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  private int wrapParseNextWord(int iPos, StringBuilder word, boolean iUpperCase) {
    return wrapParseNextWord(iPos, word, iUpperCase, null);
  }

  private int wrapParseNextWord(int iPos, StringBuilder word, boolean iUpperCase, String expectedWord) {
    int pos = nextWord(parserText, parserTextUpperCase, iPos, word, iUpperCase);
    if (pos == -1 || (expectedWord != null && (!word.toString().equals(expectedWord)))) {
      throw new OCommandSQLParsingException("Keyword " + expectedWord + " not found. Use " + getSyntax(), parserText, iPos);
    }

    return pos;
  }

  @Override
  public OCommandExecutorSQLAlterView parse(OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    StringBuilder word = new StringBuilder();

    int pos = wrapParseNextWord(0, word, true, KEYWORD_ALTER);
    pos = wrapParseNextWord(pos, word, true, KEYWORD_VIEW);
    pos = wrapParseNextWord(pos, word, false);
    this.viewName = word.toString();

    pos = wrapParseNextWord(pos, word, true);
    if (word.toString().equals(KEYWORD_NAME)) {
      pos = wrapParseNextWord(pos, word, false);
      this.newViewName = word.toString();

      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    }

    /* Query */
    if (pos != -1) {
      if (word.toString().equals(KEYWORD_QUERY)) {
        parseQuery(pos, word);
      } else {
        throw new OCommandSQLParsingException("Expected keyword not found. Use " + getSyntax(),
          parserText, pos);
      }
    }

    /*  */
    return this;
  }

  private void parseQuery(final int iPos, final StringBuilder word) {
    int pos = nextWord(parserText, parserTextUpperCase, iPos, word, false, "");
    if (pos == -1) {
      throw new OCommandSQLParsingException("Query string not found. Use " + getSyntax(), parserText, iPos);
    }
    this.query = word.toString();
  }

  @Override
  public Object execute(final Map<Object, Object> iArgs) {
    if (this.viewName == null) {
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");
    }

    final OSchema schema = getDatabase().getMetadata().getSchema();
    OView view = schema.getView(this.viewName);
    if (view == null) {
      throw new OCommandExecutionException("Cannot alter view '" + this.viewName + "' because it is not found");
    }

    if (this.newViewName != null) {
      view.setName(this.newViewName);
    }
    if (this.query != null) {
      view.setQuery(this.query);
    }

    return null;
  }


  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getSyntax() {
    return "ALTER VIEW <view> [NAME <new-name>] [QUERY <new-query>]";
  }

  @Override
  public boolean involveSchema() {
    return true;
  }
}

