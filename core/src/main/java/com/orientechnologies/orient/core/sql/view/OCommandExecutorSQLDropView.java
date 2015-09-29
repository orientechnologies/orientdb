package com.orientechnologies.orient.core.sql.view;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;

import java.util.Map;

/**
 * Interface for a class implementing a view schema object
 *
 * @author Matan Shukry
 * @since 29/9/2015
 */
public class OCommandExecutorSQLDropView extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_DROP     = "DROP";
  public static final String KEYWORD_VIEW     = "VIEW";

  private String              viewName;

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  private int wrapParseNextWord(int iPos, StringBuilder word, boolean iUpperCase, String expectedWord) {
    int pos = nextWord(parserText, parserTextUpperCase, iPos, word, iUpperCase);
    if (pos == -1 || (expectedWord != null && (!word.toString().equals(expectedWord)))) {
      throw new OCommandSQLParsingException("Keyword " + expectedWord + " not found. Use " + getSyntax(), parserText, iPos);
    }

    return pos;
  }

  @Override
  public OCommandExecutorSQLDropView parse(OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    StringBuilder word = new StringBuilder();

    int pos = wrapParseNextWord(0, word, true, KEYWORD_DROP);

    pos = wrapParseNextWord(pos, word, true, KEYWORD_VIEW);
    pos = wrapParseNextWord(pos, word, true, null);
    this.viewName = word.toString();

    /*  */
    return this;
  }

  @Override
  public Object execute(final Map<Object, Object> iArgs) {
    if (this.viewName == null) {
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");
    }

    final OSchema schema = getDatabase().getMetadata().getSchema();

    schema.dropView(this.viewName);
    return schema.countViews();
  }


  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getSyntax() {
    return "DROP VIEW <view>";
  }

  @Override
  public boolean involveSchema() {
    return true;
  }
}
