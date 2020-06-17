package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import java.util.Map;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 2/28/2015
 */
public class OCommandExecutorSQLDropSequence extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_SEQUENCE = "SEQUENCE";

  private String sequenceName;

  @Override
  public OCommandExecutorSQLDropSequence parse(OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      final ODatabaseDocumentInternal database = getDatabase();
      final StringBuilder word = new StringBuilder();

      parserRequiredKeyword("DROP");
      parserRequiredKeyword("SEQUENCE");
      this.sequenceName = parserRequiredWord(false, "Expected <sequence name>");
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs) {
    if (this.sequenceName == null) {
      throw new OCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    final ODatabaseDocument database = getDatabase();
    try {
      database.getMetadata().getSequenceLibrary().dropSequence(this.sequenceName);
    } catch (ODatabaseException exc) {
      String message = "Unable to execute command: " + exc.getMessage();
      OLogManager.instance().error(this, message, exc, (Object) null);
      throw new OCommandExecutionException(message);
    }
    return true;
  }

  @Override
  public String getSyntax() {
    return "DROP SEQUENCE <sequence>";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }
}
