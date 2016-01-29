package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceHelper;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 2/28/2015
 */
public class OCommandExecutorSQLCreateSequence extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String     KEYWORD_CREATE    = "CREATE";
  public static final String     KEYWORD_SEQUENCE  = "SEQUENCE";
  public static final String     KEYWORD_TYPE      = "TYPE";
  public static final String     KEYWORD_START     = "START";
  public static final String     KEYWORD_INCREMENT = "INCREMENT";
  public static final String     KEYWORD_CACHE     = "CACHE";

  private String                 sequenceName;
  private SEQUENCE_TYPE          sequenceType;
  private OSequence.CreateParams params;

  @Override
  public OCommandExecutorSQLCreateSequence parse(OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final ODatabaseDocumentInternal database = getDatabase();
    final StringBuilder word = new StringBuilder();

    parserRequiredKeyword(KEYWORD_CREATE);
    parserRequiredKeyword(KEYWORD_SEQUENCE);
    this.sequenceName = parserRequiredWord(false, "Expected <sequence name>");
    this.params = new OSequence.CreateParams().setDefaults();

    String temp;
    while ((temp = parseOptionalWord(true)) != null) {
      if (parserIsEnded()) {
        break;
      }

      if (temp.equals(KEYWORD_TYPE)) {
        String typeAsString = parserRequiredWord(true, "Expected <sequence type>");
        try {
          this.sequenceType = OSequenceHelper.getSequenceTyeFromString(typeAsString);
        } catch (IllegalArgumentException e) {
          throw new OCommandSQLParsingException("Unknown sequence type '" + typeAsString + "'. Supported attributes are: "
              + Arrays.toString(SEQUENCE_TYPE.values()));
        }
      } else if (temp.equals(KEYWORD_START)) {
        String startAsString = parserRequiredWord(true, "Expected <start value>");
        this.params.start = Long.parseLong(startAsString);
      } else if (temp.equals(KEYWORD_INCREMENT)) {
        String incrementAsString = parserRequiredWord(true, "Expected <increment value>");
        this.params.increment = Integer.parseInt(incrementAsString);
      } else if (temp.equals(KEYWORD_CACHE)) {
        String cacheAsString = parserRequiredWord(true, "Expected <cache value>");
        this.params.cacheSize = Integer.parseInt(cacheAsString);
      }
    }

    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs) {
    if (this.sequenceName == null) {
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");
    }

    final ODatabaseDocument database = getDatabase();

    database.getMetadata().getSequenceLibrary().createSequence(this.sequenceName, this.sequenceType, this.params);

    return database.getMetadata().getSequenceLibrary().getSequenceCount();
  }

  @Override
  public String getSyntax() {
    return "CREATE SEQUENCE <sequence> [TYPE <CACHED|ORDERED>] [START <value>] [INCREMENT <value>] [CACHE <value>]";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }
}
