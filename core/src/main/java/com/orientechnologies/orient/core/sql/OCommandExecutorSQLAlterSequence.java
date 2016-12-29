package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;

import java.util.Map;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/5/2015
 */
public class OCommandExecutorSQLAlterSequence extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
    public static final String KEYWORD_ALTER = "ALTER";
    public static final String KEYWORD_SEQUENCE = "SEQUENCE";
    public static final String KEYWORD_START = "START";
    public static final String KEYWORD_INCREMENT = "INCREMENT";
    public static final String KEYWORD_CACHE = "CACHE";

    private String sequenceName;
    private OSequence.CreateParams params;

    @Override
    public OCommandExecutorSQLAlterSequence parse(OCommandRequest iRequest) {
        final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

        String queryText = textRequest.getText();
        String originalQuery = queryText;
        try {
            queryText = preParse(queryText, iRequest);
            textRequest.setText(queryText);

            init((OCommandRequestText) iRequest);

            final ODatabaseDocumentInternal database = getDatabase();
            final StringBuilder word = new StringBuilder();

            parserRequiredKeyword(KEYWORD_ALTER);
            parserRequiredKeyword(KEYWORD_SEQUENCE);
            this.sequenceName = parserRequiredWord(false, "Expected <sequence name>");
            this.params = new OSequence.CreateParams();

            String temp;
            while ((temp = parseOptionalWord(true)) != null) {
                if (parserIsEnded()) {
                    break;
                }

                if (temp.equals(KEYWORD_START)) {
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
        } finally {
            textRequest.setText(originalQuery);
        }
        return this;
    }

    @Override
    public Object execute(Map<Object, Object> iArgs) {
        if (this.sequenceName == null) {
            throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");
        }

        final ODatabaseDocument database = getDatabase();
        OSequence sequence = database.getMetadata().getSequenceLibrary().getSequence(this.sequenceName);

        boolean result = sequence.updateParams(this.params);
        sequence.reset();
        return result;
    }

    @Override
    public String getSyntax() {
        return "ALTER SEQUENCE <sequence> [START <value>] [INCREMENT <value>] [CACHE <value>]";
    }

    @Override
    public QUORUM_TYPE getQuorumType() {
        return QUORUM_TYPE.ALL;
    }
}
