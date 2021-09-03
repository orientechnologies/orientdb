/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Iterator;
import java.util.Map;

/**
 * SQL ALTER DATABASE command: Changes an attribute of the current database.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLOptimizeDatabase extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_OPTIMIZE = "OPTIMIZE";
  public static final String KEYWORD_DATABASE = "DATABASE";
  public static final String KEYWORD_EDGE = "-LWEDGES";
  public static final String KEYWORD_NOVERBOSE = "-NOVERBOSE";

  private boolean optimizeEdges = false;
  private boolean verbose = true;
  private int batch = 1000;

  public OCommandExecutorSQLOptimizeDatabase parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);
      init((OCommandRequestText) iRequest);

      StringBuilder word = new StringBuilder();

      int oldPos = 0;
      int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_OPTIMIZE))
        throw new OCommandSQLParsingException(
            "Keyword " + KEYWORD_OPTIMIZE + " not found. Use " + getSyntax(), parserText, oldPos);

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_DATABASE))
        throw new OCommandSQLParsingException(
            "Keyword " + KEYWORD_DATABASE + " not found. Use " + getSyntax(), parserText, oldPos);

      while (!parserIsEnded() && word.length() > 0) {
        oldPos = pos;
        pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
        if (word.toString().equals(KEYWORD_EDGE)) optimizeEdges = true;
        else if (word.toString().equals(KEYWORD_NOVERBOSE)) verbose = false;
      }
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /** Execute the ALTER DATABASE. */
  public Object execute(final Map<Object, Object> iArgs) {
    final StringBuilder result = new StringBuilder();

    if (optimizeEdges) result.append(optimizeEdges());

    return result.toString();
  }

  private String optimizeEdges() {
    final ODatabaseDocumentInternal db = getDatabase();

    db.declareIntent(new OIntentMassiveInsert());
    try {
      long transformed = 0;
      if (db.getTransaction().isActive()) db.commit();

      db.begin();

      try {

        final long totalEdges = db.countClass("E");
        long browsedEdges = 0;
        long lastLapBrowsed = 0;
        long lastLapTime = System.currentTimeMillis();

        for (ODocument doc : db.browseClass("E")) {
          if (Thread.currentThread().isInterrupted()) break;

          browsedEdges++;

          if (doc != null) {
            if (doc.fields() == 2) {
              final ORID edgeIdentity = doc.getIdentity();

              final ODocument outV = doc.field("out");
              final ODocument inV = doc.field("in");

              // OUTGOING
              final Object outField = outV.field("out_" + doc.getClassName());
              if (outField instanceof ORidBag) {
                final Iterator<OIdentifiable> it = ((ORidBag) outField).iterator();
                while (it.hasNext()) {
                  OIdentifiable v = it.next();
                  if (edgeIdentity.equals(v)) {
                    // REPLACE EDGE RID WITH IN-VERTEX RID
                    it.remove();
                    ((ORidBag) outField).add(inV.getIdentity());
                    break;
                  }
                }
              }

              outV.save();

              // INCOMING
              final Object inField = inV.field("in_" + doc.getClassName());
              if (outField instanceof ORidBag) {
                final Iterator<OIdentifiable> it = ((ORidBag) inField).iterator();
                while (it.hasNext()) {
                  OIdentifiable v = it.next();
                  if (edgeIdentity.equals(v)) {
                    // REPLACE EDGE RID WITH IN-VERTEX RID
                    it.remove();
                    ((ORidBag) inField).add(outV.getIdentity());
                    break;
                  }
                }
              }

              inV.save();

              doc.delete();

              if (++transformed % batch == 0) {
                db.commit();
                db.begin();
              }

              final long now = System.currentTimeMillis();

              if (verbose && (now - lastLapTime > 2000)) {
                final long elapsed = now - lastLapTime;

                OLogManager.instance()
                    .info(
                        this,
                        "Browsed %,d of %,d edges, transformed %,d so far (%,d edges/sec)",
                        browsedEdges,
                        totalEdges,
                        transformed,
                        (((browsedEdges - lastLapBrowsed) * 1000 / elapsed)));

                lastLapTime = System.currentTimeMillis();
                lastLapBrowsed = browsedEdges;
              }
            }
          }
        }

        // LAST COMMIT
        db.commit();

      } finally {
        if (db.getTransaction().isActive()) db.rollback();
      }
      return "Transformed " + transformed + " regular edges in lightweight edges";

    } finally {
      db.declareIntent(null);
    }
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  public String getSyntax() {
    return "OPTIMIZE DATABASE [-lwedges]";
  }
}
