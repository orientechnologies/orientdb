/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;

import java.util.Map;

/**
 * SQL DROP CLASS command: Drops a class from the database. Cluster associated are removed too if are used exclusively by the
 * deleting class.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropClass extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_DROP   = "DROP";
  public static final String KEYWORD_CLASS  = "CLASS";
  public static final String KEYWORD_UNSAFE = "UNSAFE";

  private String             className;
  private boolean            unsafe;

  public OCommandExecutorSQLDropClass parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_DROP)) {
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_DROP + " not found. Use " + getSyntax(), parserText, oldPos);
    }

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CLASS)) {
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found. Use " + getSyntax(), parserText, oldPos);
    }

    pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
    if (pos == -1) {
      throw new OCommandSQLParsingException("Expected <class>. Use " + getSyntax(), parserText, pos);
    }

    className = word.toString();

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos > -1 && KEYWORD_UNSAFE.equalsIgnoreCase(word.toString())) {
      unsafe = true;
    }

    return this;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  /**
   * Execute the DROP CLASS.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (className == null) {
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");
    }

    final ODatabaseDocument database = getDatabase();
    final OClass cls = database.getMetadata().getSchema().getClass(className);
    if (cls == null) {
      return null;
    }

    final long records = cls.count(true);

    if (records > 0 && !unsafe) {
      // NOT EMPTY, CHECK IF CLASS IS OF VERTEX OR EDGES
      if (cls.isSubClassOf("V")) {
        // FOUND VERTEX CLASS
        throw new OCommandExecutionException(
            "'DROP CLASS' command cannot drop class '"
                + className
                + "' because it contains Vertices. Use 'DELETE VERTEX' command first to avoid broken edges in a database, or apply the 'UNSAFE' keyword to force it");
      } else if (cls.isSubClassOf("E")) {
        // FOUND EDGE CLASS
        throw new OCommandExecutionException(
            "'DROP CLASS' command cannot drop class '"
                + className
                + "' because it contains Edges. Use 'DELETE EDGE' command first to avoid broken vertices in a database, or apply the 'UNSAFE' keyword to force it");
      }
    }

    database.getMetadata().getSchema().dropClass(className);

    if (records > 0 && unsafe) {
      // NOT EMPTY, CHECK IF CLASS IS OF VERTEX OR EDGES
      if (cls.isSubClassOf("V")) {
        // FOUND VERTICES
        if (unsafe)
          OLogManager.instance().warn(this,
              "Dropped class '%s' containing %d vertices using UNSAFE mode. Database could contain broken edges", className,
              records);
      } else if (cls.isSubClassOf("E")) {
        // FOUND EDGES
        OLogManager.instance().warn(this,
            "Dropped class '%s' containing %d edges using UNSAFE mode. Database could contain broken vertices", className, records);
      }
    }

    return true;
  }

  @Override
  public String getSyntax() {
    return "DROP CLASS <class> [UNSAFE]";
  }

  @Override
  public boolean involveSchema() {
    return true;
  }
}
