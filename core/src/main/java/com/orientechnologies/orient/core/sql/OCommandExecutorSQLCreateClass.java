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

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.parser.OCreateClassStatement;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SQL CREATE CLASS command: Creates a new property in the target class.
 * <p>
 * <<<<<<< HEAD
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) =======
 * @author Luca Garulli >>>>>>> a565f3e... Fixed Parsing of superclasses with backtick
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateClass extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_CREATE   = "CREATE";
  public static final String KEYWORD_CLASS    = "CLASS";
  public static final String KEYWORD_EXTENDS  = "EXTENDS";
  public static final String KEYWORD_ABSTRACT = "ABSTRACT";
  public static final String KEYWORD_CLUSTER  = "CLUSTER";
  public static final String KEYWORD_CLUSTERS = "CLUSTERS";
  public static final String KEYWORD_IF       = "IF";
  public static final String KEYWORD_NOT      = "NOT";
  public static final String KEYWORD_EXISTS   = "EXISTS";

  private String className;

  private List<OClass> superClasses = new ArrayList<OClass>();
  private int[] clusterIds;
  private Integer clusters    = null;
  private boolean ifNotExists = false;

  public OCommandExecutorSQLCreateClass parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      final ODatabaseDocumentInternal database = getDatabase();
      init((OCommandRequestText) iRequest);

      StringBuilder word = new StringBuilder();

      int oldPos = 0;
      int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
        throw new OCommandSQLParsingException("Keyword " + KEYWORD_CREATE + " not found. Use " + getSyntax(), parserText, oldPos);

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_CLASS))
        throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found. Use " + getSyntax(), parserText, oldPos);

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
      if (pos == -1)
        throw new OCommandSQLParsingException("Expected <class>", parserText, oldPos);

      className = word.toString();
      if (this.preParsedStatement != null) {
        className = ((OCreateClassStatement) preParsedStatement).name.getStringValue();
      }
      if (className == null)
        throw new OCommandSQLParsingException("Expected <class>", parserText, oldPos);

      oldPos = pos;

      while ((pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true)) > -1) {
        final String k = word.toString();
        if (k.equals(KEYWORD_EXTENDS)) {
          boolean hasNext;
          boolean newParser = this.preParsedStatement != null;
          OClass superClass;
          do {
            oldPos = pos;
            pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
            if (pos == -1)
              throw new OCommandSQLParsingException(
                  "Syntax error after EXTENDS for class " + className + ". Expected the super-class name. Use " + getSyntax(),
                  parserText, oldPos);
            String superclassName = decodeClassName(word.toString());

            if (!database.getMetadata().getSchema().existsClass(superclassName) && !newParser)
              throw new OCommandSQLParsingException("Super-class " + word + " not exists", parserText, oldPos);
            superClass = database.getMetadata().getSchema().getClass(superclassName);
            superClasses.add(superClass);
            hasNext = false;
            for (; pos < parserText.length(); pos++) {
              char ch = parserText.charAt(pos);
              if (ch == ',')
                hasNext = true;
              else if (Character.isLetterOrDigit(ch))
                break;
            }
          } while (hasNext);
          if (newParser) {
            OCreateClassStatement statement = (OCreateClassStatement) this.preParsedStatement;
            List<OIdentifier> superclasses = statement.getSuperclasses();
            this.superClasses.clear();
            for (OIdentifier superclass : superclasses) {
              String superclassName = superclass.getStringValue();
              if (!database.getMetadata().getSchema().existsClass(superclassName))
                throw new OCommandSQLParsingException("Super-class " + word + " not exists", parserText, oldPos);
              superClass = database.getMetadata().getSchema().getClass(superclassName);
              this.superClasses.add(superClass);
            }
          }
        } else if (k.equals(KEYWORD_CLUSTER)) {
          oldPos = pos;
          pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false, " =><()");
          if (pos == -1)
            throw new OCommandSQLParsingException(
                "Syntax error after CLUSTER for class " + className + ". Expected the cluster id or name. Use " + getSyntax(),
                parserText, oldPos);

          final String[] clusterIdsAsStrings = word.toString().split(",");
          if (clusterIdsAsStrings.length > 0) {
            clusterIds = new int[clusterIdsAsStrings.length];
            for (int i = 0; i < clusterIdsAsStrings.length; ++i) {
              if (Character.isDigit(clusterIdsAsStrings[i].charAt(0)))
                // GET CLUSTER ID FROM NAME
                clusterIds[i] = Integer.parseInt(clusterIdsAsStrings[i]);
              else
                // GET CLUSTER ID
                clusterIds[i] = database.getStorage().getClusterIdByName(clusterIdsAsStrings[i]);

              if (clusterIds[i] == -1)
                throw new OCommandSQLParsingException("Cluster with id " + clusterIds[i] + " does not exists", parserText, oldPos);

              try {
                database.getStorage().getClusterById(clusterIds[i]);
              } catch (Exception e) {
                throw new OCommandSQLParsingException("Cluster with id " + clusterIds[i] + " does not exists", parserText, oldPos);
              }
            }
          }
        } else if (k.equals(KEYWORD_CLUSTERS)) {
          oldPos = pos;
          pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false, " =><()");
          if (pos == -1)
            throw new OCommandSQLParsingException(
                "Syntax error after CLUSTERS for class " + className + ". Expected the number of clusters. Use " + getSyntax(),
                parserText, oldPos);

          clusters = Integer.parseInt(word.toString());
        } else if (k.equals(KEYWORD_ABSTRACT)) {
          clusterIds = new int[] { -1 };
        } else if (k.equals(KEYWORD_IF)) {
          oldPos = pos;
          pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false, " =><()");
          if (!word.toString().equalsIgnoreCase(KEYWORD_NOT)) {
            throw new OCommandSQLParsingException(
                "Syntax error after IF for class " + className + ". Expected NOT. Use " + getSyntax(), parserText, oldPos);
          }
          oldPos = pos;
          pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false, " =><()");
          if (!word.toString().equalsIgnoreCase(KEYWORD_EXISTS)) {
            throw new OCommandSQLParsingException(
                "Syntax error after IF NOT for class " + className + ". Expected EXISTS. Use " + getSyntax(), parserText, oldPos);
          }
          ifNotExists = true;
        } else
          throw new OCommandSQLParsingException("Invalid keyword: " + k);

        oldPos = pos;
      }

      if (clusterIds == null) {
        final int clusterId = database.getStorage().getClusterIdByName(className);
        if (clusterId > -1) {
          clusterIds = new int[] { clusterId };
        }
      }

    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase().getConfiguration().getValueAsLong(OGlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  /**
   * Execute the CREATE CLASS.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (className == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseDocument database = getDatabase();

    boolean alreadyExists = database.getMetadata().getSchema().existsClass(className);
    if (!alreadyExists || !ifNotExists) {
      if (clusters != null)
        database.getMetadata().getSchema().createClass(className, clusters, superClasses.toArray(new OClass[0]));
      else
        database.getMetadata().getSchema().createClass(className, clusterIds, superClasses.toArray(new OClass[0]));
    }

    return database.getMetadata().getSchema().getClasses().size();
  }

  @Override
  public String getSyntax() {
    return "CREATE CLASS <class> [IF NOT EXISTS] [EXTENDS <super-class> [,<super-class2>*] ] [CLUSTER <clusterId>*] [CLUSTERS <total-cluster-number>] [ABSTRACT]";
  }

  @Override
  public String getUndoCommand() {
    return "drop class " + className;
  }

  @Override
  public boolean involveSchema() {
    return true;
  }
}
