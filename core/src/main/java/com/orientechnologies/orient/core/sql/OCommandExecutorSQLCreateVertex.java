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

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;

import java.util.*;

/**
 * SQL CREATE VERTEX command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OCommandExecutorSQLCreateVertex extends OCommandExecutorSQLSetAware implements OCommandDistributedReplicateRequest {
  public static final String NAME = "CREATE VERTEX";
  private OClass                      clazz;
  private String                      clusterName;
  private List<OPair<String, Object>> fields;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLCreateVertex parse(final OCommandRequest iRequest) {

    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      final ODatabaseDocument database = getDatabase();

      init((OCommandRequestText) iRequest);

      String className = null;

      parserRequiredKeyword("CREATE");
      parserRequiredKeyword("VERTEX");

      String temp = parseOptionalWord(true);

      while (temp != null) {
        if (temp.equals("CLUSTER")) {
          clusterName = parserRequiredWord(false);

        } else if (temp.equals(KEYWORD_SET)) {
          fields = new ArrayList<OPair<String, Object>>();
          parseSetFields(clazz, fields);

        } else if (temp.equals(KEYWORD_CONTENT)) {
          parseContent();

        } else if (className == null && temp.length() > 0) {
          className = temp;
          if (className == null)
            // ASSIGN DEFAULT CLASS
            className = "V";

          // GET/CHECK CLASS NAME
          clazz = ((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot().getClass(className);
          if (clazz == null)
            throw new OCommandSQLParsingException("Class '" + className + "' was not found");
        }

        temp = parserOptionalWord(true);
        if (parserIsEnded())
          break;
      }

      if (className == null) {
        // ASSIGN DEFAULT CLASS
        className = "V";

        // GET/CHECK CLASS NAME
        clazz = ((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot().getClass(className);
        if (clazz == null)
          throw new OCommandSQLParsingException("Class '" + className + "' was not found");
      }
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  /**
   * Execute the command and return the ODocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clazz == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    // CREATE VERTEX DOES NOT HAVE TO BE IN TX
    final OVertex vertex = getDatabase().newVertex(clazz);

    if (fields != null)
      // EVALUATE FIELDS
      for (final OPair<String, Object> f : fields) {
        if (f.getValue() instanceof OSQLFunctionRuntime)
          f.setValue(((OSQLFunctionRuntime) f.getValue()).getValue(vertex.getRecord(), null, context));
      }

    OSQLHelper.bindParameters(vertex.getRecord(), fields, new OCommandParameters(iArgs), context);

    if (content != null)
      ((ODocument)vertex.getRecord()).merge(content, true, false);

    if (clusterName != null)
      vertex.save(clusterName);
    else
      vertex.save();

    return vertex.getRecord();

  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    if (clazz != null)
      return Collections.singleton(getDatabase().getClusterNameById(clazz.getClusterSelection().getCluster(clazz, null)));
    else if (clusterName != null)
      return getInvolvedClustersOfClusters(Collections.singleton(clusterName));

    return Collections.EMPTY_SET;
  }

  @Override
  public String getSyntax() {
    return "CREATE VERTEX [<class>] [CLUSTER <cluster>] [SET <field> = <expression>[,]*]";
  }
}
