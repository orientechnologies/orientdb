/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.OMatchStatement;
import com.orientechnologies.orient.core.sql.parser.OProfileStorageStatement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Default command operator executor factory.
 *
 * @author Johann Sorel (Geomatys)
 */
public class ODefaultCommandExecutorSQLFactory implements OCommandExecutorSQLFactory {

  private static final Map<String, Class<? extends OCommandExecutor>> COMMANDS;

  static {

    // COMMANDS
    final Map<String, Class<? extends OCommandExecutor>> commands =
        new HashMap<String, Class<? extends OCommandExecutor>>();
    commands.put(
        OCommandExecutorSQLAlterDatabase.KEYWORD_ALTER
            + " "
            + OCommandExecutorSQLAlterDatabase.KEYWORD_DATABASE,
        OCommandExecutorSQLAlterDatabase.class);
    commands.put(OCommandExecutorSQLSelect.KEYWORD_SELECT, OCommandExecutorSQLSelect.class);
    commands.put(OCommandExecutorSQLSelect.KEYWORD_FOREACH, OCommandExecutorSQLSelect.class);
    commands.put(OCommandExecutorSQLTraverse.KEYWORD_TRAVERSE, OCommandExecutorSQLTraverse.class);
    commands.put(OCommandExecutorSQLInsert.KEYWORD_INSERT, OCommandExecutorSQLInsert.class);
    commands.put(OCommandExecutorSQLUpdate.KEYWORD_UPDATE, OCommandExecutorSQLUpdate.class);
    commands.put(OCommandExecutorSQLDelete.NAME, OCommandExecutorSQLDelete.class);
    commands.put(OCommandExecutorSQLCreateFunction.NAME, OCommandExecutorSQLCreateFunction.class);
    commands.put(OCommandExecutorSQLGrant.KEYWORD_GRANT, OCommandExecutorSQLGrant.class);
    commands.put(OCommandExecutorSQLRevoke.KEYWORD_REVOKE, OCommandExecutorSQLRevoke.class);
    commands.put(
        OCommandExecutorSQLCreateLink.KEYWORD_CREATE
            + " "
            + OCommandExecutorSQLCreateLink.KEYWORD_LINK,
        OCommandExecutorSQLCreateLink.class);
    commands.put(
        OCommandExecutorSQLCreateIndex.KEYWORD_CREATE
            + " "
            + OCommandExecutorSQLCreateIndex.KEYWORD_INDEX,
        OCommandExecutorSQLCreateIndex.class);
    commands.put(
        OCommandExecutorSQLDropIndex.KEYWORD_DROP
            + " "
            + OCommandExecutorSQLDropIndex.KEYWORD_INDEX,
        OCommandExecutorSQLDropIndex.class);
    commands.put(
        OCommandExecutorSQLRebuildIndex.KEYWORD_REBUILD
            + " "
            + OCommandExecutorSQLRebuildIndex.KEYWORD_INDEX,
        OCommandExecutorSQLRebuildIndex.class);
    commands.put(
        OCommandExecutorSQLCreateClass.KEYWORD_CREATE
            + " "
            + OCommandExecutorSQLCreateClass.KEYWORD_CLASS,
        OCommandExecutorSQLCreateClass.class);
    commands.put(
        OCommandExecutorSQLCreateCluster.KEYWORD_CREATE
            + " "
            + OCommandExecutorSQLCreateCluster.KEYWORD_CLUSTER,
        OCommandExecutorSQLCreateCluster.class);
    commands.put(
        OCommandExecutorSQLCreateCluster.KEYWORD_CREATE
            + " "
            + OCommandExecutorSQLCreateCluster.KEYWORD_BLOB
            + " "
            + OCommandExecutorSQLCreateCluster.KEYWORD_CLUSTER,
        OCommandExecutorSQLCreateCluster.class);
    commands.put(
        OCommandExecutorSQLAlterClass.KEYWORD_ALTER
            + " "
            + OCommandExecutorSQLAlterClass.KEYWORD_CLASS,
        OCommandExecutorSQLAlterClass.class);
    commands.put(
        OCommandExecutorSQLCreateProperty.KEYWORD_CREATE
            + " "
            + OCommandExecutorSQLCreateProperty.KEYWORD_PROPERTY,
        OCommandExecutorSQLCreateProperty.class);
    commands.put(
        OCommandExecutorSQLAlterProperty.KEYWORD_ALTER
            + " "
            + OCommandExecutorSQLAlterProperty.KEYWORD_PROPERTY,
        OCommandExecutorSQLAlterProperty.class);
    commands.put(
        OCommandExecutorSQLDropCluster.KEYWORD_DROP
            + " "
            + OCommandExecutorSQLDropCluster.KEYWORD_CLUSTER,
        OCommandExecutorSQLDropCluster.class);
    commands.put(
        OCommandExecutorSQLDropClass.KEYWORD_DROP
            + " "
            + OCommandExecutorSQLDropClass.KEYWORD_CLASS,
        OCommandExecutorSQLDropClass.class);
    commands.put(
        OCommandExecutorSQLDropProperty.KEYWORD_DROP
            + " "
            + OCommandExecutorSQLDropProperty.KEYWORD_PROPERTY,
        OCommandExecutorSQLDropProperty.class);
    commands.put(
        OCommandExecutorSQLFindReferences.KEYWORD_FIND
            + " "
            + OCommandExecutorSQLFindReferences.KEYWORD_REFERENCES,
        OCommandExecutorSQLFindReferences.class);
    commands.put(
        OCommandExecutorSQLTruncateClass.KEYWORD_TRUNCATE
            + " "
            + OCommandExecutorSQLTruncateClass.KEYWORD_CLASS,
        OCommandExecutorSQLTruncateClass.class);
    commands.put(
        OCommandExecutorSQLTruncateCluster.KEYWORD_TRUNCATE
            + " "
            + OCommandExecutorSQLTruncateCluster.KEYWORD_CLUSTER,
        OCommandExecutorSQLTruncateCluster.class);
    commands.put(
        OCommandExecutorSQLTruncateRecord.KEYWORD_TRUNCATE
            + " "
            + OCommandExecutorSQLTruncateRecord.KEYWORD_RECORD,
        OCommandExecutorSQLTruncateRecord.class);
    commands.put(
        OCommandExecutorSQLAlterCluster.KEYWORD_ALTER
            + " "
            + OCommandExecutorSQLAlterCluster.KEYWORD_CLUSTER,
        OCommandExecutorSQLAlterCluster.class);
    commands.put(
        OCommandExecutorSQLCreateSequence.KEYWORD_CREATE
            + " "
            + OCommandExecutorSQLCreateSequence.KEYWORD_SEQUENCE,
        OCommandExecutorSQLCreateSequence.class);
    commands.put(
        OCommandExecutorSQLAlterSequence.KEYWORD_ALTER
            + " "
            + OCommandExecutorSQLAlterSequence.KEYWORD_SEQUENCE,
        OCommandExecutorSQLAlterSequence.class);
    commands.put(
        OCommandExecutorSQLDropSequence.KEYWORD_DROP
            + " "
            + OCommandExecutorSQLDropSequence.KEYWORD_SEQUENCE,
        OCommandExecutorSQLDropSequence.class);
    commands.put(
        OCommandExecutorSQLCreateUser.KEYWORD_CREATE
            + " "
            + OCommandExecutorSQLCreateUser.KEYWORD_USER,
        OCommandExecutorSQLCreateUser.class);
    commands.put(
        OCommandExecutorSQLDropUser.KEYWORD_DROP + " " + OCommandExecutorSQLDropUser.KEYWORD_USER,
        OCommandExecutorSQLDropUser.class);
    commands.put(OCommandExecutorSQLExplain.KEYWORD_EXPLAIN, OCommandExecutorSQLExplain.class);
    commands.put(
        OCommandExecutorSQLTransactional.KEYWORD_TRANSACTIONAL,
        OCommandExecutorSQLTransactional.class);

    commands.put(OMatchStatement.KEYWORD_MATCH, OMatchStatement.class);
    commands.put(
        OCommandExecutorSQLOptimizeDatabase.KEYWORD_OPTIMIZE,
        OCommandExecutorSQLOptimizeDatabase.class);

    commands.put(
        OProfileStorageStatement.KEYWORD_PROFILE, OCommandExecutorToOStatementWrapper.class);

    // GRAPH

    commands.put(OCommandExecutorSQLCreateEdge.NAME, OCommandExecutorSQLCreateEdge.class);
    commands.put(OCommandExecutorSQLDeleteEdge.NAME, OCommandExecutorSQLDeleteEdge.class);
    commands.put(OCommandExecutorSQLCreateVertex.NAME, OCommandExecutorSQLCreateVertex.class);
    commands.put(OCommandExecutorSQLDeleteVertex.NAME, OCommandExecutorSQLDeleteVertex.class);
    commands.put(OCommandExecutorSQLMoveVertex.NAME, OCommandExecutorSQLMoveVertex.class);

    COMMANDS = Collections.unmodifiableMap(commands);
  }

  /** {@inheritDoc} */
  public Set<String> getCommandNames() {
    return COMMANDS.keySet();
  }

  /** {@inheritDoc} */
  public OCommandExecutor createCommand(final String name) throws OCommandExecutionException {
    final Class<? extends OCommandExecutor> clazz = COMMANDS.get(name);

    if (clazz == null) {
      throw new OCommandExecutionException("Unknowned command name :" + name);
    }

    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw OException.wrapException(
          new OCommandExecutionException(
              "Error in creation of command "
                  + name
                  + "(). Probably there is not an empty constructor or the constructor generates errors"),
          e);
    }
  }
}
