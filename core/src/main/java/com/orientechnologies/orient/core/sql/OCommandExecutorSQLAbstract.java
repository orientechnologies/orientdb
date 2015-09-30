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

import com.orientechnologies.orient.core.command.OCommandContext.TIMEOUT_STRATEGY;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestAbstract;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.OrientSql;
import com.orientechnologies.orient.core.sql.parser.ParseException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * SQL abstract Command Executor implementation.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OCommandExecutorSQLAbstract extends OCommandExecutorAbstract {

  public static final String KEYWORD_FROM             = "FROM";
  public static final String KEYWORD_LET              = "LET";
  public static final String KEYWORD_WHERE            = "WHERE";
  public static final String KEYWORD_LIMIT            = "LIMIT";
  public static final String KEYWORD_SKIP             = "SKIP";
  public static final String KEYWORD_OFFSET           = "OFFSET";
  public static final String KEYWORD_TIMEOUT          = "TIMEOUT";
  public static final String KEYWORD_LOCK             = "LOCK";
  public static final String KEYWORD_RETURN           = "RETURN";
  public static final String KEYWORD_KEY              = "key";
  public static final String KEYWORD_RID              = "rid";
  public static final String CLUSTER_PREFIX           = "CLUSTER:";
  public static final String CLASS_PREFIX             = "CLASS:";
  public static final String INDEX_PREFIX             = "INDEX:";
  public static final String KEYWORD_UNSAFE           = "UNSAFE";

  public static final String INDEX_VALUES_PREFIX      = "INDEXVALUES:";
  public static final String INDEX_VALUES_ASC_PREFIX  = "INDEXVALUESASC:";
  public static final String INDEX_VALUES_DESC_PREFIX = "INDEXVALUESDESC:";

  public static final String DICTIONARY_PREFIX        = "DICTIONARY:";
  public static final String METADATA_PREFIX          = "METADATA:";
  public static final String METADATA_SCHEMA          = "SCHEMA";
  public static final String METADATA_INDEXMGR        = "INDEXMANAGER";

  public static final String DEFAULT_PARAM_USER       = "$user";

  protected long             timeoutMs                = OGlobalConfiguration.COMMAND_TIMEOUT.getValueAsLong();
  protected TIMEOUT_STRATEGY timeoutStrategy          = TIMEOUT_STRATEGY.EXCEPTION;
  protected OStatement       preParsedStatement;

  /**
   * The command is replicated
   *
   * @return
   */
  public OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE.REPLICATE;
  }

  public boolean isIdempotent() {
    return false;
  }

  protected void throwSyntaxErrorException(final String iText) {
    throw new OCommandSQLParsingException(iText + ". Use " + getSyntax(), parserText, parserGetPreviousPosition());
  }

  protected void throwParsingException(final String iText) {
    throw new OCommandSQLParsingException(iText, parserText, parserGetPreviousPosition());
  }

  /**
   * Parses the timeout keyword if found.
   */
  protected boolean parseTimeout(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_TIMEOUT))
      return false;

    String word = parserNextWord(true);

    try {
      timeoutMs = Long.parseLong(word);
    } catch (Exception e) {
      throwParsingException("Invalid " + KEYWORD_TIMEOUT + " value set to '" + word + "' but it should be a valid long. Example: "
          + KEYWORD_TIMEOUT + " 3000");
    }

    if (timeoutMs < 0)
      throwParsingException("Invalid " + KEYWORD_TIMEOUT + ": value set minor than ZERO. Example: " + KEYWORD_TIMEOUT + " 10000");

    word = parserNextWord(true);

    if (word != null)
      if (word.equals(TIMEOUT_STRATEGY.EXCEPTION.toString()))
        timeoutStrategy = TIMEOUT_STRATEGY.EXCEPTION;
      else if (word.equals(TIMEOUT_STRATEGY.RETURN.toString()))
        timeoutStrategy = TIMEOUT_STRATEGY.RETURN;
      else
        parserGoBack();

    return true;
  }

  /**
   * Parses the lock keyword if found.
   */
  protected String parseLock() throws OCommandSQLParsingException {
    final String lockStrategy = parserNextWord(true);

    if (!lockStrategy.equalsIgnoreCase("DEFAULT") && !lockStrategy.equalsIgnoreCase("NONE")
        && !lockStrategy.equalsIgnoreCase("RECORD"))
      throwParsingException("Invalid " + KEYWORD_LOCK + " value set to '" + lockStrategy
          + "' but it should be NONE (default) or RECORD. Example: " + KEYWORD_LOCK + " RECORD");

    return lockStrategy;
  }

  protected Set<String> getInvolvedClustersOfClasses(final Collection<String> iClassNames) {
    final ODatabaseDocument db = getDatabase();

    final Set<String> clusters = new HashSet<String>();

    for (String clazz : iClassNames) {
      final OClass cls = ((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot().getClass(clazz);
      if (cls != null)
        for (int clId : cls.getPolymorphicClusterIds()) {
          // FILTER THE CLUSTER WHERE THE USER HAS THE RIGHT ACCESS
          if (clId > -1 && checkClusterAccess(db, db.getClusterNameById(clId)))
            clusters.add(db.getClusterNameById(clId).toLowerCase());
        }
    }

    return clusters;
  }

  protected Set<String> getInvolvedClustersOfClusters(final Collection<String> iClusterNames) {
    final ODatabaseDocument db = getDatabase();

    final Set<String> clusters = new HashSet<String>();

    for (String cluster : iClusterNames) {
      final String c = cluster.toLowerCase();
      // FILTER THE CLUSTER WHERE THE USER HAS THE RIGHT ACCESS
      if (checkClusterAccess(db, c))
        clusters.add(c);
    }

    return clusters;
  }

  protected Set<String> getInvolvedClustersOfIndex(final String iIndexName) {
    final ODatabaseDocumentInternal db = getDatabase();

    final Set<String> clusters = new HashSet<String>();

    final OMetadataInternal metadata = (OMetadataInternal) db.getMetadata();
    final OIndex<?> idx = metadata.getIndexManager().getIndex(iIndexName);
    if (idx != null) {
      final String clazz = idx.getDefinition().getClassName();

      if (clazz != null) {
        final OClass cls = metadata.getImmutableSchemaSnapshot().getClass(clazz);
        if (cls != null)
          for (int clId : cls.getClusterIds()) {
            final String clName = db.getClusterNameById(clId);
            if (clName != null)
              clusters.add(clName.toLowerCase());
          }
      }
    }

    return clusters;
  }

  protected boolean checkClusterAccess(final ODatabaseDocument db, final String iClusterName) {
    return db.getUser() == null
        || db.getUser().checkIfAllowed(ORule.ResourceGeneric.CLUSTER, iClusterName, getSecurityOperationType()) != null;
  }

  protected void bindDefaultContextVariables() {
    if (context != null) {
      if (getDatabase() != null && getDatabase().getUser() != null) {
        context.setVariable(DEFAULT_PARAM_USER, getDatabase().getUser().getIdentity());
      }
    }
  }

  protected String preParse(final String queryText, final OCommandRequest iRequest) {
    final boolean strict = getDatabase().getStorage().getConfiguration().isStrictSql();

    if (strict) {
      final InputStream is = new ByteArrayInputStream(queryText.getBytes());
      final OrientSql osql = new OrientSql(is);
      try {
        final OStatement result = osql.parse();
        preParsedStatement = result;

        if (iRequest instanceof OCommandRequestAbstract) {
          final Map<Object, Object> params = ((OCommandRequestAbstract) iRequest).getParameters();
          result.replaceParameters(params);
        }

        return result.toString();
      } catch (ParseException e) {
        throwParsingException("Error parsing query: \n" + queryText + "\n" + e.getMessage());
      }
      return "ERROR!";
    }
    return queryText;
  }

}
