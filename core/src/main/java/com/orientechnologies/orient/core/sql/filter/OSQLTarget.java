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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLResultsetDelegate;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OCommandSQLResultset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Target parser.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLTarget extends OBaseParser {
  protected final boolean                     empty;
  protected final OCommandContext             context;
  protected String                            targetVariable;
  protected String                            targetQuery;
  protected Iterable<? extends OIdentifiable> targetRecords;
  protected Map<String, String>               targetClusters;
  protected Map<OClass, String>               targetClasses;

  protected String                            targetIndex;

  protected String                            targetIndexValues;
  protected boolean                           targetIndexValuesAsc;

  public OSQLTarget(final String iText, final OCommandContext iContext, final String iFilterKeyword) {
    super();
    context = iContext;
    parserText = iText;
    parserTextUpperCase = upperCase(iText);

    try {
      empty = !extractTargets();

    } catch (OQueryParsingException e) {
      if (e.getText() == null)
        // QUERY EXCEPTION BUT WITHOUT TEXT: NEST IT
        throw new OQueryParsingException("Error on parsing query", parserText, parserGetCurrentPosition(), e);

      throw e;
    } catch (Throwable t) {
      throw new OQueryParsingException("Error on parsing query", parserText, parserGetCurrentPosition(), t);
    }
  }

  protected String upperCase(String text) {
    // TODO remove and refactor (see same method in OCommandExecutorAbstract)
    StringBuilder result = new StringBuilder(text.length());
    for (char c : text.toCharArray()) {
      String upper = ("" + c).toUpperCase(Locale.ENGLISH);
      if (upper.length() > 1) {
        result.append(c);
      } else {
        result.append(upper);
      }
    }
    return result.toString();
  }

  public Map<String, String> getTargetClusters() {
    return targetClusters;
  }

  public Map<OClass, String> getTargetClasses() {
    return targetClasses;
  }

  public Iterable<? extends OIdentifiable> getTargetRecords() {
    return targetRecords;
  }

  public String getTargetQuery() {
    return targetQuery;
  }

  public String getTargetIndex() {
    return targetIndex;
  }

  public String getTargetIndexValues() {
    return targetIndexValues;
  }

  public boolean isTargetIndexValuesAsc() {
    return targetIndexValuesAsc;
  }

  @Override
  public String toString() {
    if (targetClasses != null)
      return "class " + targetClasses.keySet();
    else if (targetClusters != null)
      return "cluster " + targetClusters.keySet();
    if (targetIndex != null)
      return "index " + targetIndex;
    if (targetRecords != null)
      return "records from " + targetRecords.getClass().getSimpleName();
    if (targetVariable != null)
      return "variable " + targetVariable;
    return "?";
  }

  public String getTargetVariable() {
    return targetVariable;
  }

  public boolean isEmpty() {
    return empty;
  }

  @Override
  protected void throwSyntaxErrorException(String iText) {
    throw new OCommandSQLParsingException(iText + ". Use " + getSyntax(), parserText, parserGetPreviousPosition());
  }

  @SuppressWarnings("unchecked")
  private boolean extractTargets() {
    parserSkipWhiteSpaces();

    if (parserIsEnded())
      throw new OQueryParsingException("No query target found", parserText, 0);

    final char c = parserGetCurrentChar();

    if (c == '$') {
      targetVariable = parserRequiredWord(false, "No valid target");
      targetVariable = targetVariable.substring(1);
    } else if (c == OStringSerializerHelper.LINK || Character.isDigit(c)) {
      // UNIQUE RID
      targetRecords = new ArrayList<OIdentifiable>();
      ((List<OIdentifiable>) targetRecords).add(new ORecordId(parserRequiredWord(true, "No valid RID")));

    } else if (c == OStringSerializerHelper.EMBEDDED_BEGIN) {
      // SUB QUERY
      final StringBuilder subText = new StringBuilder(256);
      parserSetCurrentPosition(OStringSerializerHelper.getEmbedded(parserText, parserGetCurrentPosition(), -1, subText) + 1);
      final OCommandSQL subCommand = new OCommandSQLResultset(subText.toString());

      final OCommandExecutorSQLResultsetDelegate executor = (OCommandExecutorSQLResultsetDelegate) OCommandManager.instance()
          .getExecutor(subCommand);
      executor.setProgressListener(subCommand.getProgressListener());
      executor.parse(subCommand);
      OCommandContext childContext = executor.getContext();
      if(childContext!=null) {
        childContext.setParent(context);
      }

      if (!(executor instanceof Iterable<?>))
        throw new OCommandSQLParsingException("Sub-query cannot be iterated because doesn't implement the Iterable interface: "
            + subCommand);

      targetQuery = subText.toString();
      targetRecords = executor;

    } else if (c == OStringSerializerHelper.LIST_BEGIN) {
      // COLLECTION OF RIDS
      final List<String> rids = new ArrayList<String>();
      parserSetCurrentPosition(OStringSerializerHelper.getCollection(parserText, parserGetCurrentPosition(), rids));

      targetRecords = new ArrayList<OIdentifiable>();
      for (String rid : rids)
        ((List<OIdentifiable>) targetRecords).add(new ORecordId(rid));

      parserMoveCurrentPosition(1);
    } else {

      while (!parserIsEnded()
          && (targetClasses == null && targetClusters == null && targetIndex == null && targetIndexValues == null && targetRecords == null)) {
        String originalSubjectName = parserRequiredWord(false, "Target not found");
        String subjectName = originalSubjectName.toUpperCase();

        final String alias;
        if (subjectName.equals("AS"))
          alias = parserRequiredWord(true, "Alias not found");
        else
          alias = subjectName;

        final String subjectToMatch = subjectName;
        if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX)) {
          // REGISTER AS CLUSTER
          if (targetClusters == null)
            targetClusters = new HashMap<String, String>();
          final String clusterNames = subjectName.substring(OCommandExecutorSQLAbstract.CLUSTER_PREFIX.length());
          if (clusterNames.startsWith("[") && clusterNames.endsWith("]")) {
            final Collection<String> clusters = new HashSet<String>(3);
            OStringSerializerHelper.getCollection(clusterNames, 0, clusters);
            for (String cl : clusters) {
              targetClusters.put(cl, cl);
            }
          } else
            targetClusters.put(clusterNames, alias);

        } else if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.INDEX_PREFIX)) {
          // REGISTER AS INDEX
          targetIndex = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_PREFIX.length());
        } else if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.METADATA_PREFIX)) {
          // METADATA
          final String metadataTarget = subjectName.substring(OCommandExecutorSQLAbstract.METADATA_PREFIX.length());
          targetRecords = new ArrayList<OIdentifiable>();

          if (metadataTarget.equals(OCommandExecutorSQLAbstract.METADATA_SCHEMA)) {
            ((ArrayList<OIdentifiable>) targetRecords).add(new ORecordId(ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
                .getConfiguration().schemaRecordId));
          } else if (metadataTarget.equals(OCommandExecutorSQLAbstract.METADATA_INDEXMGR)) {
            ((ArrayList<OIdentifiable>) targetRecords).add(new ORecordId(ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
                .getConfiguration().indexMgrRecordId));
          } else
            throw new OQueryParsingException("Metadata element not supported: " + metadataTarget);

        } else if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.DICTIONARY_PREFIX)) {
          // DICTIONARY
          final String key = originalSubjectName.substring(OCommandExecutorSQLAbstract.DICTIONARY_PREFIX.length());
          targetRecords = new ArrayList<OIdentifiable>();

          final OIdentifiable value = ODatabaseRecordThreadLocal.INSTANCE.get().getDictionary().get(key);
          if (value != null)
            ((List<OIdentifiable>) targetRecords).add(value);

        } else if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.INDEX_VALUES_PREFIX)) {
          targetIndexValues = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_VALUES_PREFIX.length());
          targetIndexValuesAsc = true;
        } else if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.INDEX_VALUES_ASC_PREFIX)) {
          targetIndexValues = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_VALUES_ASC_PREFIX.length());
          targetIndexValuesAsc = true;
        } else if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.INDEX_VALUES_DESC_PREFIX)) {
          targetIndexValues = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_VALUES_DESC_PREFIX.length());
          targetIndexValuesAsc = false;
        } else {
          if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.CLASS_PREFIX))
            // REGISTER AS CLASS
            subjectName = subjectName.substring(OCommandExecutorSQLAbstract.CLASS_PREFIX.length());

          // REGISTER AS CLASS
          if (targetClasses == null)
            targetClasses = new HashMap<OClass, String>();

          final OClass cls = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().getClass(subjectName);
          if (cls == null)
            throw new OCommandExecutionException("Class '" + subjectName + "' was not found in current database");

          targetClasses.put(cls, alias);
        }
      }
    }

    return !parserIsEnded();
  }
}
