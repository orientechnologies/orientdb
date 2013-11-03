/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.comparator.OCaseInsentiveComparator;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropProperty extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_DROP     = "DROP";
  public static final String KEYWORD_PROPERTY = "PROPERTY";

  private String             className;
  private String             fieldName;
  private boolean            force            = false;

  public OCommandExecutorSQLDropProperty parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_DROP))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_DROP + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_PROPERTY))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_PROPERTY + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected <class>.<property>. Use " + getSyntax(), parserText, pos);

    String[] parts = word.toString().split("\\.");
    if (parts.length != 2)
      throw new OCommandSQLParsingException("Expected <class>.<property>. Use " + getSyntax(), parserText, pos);

    className = parts[0];
    if (className == null)
      throw new OCommandSQLParsingException("Class not found", parserText, pos);
    fieldName = parts[1];

    pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
    if (pos != -1) {
      final String forceParameter = word.toString();
      if ("FORCE".equals(forceParameter)) {
        force = true;
      } else {
        throw new OCommandSQLParsingException("Wrong query parameter", parserText, pos);
      }
    }

    return this;
  }

  /**
   * Execute the CREATE PROPERTY.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (fieldName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not yet been parsed");

    final ODatabaseRecord database = getDatabase();
    final OClassImpl sourceClass = (OClassImpl) database.getMetadata().getSchema().getClass(className);
    if (sourceClass == null)
      throw new OCommandExecutionException("Source class '" + className + "' not found");

    final List<OIndex<?>> indexes = relatedIndexes(fieldName);
    if (!indexes.isEmpty()) {
      if (force) {
        dropRelatedIndexes(indexes);
      } else {
        final StringBuilder indexNames = new StringBuilder();

        boolean first = true;
        for (final OIndex<?> index : sourceClass.getClassInvolvedIndexes(fieldName)) {
          if (!first) {
            indexNames.append(", ");
          } else {
            first = false;
          }
          indexNames.append(index.getName());
        }

        throw new OCommandExecutionException("Property used in indexes (" + indexNames.toString()
            + "). Please drop these indexes before removing property or use FORCE parameter.");
      }
    }

    // REMOVE THE PROPERTY
    sourceClass.dropPropertyInternal(fieldName);
    sourceClass.saveInternal();

    return null;
  }

  private void dropRelatedIndexes(final List<OIndex<?>> indexes) {
    final ODatabaseRecord database = getDatabase();
    for (final OIndex<?> index : indexes) {
      database.command(new OCommandSQL("DROP INDEX " + index.getName())).execute();
    }
  }

  private List<OIndex<?>> relatedIndexes(final String fieldName) {
    final List<OIndex<?>> result = new ArrayList<OIndex<?>>();

    final ODatabaseRecord database = getDatabase();
    for (final OIndex<?> oIndex : database.getMetadata().getIndexManager().getClassIndexes(className)) {
      if (OCollections.indexOf(oIndex.getDefinition().getFields(), fieldName, new OCaseInsentiveComparator()) > -1) {
        result.add(oIndex);
      }
    }

    return result;
  }

  @Override
  public String getSyntax() {
    return "DROP PROPERTY <class>.<property>";
  }
}
