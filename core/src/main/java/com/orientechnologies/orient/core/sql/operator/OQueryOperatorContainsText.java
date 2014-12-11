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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexCursorCollectionValue;
import com.orientechnologies.orient.core.index.OIndexCursorSingleValue;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexFullText;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

import java.util.Collection;
import java.util.List;

/**
 * CONTAINSTEXT operator. Look if a text is contained in a property. This is usually used with the FULLTEXT-INDEX for fast lookup at
 * piece of text.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorContainsText extends OQueryTargetOperator {
  private boolean ignoreCase = true;

  public OQueryOperatorContainsText(final boolean iIgnoreCase) {
    super("CONTAINSTEXT", 5, false);
    ignoreCase = iIgnoreCase;
  }

  public OQueryOperatorContainsText() {
    super("CONTAINSTEXT", 5, false);
  }

  @Override
  public String getSyntax() {
    return "<left> CONTAINSTEXT[( noignorecase ] )] <right>";
  }

  /**
   * This is executed on non-indexed fields.
   */
  @Override
  public Object evaluateRecord(final OIdentifiable iRecord, ODocument iCurrentResult, final OSQLFilterCondition iCondition,
      final Object iLeft, final Object iRight, OCommandContext iContext) {
    if (iLeft == null || iRight == null)
      return false;

    return iLeft.toString().indexOf(iRight.toString()) > -1;
  }

  @SuppressWarnings({ "unchecked", "deprecation" })
  @Override
  public Collection<OIdentifiable> filterRecords(final ODatabase<?> iDatabase, final List<String> iTargetClasses,
      final OSQLFilterCondition iCondition, final Object iLeft, final Object iRight) {

    final String fieldName;
    if (iCondition.getLeft() instanceof OSQLFilterItemField)
      fieldName = iCondition.getLeft().toString();
    else
      fieldName = iCondition.getRight().toString();

    final String fieldValue;
    if (iCondition.getLeft() instanceof OSQLFilterItemField)
      fieldValue = iCondition.getRight().toString();
    else
      fieldValue = iCondition.getLeft().toString();

    final String className = iTargetClasses.get(0);

    final OProperty prop = ((OMetadataInternal) iDatabase.getMetadata()).getImmutableSchemaSnapshot().getClass(className)
        .getProperty(fieldName);
    if (prop == null)
      // NO PROPERTY DEFINED
      return null;

    OIndex<?> fullTextIndex = null;
    for (final OIndex<?> indexDefinition : prop.getIndexes()) {
      if (indexDefinition instanceof OIndexFullText) {
        fullTextIndex = indexDefinition;
        break;
      }
    }

    if (fullTextIndex == null) {
      return null;
    }

    return (Collection<OIdentifiable>) fullTextIndex.get(fieldValue);
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.INDEX_METHOD;
  }

  @Override
  public OIndexCursor executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder) {

    final OIndexDefinition indexDefinition = index.getDefinition();
    if (indexDefinition.getParamCount() > 1)
      return null;

    final OIndex<?> internalIndex = index.getInternal();

    OIndexCursor cursor;
    if (internalIndex instanceof OIndexFullText) {
      final Object key = indexDefinition.createValue(keyParams);
      final Object indexResult = index.get(key);

      if (indexResult == null || indexResult instanceof OIdentifiable)
        cursor = new OIndexCursorSingleValue((OIdentifiable) indexResult, key);
      else
        cursor = new OIndexCursorCollectionValue(((Collection<OIdentifiable>) indexResult).iterator(), key);
    } else
      return null;

    updateProfiler(iContext, internalIndex, keyParams, indexDefinition);

    return cursor;
  }

  @Override
  public ORID getBeginRidRange(Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public ORID getEndRidRange(Object iLeft, Object iRight) {
    return null;
  }
}
