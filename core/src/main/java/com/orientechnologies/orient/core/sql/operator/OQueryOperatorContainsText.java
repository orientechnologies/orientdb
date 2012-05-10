/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.sql.operator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexFullText;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

/**
 * CONTAINS KEY operator.
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

  @SuppressWarnings({ "unchecked", "deprecation" })
  @Override
  public Collection<OIdentifiable> filterRecords(final ODatabaseComplex<?> iDatabase, final List<String> iTargetClasses,
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

    final OProperty prop = iDatabase.getMetadata().getSchema().getClass(className).getProperty(fieldName);
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

  @SuppressWarnings("unchecked")
  @Override
  public Collection<OIdentifiable> executeIndexQuery(OIndex<?> index, List<Object> keyParams, int fetchLimit) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    if (indexDefinition.getParamCount() > 1)
      return null;

    final OIndex<?> internalIndex = index.getInternal();

    if (internalIndex instanceof OIndexFullText) {
      final Object indexResult = index.get(indexDefinition.createValue(keyParams));
      if (indexResult instanceof Collection)
        return (Collection<OIdentifiable>) indexResult;

      return indexResult == null ? null : Collections.singletonList((OIdentifiable) indexResult);
    }
    return null;
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
