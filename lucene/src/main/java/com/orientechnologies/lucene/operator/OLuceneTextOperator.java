/*
 * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.lucene.operator;

import com.orientechnologies.lucene.collections.OFullTextCompositeKey;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexCursorCollectionValue;
import com.orientechnologies.orient.core.index.OIndexCursorSingleValue;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OIndexSearchResult;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryTargetOperator;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Query;

import java.util.*;

public class OLuceneTextOperator extends OQueryTargetOperator {

  public OLuceneTextOperator() {
    this("LUCENE", 5, false);
  }

  public OLuceneTextOperator(String iKeyword, int iPrecedence, boolean iLogical) {
    super(iKeyword, iPrecedence, iLogical);
  }

  @Override
  public OIndexCursor executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder) {
    OIndexCursor cursor;
    Object indexResult = index.get(new OFullTextCompositeKey(keyParams).setContext(iContext));
    if (indexResult == null || indexResult instanceof OIdentifiable)
      cursor = new OIndexCursorSingleValue((OIdentifiable) indexResult, new OFullTextCompositeKey(keyParams));
    else
      cursor = new OIndexCursorCollectionValue(((Collection<OIdentifiable>) indexResult).iterator(), new OFullTextCompositeKey(
          keyParams));
    iContext.setVariable("$luceneIndex", true);
    return cursor;
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
    return OIndexReuseType.INDEX_OPERATOR;
  }

  @Override
  public ORID getBeginRidRange(Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public ORID getEndRidRange(Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public OIndexSearchResult getOIndexSearchResult(OClass iSchemaClass, OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults, OCommandContext context) {
    return OLuceneOperatorUtil.buildOIndexSearchResult(iSchemaClass, iCondition, iIndexSearchResults, context);
  }

  @Override
  public Collection<OIdentifiable> filterRecords(ODatabase<?> iRecord, List<String> iTargetClasses, OSQLFilterCondition iCondition,
      Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public Object evaluateRecord(OIdentifiable iRecord, ODocument iCurrentResult, OSQLFilterCondition iCondition, Object iLeft,
      Object iRight, OCommandContext iContext) {

    OLuceneFullTextIndex index = involvedIndex(iRecord, iCurrentResult, iCondition, iLeft, iRight);

    if (index == null) {
      throw new OCommandExecutionException("Cannot evaluate lucene condition without index configuration.");
    }
    MemoryIndex memoryIndex = (MemoryIndex) iContext.getVariable("_memoryIndex");
    if (memoryIndex == null) {
      memoryIndex = new MemoryIndex();
      iContext.setVariable("_memoryIndex", memoryIndex);
    }
    memoryIndex.reset();
    Document doc = index.buildDocument(iLeft);

    for (IndexableField field : doc.getFields()) {
      memoryIndex.addField(field.name(), field.stringValue(), index.analyzer(field.name()));
    }
    Query query = null;
    try {
      query = index.buildQuery(iRight);
    } catch (Exception e) {
      throw new OCommandExecutionException("Error executing lucene query.", e);
    }
    return memoryIndex.search(query) > 0.0f;
  }

  protected OLuceneFullTextIndex involvedIndex(OIdentifiable iRecord, ODocument iCurrentResult, OSQLFilterCondition iCondition,
      Object iLeft, Object iRight) {

    Object left = iCondition.getLeft();
    ODocument doc = iRecord.getRecord();
    OClass cls = getDatabase().getMetadata().getSchema().getClass(doc.getClassName());
    if (isChained(iCondition.getLeft())) {

      OSQLFilterItemField chained = (OSQLFilterItemField) left;

      OSQLFilterItemField.FieldChain fieldChain = chained.getFieldChain();
      OClass oClass = cls;
      for (int i = 0; i < fieldChain.getItemCount() - 1; i++) {
        oClass = oClass.getProperty(fieldChain.getItemName(i)).getLinkedClass();
      }
      if (oClass != null) {
        cls = oClass;
      }
    }
    Set<OIndex<?>> classInvolvedIndexes = cls.getInvolvedIndexes(fields(iCondition));
    OLuceneFullTextIndex idx = null;
    for (OIndex<?> classInvolvedIndex : classInvolvedIndexes) {

      if (classInvolvedIndex.getInternal() instanceof OLuceneFullTextIndex) {
        idx = (OLuceneFullTextIndex) classInvolvedIndex.getInternal();
        break;
      }
    }
    return idx;

  }

  private boolean isChained(Object left) {
    if (left instanceof OSQLFilterItemField) {
      OSQLFilterItemField field = (OSQLFilterItemField) left;
      return field.isFieldChain();
    }
    return false;
  }

  protected static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  protected Collection<String> fields(OSQLFilterCondition iCondition) {

    Object left = iCondition.getLeft();

    if (left instanceof String) {
      String fName = (String) left;
      return Arrays.asList(fName);
    }
    if (left instanceof Collection) {
      Collection<OSQLFilterItemField> f = (Collection<OSQLFilterItemField>) left;

      List<String> fields = new ArrayList<String>();
      for (OSQLFilterItemField field : f) {
        fields.add(field.toString());
      }
      return fields;
    }
    if (left instanceof OSQLFilterItemField) {

      OSQLFilterItemField fName = (OSQLFilterItemField) left;
      if (fName.isFieldChain()) {
        int itemCount = fName.getFieldChain().getItemCount();
        return Arrays.asList(fName.getFieldChain().getItemName(itemCount - 1));
      } else {
        return Arrays.asList(fName.toString());
      }
    }
    return Collections.emptyList();
  }
}
