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

package com.orientechnologies.lucene.operator;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.lucene.query.OLuceneKeyAndMetadata;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.sql.OIndexSearchResult;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryTargetOperator;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

public class OLuceneTextOperator extends OQueryTargetOperator {

  public static final String MEMORY_INDEX = "_memoryIndex";

  public OLuceneTextOperator() {
    this("LUCENE", 5, false);
  }

  public OLuceneTextOperator(String iKeyword, int iPrecedence, boolean iLogical) {
    super(iKeyword, iPrecedence, iLogical);
  }

  protected static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
    return OIndexReuseType.INDEX_OPERATOR;
  }

  @Override
  public OIndexSearchResult getOIndexSearchResult(
      OClass iSchemaClass,
      OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults,
      OCommandContext context) {

    // FIXME questo non trova l'indice se l'ordine e' errato
    return OLuceneOperatorUtil.buildOIndexSearchResult(
        iSchemaClass, iCondition, iIndexSearchResults, context);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> executeIndexQuery(
      OCommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    if (!index.getType().toLowerCase().contains("fulltext")) {
      return null;
    }
    if (index.getAlgorithm() == null || !index.getAlgorithm().toLowerCase().contains("lucene")) {
      return null;
    }

    //noinspection resource
    return index
        .getInternal()
        .getRids(
            new OLuceneKeyAndMetadata(
                new OLuceneCompositeKey(keyParams).setContext(iContext), new ODocument()))
        .map((rid) -> new ORawPair<>(new OLuceneCompositeKey(keyParams), rid));
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
  public boolean canBeMerged() {
    return false;
  }

  @Override
  public Object evaluateRecord(
      OIdentifiable iRecord,
      ODocument iCurrentResult,
      OSQLFilterCondition iCondition,
      Object iLeft,
      Object iRight,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {

    OLuceneFullTextIndex index = involvedIndex(iRecord, iCurrentResult, iCondition, iLeft, iRight);
    if (index == null) {
      return false;
    }

    MemoryIndex memoryIndex = (MemoryIndex) iContext.getVariable(MEMORY_INDEX);
    if (memoryIndex == null) {
      memoryIndex = new MemoryIndex();
      iContext.setVariable(MEMORY_INDEX, memoryIndex);
    }
    memoryIndex.reset();

    try {
      // In case of collection field evaluate the query with every item until matched

      if (iLeft instanceof List && index.isCollectionIndex()) {
        return matchCollectionIndex((List) iLeft, iRight, index, memoryIndex);
      } else {
        return matchField(iLeft, iRight, index, memoryIndex);
      }

    } catch (ParseException e) {
      OLogManager.instance().error(this, "error occurred while building query", e);

    } catch (IOException e) {
      OLogManager.instance().error(this, "error occurred while building memory index", e);
    }
    return null;
  }

  private boolean matchField(
      Object iLeft, Object iRight, OLuceneFullTextIndex index, MemoryIndex memoryIndex)
      throws IOException, ParseException {
    for (IndexableField field : index.buildDocument(iLeft).getFields()) {
      memoryIndex.addField(field, index.indexAnalyzer());
    }
    return memoryIndex.search(index.buildQuery(iRight)) > 0.0f;
  }

  private boolean matchCollectionIndex(
      List iLeft, Object iRight, OLuceneFullTextIndex index, MemoryIndex memoryIndex)
      throws IOException, ParseException {
    boolean match = false;
    List<Object> collections = transformInput(iLeft, iRight, index, memoryIndex);
    for (Object collection : collections) {
      memoryIndex.reset();
      match = match || matchField(collection, iRight, index, memoryIndex);
      if (match) {
        break;
      }
    }
    return match;
  }

  private List<Object> transformInput(
      List iLeft, Object iRight, OLuceneFullTextIndex index, MemoryIndex memoryIndex) {

    Integer collectionIndex = getCollectionIndex(iLeft);
    if (collectionIndex == -1) {
      // collection not found;
      return iLeft;
    }
    if (collectionIndex > 1) {
      throw new UnsupportedOperationException("Index of collection cannot be > 1");
    }
    // otherwise the input is [val,[]] or [[],val]
    Collection collection = (Collection) iLeft.get(collectionIndex);
    if (iLeft.size() == 1) {
      return new ArrayList<Object>(collection);
    }
    List<Object> transformed = new ArrayList<Object>(collection.size());
    for (Object o : collection) {
      List<Object> objects = new ArrayList<Object>();
      //  [[],val]
      if (collectionIndex == 0) {
        objects.add(o);
        objects.add(iLeft.get(1));
        //  [val,[]]
      } else {
        objects.add(iLeft.get(0));
        objects.add(o);
      }
      transformed.add(objects);
    }
    return transformed;
  }

  private Integer getCollectionIndex(List iLeft) {
    int i = 0;
    for (Object o : iLeft) {
      if (o instanceof Collection) {
        return i;
      }
      i++;
    }
    return -1;
  }

  protected OLuceneFullTextIndex involvedIndex(
      OIdentifiable iRecord,
      ODocument iCurrentResult,
      OSQLFilterCondition iCondition,
      Object iLeft,
      Object iRight) {

    ODocument doc = iRecord.getRecord();
    if (doc.getClassName() != null) {
      OClass cls = getDatabase().getMetadata().getSchema().getClass(doc.getClassName());

      if (isChained(iCondition.getLeft())) {

        OSQLFilterItemField chained = (OSQLFilterItemField) iCondition.getLeft();

        OSQLFilterItemField.FieldChain fieldChain = chained.getFieldChain();
        OClass oClass = cls;
        for (int i = 0; i < fieldChain.getItemCount() - 1; i++) {
          oClass = oClass.getProperty(fieldChain.getItemName(i)).getLinkedClass();
        }
        if (oClass != null) {
          cls = oClass;
        }
      }
      Set<OIndex> classInvolvedIndexes = cls.getInvolvedIndexes(fields(iCondition));
      OLuceneFullTextIndex idx = null;
      for (OIndex classInvolvedIndex : classInvolvedIndexes) {

        if (classInvolvedIndex.getInternal() instanceof OLuceneFullTextIndex) {
          idx = (OLuceneFullTextIndex) classInvolvedIndex.getInternal();
          break;
        }
      }
      return idx;
    } else {
      return null;
    }
  }

  private boolean isChained(Object left) {
    if (left instanceof OSQLFilterItemField) {
      OSQLFilterItemField field = (OSQLFilterItemField) left;
      return field.isFieldChain();
    }
    return false;
  }

  // returns a list of field names
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
