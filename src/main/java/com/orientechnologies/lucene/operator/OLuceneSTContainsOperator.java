/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.lucene.operator;

import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.lucene.strategy.SpatialQueryBuilderAbstract;
import com.orientechnologies.lucene.strategy.SpatialQueryBuilderContains;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexCursorCollectionValue;
import com.orientechnologies.orient.core.index.OIndexCursorSingleValue;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class OLuceneSTContainsOperator extends OLuceneSpatialOperator {

  public OLuceneSTContainsOperator() {
    super("STCONTAINS", 5, false);
  }

  @Override
  public Collection<OIdentifiable> filterRecords(ODatabase<?> iRecord, List<String> iTargetClasses, OSQLFilterCondition iCondition,
      Object iLeft, Object iRight) {
    return null;
  }



  @Override
  public OIndexCursor executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder) {
    OIndexCursor cursor;
    Object key;
    key = keyParams.get(0);
    if (key instanceof Map) {
      ((Map) key).put(SpatialQueryBuilderAbstract.GEO_FILTER, SpatialQueryBuilderContains.NAME);
    }
    Object indexResult = index.get(key);
    if (indexResult == null || indexResult instanceof OIdentifiable)
      cursor = new OIndexCursorSingleValue((OIdentifiable) indexResult, new OSpatialCompositeKey(keyParams));
    else
      cursor = new OIndexCursorCollectionValue(((Collection<OIdentifiable>) indexResult).iterator(), new OSpatialCompositeKey(
          keyParams));

    return cursor;
  }

}
