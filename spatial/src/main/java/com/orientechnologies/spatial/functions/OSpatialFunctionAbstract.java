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

package com.orientechnologies.spatial.functions;

import com.orientechnologies.lucene.collections.LuceneResultSet;
import com.orientechnologies.spatial.index.OLuceneSpatialIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.*;
import com.orientechnologies.spatial.shape.OShapeFactory;
import com.orientechnologies.spatial.strategy.SpatialQueryBuilderAbstract;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Enrico Risa on 31/08/15.
 */
public abstract class OSpatialFunctionAbstract extends OSQLFunctionAbstract implements OIndexableSQLFunction {

  OShapeFactory factory = OShapeFactory.INSTANCE;

  public OSpatialFunctionAbstract(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  protected OIndex searchForIndex(OFromClause target, OExpression[] args) {

    // TODO Check if target is a class otherwise exception

    OFromItem item = target.getItem();
    OBaseIdentifier identifier = item.getIdentifier();
    String fieldName = args[0].toString();

    Set<OIndex<?>> indexes = getDb().getMetadata().getIndexManager().getClassInvolvedIndexes(identifier.toString(), fieldName);
    for (OIndex<?> index : indexes) {
      if (index.getInternal() instanceof OLuceneSpatialIndex) {
        return index;
      }
    }
    return null;
  }

  protected ODatabaseDocumentInternal getDb() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  protected LuceneResultSet results(OFromClause target, OExpression[] args, OCommandContext ctx) {
    OIndex oIndex = searchForIndex(target, args);
    if (oIndex != null) {
      Map<String, Object> queryParams = new HashMap<String, Object>();
      queryParams.put(SpatialQueryBuilderAbstract.GEO_FILTER, operator());
      Object shape;
      if (args[1].getValue() instanceof OJson) {
        OJson json = (OJson) args[1].getValue();
        ODocument doc = new ODocument().fromJSON(json.toString());
        shape = doc.toMap();
      } else {
        shape = args[1].execute(null, ctx);
      }
      queryParams.put(SpatialQueryBuilderAbstract.SHAPE, shape);
      return (LuceneResultSet) oIndex.get(queryParams);
    }
    return null;
  }

  protected abstract String operator();
}
