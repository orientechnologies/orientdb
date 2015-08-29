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

package com.orientechnologies.orient.spatial.functions;

import com.orientechnologies.orient.spatial.shape.OShapeFactory;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;

import java.util.Map;

/**
 * Created by Enrico Risa on 12/08/15.
 */
public class STContainsFunction extends OSQLFunctionAbstract {

  public static final String NAME    = "st_contains";

  OShapeFactory              factory = OShapeFactory.INSTANCE;

  public STContainsFunction() {
    super(NAME, 2, 2);
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {
    Shape shape = factory.fromDoc((ODocument) iParams[0]);
    Map map = (Map) iParams[1];
    Shape shape1 = factory.fromMapGeoJson((Map) map.get("shape"));
    return shape.relate(shape1) == SpatialRelation.CONTAINS;
  }

  @Override
  public String getSyntax() {
    return null;
  }
}
