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

import com.orientechnologies.spatial.shape.OShapeFactory;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Created by Enrico Risa on 06/08/15.
 */
public class OToGeoJsonFunction extends OSQLFunctionAbstract {

  public static final String NAME    = "asgeojson";

  OShapeFactory factory = OShapeFactory.INSTANCE;

  public OToGeoJsonFunction() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {
    return factory.asText((ODocument) iParams[0]);
  }

  @Override
  public String getSyntax() {
    return "asGeoJson(<doc>)";
  }
}
