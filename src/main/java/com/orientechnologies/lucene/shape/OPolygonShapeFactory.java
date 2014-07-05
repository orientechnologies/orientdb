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

package com.orientechnologies.lucene.shape;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;

import java.text.ParseException;

/**
 * Created by enricorisa on 24/04/14.
 */
public class OPolygonShapeFactory implements OShapeFactory {
  @Override
  public Shape makeShape(OCompositeKey key, SpatialContext ctx) {

    SpatialContext ctx1 = JtsSpatialContext.GEO;
    String value = key.getKeys().get(0).toString();

    try {
      return ctx1.getWktShapeParser().parse(value);
    } catch (ParseException e) {
      OLogManager.instance().error(this, "Error on making shape", e);
    }
    return null;
  }

  @Override
  public boolean canHandle(OCompositeKey key) {
    return false;
  }
}
