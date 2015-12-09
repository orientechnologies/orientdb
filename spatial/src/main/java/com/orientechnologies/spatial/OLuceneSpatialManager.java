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

package com.orientechnologies.spatial;

import com.orientechnologies.orient.spatial.shape.OShapeBuilder;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * Created by Enrico Risa on 06/08/15.
 */
public class OLuceneSpatialManager {

  private OShapeBuilder shapeFactory;

  public OLuceneSpatialManager(OShapeBuilder oShapeBuilder) {
    shapeFactory = oShapeBuilder;
  }

  public void init(ODatabaseDocumentTx db) {

    if (db.getMetadata().getSchema().getClass("OShape") == null) {
      db.getMetadata().getSchema().createAbstractClass("OShape");
      shapeFactory.initClazz(db);
    }
  }
}
