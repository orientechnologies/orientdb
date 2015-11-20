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

package com.orientechnologies.lucene.factory;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.spatial.shape.OShapeBuilder;
import com.spatial4j.core.context.SpatialContext;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.bbox.BBoxStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;

/**
 * Created by Enrico Risa on 02/10/15.
 */
public class OSpatialStrategyFactory {

  private OShapeBuilder factory;

  public OSpatialStrategyFactory(OShapeBuilder factory) {
    this.factory = factory;
  }

  public SpatialStrategy createStrategy(SpatialContext ctx, ODatabaseDocumentInternal db, OIndexDefinition indexDefinition,
      ODocument metadata) {

    SpatialStrategy strategy;

    OClass aClass = db.getMetadata().getSchema().getClass(indexDefinition.getClassName());

    OProperty property = aClass.getProperty(indexDefinition.getFields().get(0));

    OClass linkedClass = property.getLinkedClass();

    if ("OPoint".equalsIgnoreCase(linkedClass.getName())) {
      RecursivePrefixTreeStrategy recursivePrefixTreeStrategy = new RecursivePrefixTreeStrategy(new GeohashPrefixTree(ctx, 11),
          "location");
      recursivePrefixTreeStrategy.setDistErrPct(0);
      strategy = recursivePrefixTreeStrategy;

    } else {
      strategy = new BBoxStrategy(ctx, "location");
    }
    return strategy;
  }

}
