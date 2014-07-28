/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OVertexTransformer extends OAbstractTransformer {
  protected String        vertexClass;
  private OrientBaseGraph graph;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[{class:{optional:true,description:'Vertex class name to assign. Default is V'}}]"
        + ",input:['OrientVertex','ODocument'],output:'OrientVertex'}");
  }

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, final OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    graph = processor.getGraphDatabase();
    vertexClass = iConfiguration.field("class");
  }

  @Override
  public String getName() {
    return "vertex";
  }

  @Override
  public Object executeTransform(final Object input) {
    vertexClass = (String) resolve(vertexClass);
    if (vertexClass != null) {
      final OClass cls = graph.getVertexType(vertexClass);
      if (cls == null)
        graph.createVertexType(vertexClass);
    }

    final OrientVertex v = graph.getVertex(input);
    if (vertexClass != null)
      v.setProperty("@class", vertexClass);
    return v;
  }
}
