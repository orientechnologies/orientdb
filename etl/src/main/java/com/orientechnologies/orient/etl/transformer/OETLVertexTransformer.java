/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class OETLVertexTransformer extends OETLAbstractTransformer {
  private String vertexClass;
  private String clusterName;
  private boolean skipDuplicates = false;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{class:{optional:true,description:'Vertex class name to assign. Default is " + OrientVertexType.CLASS_NAME + "'}}"
        + ",skipDuplicates:{optional:true,description:'Vertices with duplicate keys are skipped', default:false}" + "]"
        + ",input:['OrientVertex','ODocument'],output:'OrientVertex'}");
  }

  @Override
  public void configure(final ODocument iConfiguration, final OCommandContext iContext) {
    super.configure(iConfiguration, iContext);
    clusterName = iConfiguration.field("cluster");

    if (iConfiguration.containsField("class"))
      vertexClass = (String) resolve(iConfiguration.field("class"));
    if (iConfiguration.containsField("skipDuplicates"))
      skipDuplicates = (Boolean) resolve(iConfiguration.field("skipDuplicates"));
  }

  @Override
  public void begin() {
    if (vertexClass != null) {
      OrientBaseGraph graph = databaseProvider.getGraphDatabase();
      final OClass cls = graph.getVertexType(vertexClass);
      if (cls == null)
        graph.createVertexType(vertexClass);
    }
  }

  @Override
  public String getName() {
    return "vertex";
  }

  @Override
  public Object executeTransform(final Object input) {

    OrientBaseGraph graph = databaseProvider.getGraphDatabase();
    final OrientVertex v = graph.getVertex(input);

    if (v != null && vertexClass != null && !vertexClass.equals(v.getRecord().getClassName())) {
      v.getRecord().setClassName(vertexClass);
    }
    return v;
  }
}
