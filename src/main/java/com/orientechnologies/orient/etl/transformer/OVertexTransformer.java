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
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OVertexTransformer extends OAbstractTransformer {
  protected String        vertexClass;
  private OrientBaseGraph graph;
  private boolean         skipDuplicates = false;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{class:{optional:true,description:'Vertex class name to assign. Default is V'}}"
        + ",skipDuplicates:{optional:true,description:'Vertices with duplicate keys are skipped', default:false}" + "]"
        + ",input:['OrientVertex','ODocument'],output:'OrientVertex'}");
  }

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, final OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    if (iConfiguration.containsField("class"))
      vertexClass = (String) resolve(iConfiguration.field("class"));
    if (iConfiguration.containsField("skipDuplicates"))
      skipDuplicates = (Boolean) resolve(iConfiguration.field("skipDuplicates"));
  }

  @Override
  public String getName() {
    return "vertex";
  }

  @Override
  public Object executeTransform(final Object input) {
    if (graph == null)
      graph = pipeline.getGraphDatabase();

    if (graph == null)
      throw new OETLProcessHaltedException("Graph instance not found. Assure you have configured it in the Loader");

    vertexClass = (String) resolve(vertexClass);
    if (vertexClass != null) {
      final OClass cls = graph.getVertexType(vertexClass);
      if (cls == null)
        try {
          graph.createVertexType(vertexClass);
        } catch (OSchemaException e) {
        }
    }

    final OrientVertex v = graph.getVertex(input);
    if (v == null)
      return null;

    if (vertexClass != null && !vertexClass.equals(v.getRecord().getClassName()))
      try {
        v.setProperty("@class", vertexClass);
      } catch (ORecordDuplicatedException e) {
        if (skipDuplicates) {
          return null;
        } else {
          throw e;
        }
      }
    return v;
  }
}
