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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class OVertexTransformer extends OAbstractTransformer {
  private String  vertexClass;
  private boolean skipDuplicates = false;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{class:{optional:true,description:'Vertex class name to assign. Default is " + OrientVertexType.CLASS_NAME + "'}}"
        + ",skipDuplicates:{optional:true,description:'Vertices with duplicate keys are skipped', default:false}" + "]"
        + ",input:['OrientVertex','ODocument'],output:'OrientVertex'}");
  }

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, final OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    if (iConfiguration.containsField("class"))
      vertexClass = (String) resolve(iConfiguration.<String>field("class"));
    if (iConfiguration.containsField("skipDuplicates"))
      skipDuplicates = (Boolean) resolve(iConfiguration.field("skipDuplicates"));
  }

  @Override
  public void begin() {
    if (vertexClass != null) {
      final OClass cls = pipeline.getGraphDatabase().getVertexType(vertexClass);
      if (cls == null)
        pipeline.getGraphDatabase().createVertexType(vertexClass);
    }
  }

  @Override
  public String getName() {
    return "vertex";
  }

  @Override
  public Object executeTransform(final Object input) {

    final OrientVertex v = pipeline.getGraphDatabase().getVertex(input);
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
