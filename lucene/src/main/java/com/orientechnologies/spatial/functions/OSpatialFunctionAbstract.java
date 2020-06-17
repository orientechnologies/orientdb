/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.functions;

import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.spatial.shape.OShapeFactory;
import org.locationtech.spatial4j.shape.Shape;

/** Created by Enrico Risa on 22/07/16. */
public abstract class OSpatialFunctionAbstract extends OSQLFunctionAbstract {

  protected OShapeFactory factory = OShapeFactory.INSTANCE;

  public OSpatialFunctionAbstract(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  boolean containsNull(Object[] params) {
    for (Object param : params) {
      if (param == null) return true;
    }

    return false;
  }

  protected Shape toShape(Object param) {
    final Object singleItem = getSingleItem(param);
    if (singleItem != null) {
      final Object singleProp = getSingleProperty(singleItem, false);
      if (singleProp != null) {
        return factory.fromObject(singleProp);
      }
    }
    return null;
  }
}
