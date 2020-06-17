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

import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactoryTemplate;

public class OSpatialFunctionsFactory extends OSQLFunctionFactoryTemplate {

  public OSpatialFunctionsFactory() {
    register(new OSTGeomFromTextFunction());
    register(new OSTAsTextFunction());
    register(new OSTWithinFunction());
    register(new OSTDWithinFunction());
    register(new OSTEqualsFunction());
    register(new OSTAsBinaryFunction());
    register(new OSTEnvelopFunction());
    register(new OSTBufferFunction());
    register(new OSTDistanceFunction());
    register(new OSTDistanceSphereFunction());
    register(new OSTDisjointFunction());
    register(new OSTIntersectsFunction());
    register(new OSTContainsFunction());
    register(new OSTSrid());
    register(new OSTAsGeoJSONFunction());
    register(new OSTGeomFromGeoJSONFunction());
  }
}
