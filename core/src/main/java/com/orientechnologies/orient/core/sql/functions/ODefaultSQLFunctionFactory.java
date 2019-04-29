/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions;

import com.orientechnologies.orient.core.sql.functions.coll.*;
import com.orientechnologies.orient.core.sql.functions.geo.OSQLFunctionDistance;
import com.orientechnologies.orient.core.sql.functions.graph.*;
import com.orientechnologies.orient.core.sql.functions.math.*;
import com.orientechnologies.orient.core.sql.functions.misc.*;
import com.orientechnologies.orient.core.sql.functions.sequence.OSQLFunctionSequence;
import com.orientechnologies.orient.core.sql.functions.stat.*;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionConcat;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionFormat;

/**
 * Default set of SQL function.
 *
 * @author Johann Sorel (Geomatys)
 */
public final class ODefaultSQLFunctionFactory extends OSQLFunctionFactoryTemplate {
  public ODefaultSQLFunctionFactory() {
    // MISC FUNCTIONS
    register(OSQLFunctionAverage.NAME, OSQLFunctionAverage.class);
    register(OSQLFunctionCoalesce.NAME, new OSQLFunctionCoalesce());
    register(OSQLFunctionCount.NAME, OSQLFunctionCount.class);
    register(OSQLFunctionDate.NAME, OSQLFunctionDate.class);
    register(OSQLFunctionDecode.NAME, new OSQLFunctionDecode());
    register(OSQLFunctionDifference.NAME, OSQLFunctionDifference.class);
    register(OSQLFunctionSymmetricDifference.NAME, OSQLFunctionSymmetricDifference.class);
    register(OSQLFunctionDistance.NAME, new OSQLFunctionDistance());
    register(OSQLFunctionDistinct.NAME, OSQLFunctionDistinct.class);
    register(OSQLFunctionDocument.NAME, OSQLFunctionDocument.class);
    register(OSQLFunctionEncode.NAME, new OSQLFunctionEncode());
    register(OSQLFunctionEval.NAME, OSQLFunctionEval.class);
    register(OSQLFunctionFirst.NAME, new OSQLFunctionFirst());
    register(OSQLFunctionFormat.NAME, new OSQLFunctionFormat());
    register(OSQLFunctionTraversedEdge.NAME, OSQLFunctionTraversedEdge.class);
    register(OSQLFunctionTraversedElement.NAME, OSQLFunctionTraversedElement.class);
    register(OSQLFunctionTraversedVertex.NAME, OSQLFunctionTraversedVertex.class);
    register(OSQLFunctionIf.NAME, new OSQLFunctionIf());
    register(OSQLFunctionIfNull.NAME, new OSQLFunctionIfNull());
    register(OSQLFunctionIntersect.NAME, OSQLFunctionIntersect.class);
    register(OSQLFunctionLast.NAME, new OSQLFunctionLast());
    register(OSQLFunctionList.NAME, OSQLFunctionList.class);
    register(OSQLFunctionMap.NAME, OSQLFunctionMap.class);
    register(OSQLFunctionMax.NAME, OSQLFunctionMax.class);
    register(OSQLFunctionMin.NAME, OSQLFunctionMin.class);
    register(OSQLFunctionSet.NAME, OSQLFunctionSet.class);
    register(OSQLFunctionSysdate.NAME, OSQLFunctionSysdate.class);
    register(OSQLFunctionSum.NAME, OSQLFunctionSum.class);
    register(OSQLFunctionUnionAll.NAME, OSQLFunctionUnionAll.class);
    register(OSQLFunctionMode.NAME, OSQLFunctionMode.class);
    register(OSQLFunctionPercentile.NAME, OSQLFunctionPercentile.class);
    register(OSQLFunctionMedian.NAME, OSQLFunctionMedian.class);
    register(OSQLFunctionVariance.NAME, OSQLFunctionVariance.class);
    register(OSQLFunctionStandardDeviation.NAME, OSQLFunctionStandardDeviation.class);
    register(OSQLFunctionUUID.NAME, OSQLFunctionUUID.class);
    register(OSQLFunctionConcat.NAME, OSQLFunctionConcat.class);
    register(OSQLFunctionDecimal.NAME, OSQLFunctionDecimal.class);
    register(OSQLFunctionSequence.NAME, OSQLFunctionSequence.class);
    register(OSQLFunctionAbsoluteValue.NAME, OSQLFunctionAbsoluteValue.class);
    register(OSQLFunctionIndexKeySize.NAME, OSQLFunctionIndexKeySize.class);
    register(OSQLFunctionStrcmpci.NAME, OSQLFunctionStrcmpci.class);
    register(OSQLFunctionThrowCME.NAME, OSQLFunctionThrowCME.class);
    //graph
    register(OSQLFunctionOut.NAME, OSQLFunctionOut.class);
    register(OSQLFunctionIn.NAME, OSQLFunctionIn.class);
    register(OSQLFunctionBoth.NAME, OSQLFunctionBoth.class);
    register(OSQLFunctionOutE.NAME, OSQLFunctionOutE.class);
    register(OSQLFunctionOutV.NAME, OSQLFunctionOutV.class);
    register(OSQLFunctionInE.NAME, OSQLFunctionInE.class);
    register(OSQLFunctionInV.NAME, OSQLFunctionInV.class);
    register(OSQLFunctionBothE.NAME, OSQLFunctionBothE.class);
    register(OSQLFunctionBothV.NAME, OSQLFunctionBothV.class);
    register(OSQLFunctionShortestPath.NAME, OSQLFunctionShortestPath.class);
    register(OSQLFunctionDijkstra.NAME, OSQLFunctionDijkstra.class);
    register(OSQLFunctionAstar.NAME, OSQLFunctionAstar.class);

  }

}
