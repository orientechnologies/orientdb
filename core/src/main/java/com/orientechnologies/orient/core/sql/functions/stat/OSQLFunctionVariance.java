/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql.functions.stat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Compute the variance estimation for a given field.
 * 
 * This class uses the Weldford's algorithm (presented in Donald Knuth's Art of Computer Programming) to avoid multiple distribution
 * values' passes. When executed in distributed mode it uses the Chan at al. pairwise variance algorithm to merge the results.
 * 
 * <p>
 * <b>References</b>
 * </p>
 *
 * <ul>
 * 
 * <li>Cook, John D. <a href="http://www.johndcook.com/standard_deviation.html">Accurately computing running variance</a>.</li>
 *
 * <li>Knuth, Donald E. (1998) <i>The Art of Computer Programming, Volume 2: Seminumerical Algorithms, 3rd Edition.</i></li>
 *
 * <li>Welford, B. P. (1962) Note on a method for calculating corrected sums of squares and products. <i>Technometrics</i></li>
 * 
 * <li>Chan, Tony F.; Golub, Gene H.; LeVeque, Randall J. (1979), <a
 * href="http://cpsc.yale.edu/sites/default/files/files/tr222.pdf">Parallel Algorithm</a>.</li>
 *
 * </ul>
 * 
 * @author Fabrizio Fortino
 */
public class OSQLFunctionVariance extends OSQLFunctionAbstract {

  public static final String NAME = "variance";

  private long               n;
  private double             mean;
  private double             m2;

  public OSQLFunctionVariance() {
    super(NAME, 1, 1);
  }

  public OSQLFunctionVariance(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMaxParams, iMaxParams);
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {
    if (iParams[0] instanceof Number) {
      addValue((Number) iParams[0]);
    } else if (OMultiValue.isMultiValue(iParams[0])) {
      for (Object n : OMultiValue.getMultiValueIterable(iParams[0])) {
        addValue((Number) n);
      }
    }
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (returnDistributedResult()) {
      final Map<String, Object> doc = new HashMap<String, Object>();
      doc.put("n", n);
      doc.put("mean", mean);
      doc.put("var", this.evaluate());
      return doc;
    } else {
      return this.evaluate();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    if (returnDistributedResult()) {
      long dN = 0;
      double dMean = 0;
      Double var = null;
      for (Object iParameter : resultsToMerge) {
        final Map<String, Object> item = (Map<String, Object>) iParameter;
        if (dN == 0) { // first element
          dN = (Long) item.get("n");
          dMean = (Double) item.get("mean");
          var = (Double) item.get("var");
        } else {
          long rhsN = (Long) item.get("n");
          double rhsMean = (Double) item.get("mean");
          double rhsVar = (Double) item.get("var");

          long totalN = dN + rhsN;
          double totalMean = ((dMean * dN) + (rhsMean * rhsN)) / totalN;

          var = (((dN * var) + (rhsN * rhsVar)) / totalN) + ((dN * rhsN) * Math.pow((rhsMean - dMean) / totalN, 2));
          dN = totalN;
          dMean = totalMean;
        }

      }
      return var;
    }

    return resultsToMerge.get(0);

  }

  @Override
  public String getSyntax() {
    return NAME + "(<field>)";
  }

  private void addValue(Number value) {
    if (value != null) {
      ++n;
      double doubleValue = value.doubleValue();
      double nextM = mean + (doubleValue - mean) / n;
      m2 += (doubleValue - mean) * (doubleValue - nextM);
      mean = nextM;
    }
  }

  private Double evaluate() {
    return n > 1 ? m2 / n : null;
  }

}
