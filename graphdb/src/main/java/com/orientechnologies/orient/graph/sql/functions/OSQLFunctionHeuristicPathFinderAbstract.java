/*
  *
  *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
  *  * For more information: http://orientdb.com
  *
  */
package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.*;


/**
 * Abstract class to find paths between nodes using heuristic .
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public abstract class OSQLFunctionHeuristicPathFinderAbstract extends OSQLFunctionMathAbstract {
    public static final String PARAM_DIRECTION = "direction";
    public static final String PARAM_EDGE_TYPE_NAMES = "edgeTypeNames";
    public static final String PARAM_VERTEX_AXIS_NAMES = "vertexAxisNames";
    public static final String PARAM_PARALLEL = "parallel";
    public static final String PARAM_MAX_DEPTH = "maxDepth";
    public static final String PARAM_HEURISTIC_FORMULA = "heuristicFormula";
    public static final String PARAM_CUSTOM_HEURISTIC_FORMULA = "customHeuristicFormula";
    public static final String PARAM_D_FACTOR = "dFactor";
    public static final String PARAM_TIE_BREAKER = "tieBreaker";
    public static final String PARAM_EMPTY_IF_MAX_DEPTH = "emptyIfMaxDepth";
    protected OrientBaseGraph db;
    protected static Random rnd = new Random();

    protected Boolean paramParallel = false;
    protected Boolean paramTieBreaker = true;
    protected Boolean paramEmptyIfMaxDepth = false;
    protected String[] paramEdgeTypeNames = new String[]{};
    protected String[] paramVertexAxisNames = new String[]{};
    protected OrientVertex paramSourceVertex;
    protected OrientVertex paramDestinationVertex;
    protected HeuristicFormula paramHeuristicFormula = HeuristicFormula.MANHATAN;
    protected Direction paramDirection = Direction.OUT;
    protected long paramMaxDepth = Long.MAX_VALUE;
    protected double paramDFactor = 1.0;
    protected String paramCustomHeuristicFormula = "";

    protected OCommandContext context;
    protected List<OrientVertex> route = new LinkedList<OrientVertex>();
    protected static final float MIN = 0f;

    public OSQLFunctionHeuristicPathFinderAbstract(final String iName, final int iMinParams, final int iMaxParams) {
        super(iName, iMinParams, iMaxParams);
    }

    // Approx Great-circle distance.  Args in degrees, result in kilometers
    // obtains from https://github.com/enderceylan/CS-314--Data-Structures/blob/master/HW10-Graph.java
    public double gcdist(double lata, double longa, double latb, double longb) {
        double midlat, psi, dist;
        midlat = 0.5 * (lata + latb);
        psi = 0.0174532925
                * Math.sqrt(Math.pow(lata - latb, 2)
                + Math.pow((longa - longb)
                * Math.cos(0.0174532925 * midlat), 2));
        dist = 6372.640112 * psi;
        return dist;
    }

    protected boolean isVariableEdgeWeight() {
        return false;
    }

    protected abstract double getDistance(final OrientVertex node, final OrientVertex parent, final OrientVertex target);

    protected abstract double getHeuristicCost(final OrientVertex node, final OrientVertex parent, final OrientVertex target);

    protected LinkedList<OrientVertex> getPath() {
        final LinkedList<OrientVertex> path = new LinkedList<OrientVertex>(route);
        return path;
    }

    protected Set<OrientVertex> getNeighbors(final OrientVertex node) {
        context.incrementVariable("getNeighbors");

        final Set<OrientVertex> neighbors = new HashSet<OrientVertex>();
        if (node != null) {
            for (Vertex v : node.getVertices(paramDirection, paramEdgeTypeNames)) {
                final OrientVertex ov = (OrientVertex) v;
                if (ov != null)
                    neighbors.add(ov);
            }
        }
        return neighbors;
    }


    // obtains from http://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
    protected double getSimpleHeuristicCost(double x, double g, double dFactor) {
        double dx = Math.abs(x - g);
        return dFactor * (dx);
    }

    // obtains from http://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
    protected double getManhatanHeuristicCost(double x, double y, double gx, double gy, double dFactor) {
        double dx = Math.abs(x - gx);
        double dy = Math.abs(y - gy);
        return dFactor * (dx + dy);
    }

    protected double getMaxAxisHeuristicCost(double x, double y, double gx, double gy, double dFactor) {
        double dx = Math.abs(x - gx);
        double dy = Math.abs(y - gy);
        return dFactor * Math.max(dx, dy);
    }

    // obtains from http://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
    protected double getDiagonalHeuristicCost(double x, double y, double gx, double gy, double dFactor) {
        double dx = Math.abs(x - gx);
        double dy = Math.abs(y - gy);
        double h_diagonal = Math.min(dx, dy);
        double h_straight = dx + dy;
        return (dFactor * 2) * h_diagonal + dFactor * (h_straight - 2 * h_diagonal);
    }

    // obtains from http://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
    protected double getEuclideanHeuristicCost(double x, double y, double gx, double gy, double dFactor) {
        double dx = Math.abs(x - gx);
        double dy = Math.abs(y - gy);

        return (dFactor * Math.sqrt(Math.pow(dx, 2) + Math.pow(dy, 2)));
    }

    protected double getEuclideanNoSQRHeuristicCost(double x, double y, double gx, double gy, double dFactor) {
        double dx = Math.abs(x - gx);
        double dy = Math.abs(y - gy);

        return (dFactor * (Math.pow(dx, 2) + Math.pow(dy, 2)));
    }

    protected double getCustomHeuristicCost(final String functionName, final String[] vertextAxisNames, final OrientVertex start, final OrientVertex goal, final OrientVertex current, final OrientVertex parent, final long depth, double dFactor) {

        double heuristic = 0.0;
        OrientGraph ff;

        OFunction func = OrientGraph.getActiveGraph().getRawGraph().getMetadata().getFunctionLibrary().getFunction(functionName);
        Object fValue = func.executeInContext(context, vertextAxisNames, start, goal, current, parent, depth, dFactor);
        if (fValue != null && fValue instanceof Number) {
            heuristic = doubleOrDefault(fValue, heuristic);
        }
        return heuristic;
    }

    // obtains from http://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
    protected double getTieBreakingHeuristicCost(double x, double y, double sx, double sy, double gx, double gy, double heuristic) {
        double dx1 = x - gx;
        double dy1 = y - gy;
        double dx2 = sx - gx;
        double dy2 = sy - gy;
        double cross = Math.abs(dx1 * dy2 - dx2 * dy1);
        heuristic += (cross * 0.0001);
        return heuristic;
    }

    protected double getTieBreakingRandomHeuristicCost(double x, double y, double sx, double sy, double gx, double gy, double heuristic) {
        double dx1 = x - gx;
        double dy1 = y - gy;
        double dx2 = sx - gx;
        double dy2 = sy - gy;
        double cross = Math.abs(dx1 * dy2 - dx2 * dy1) + rnd.nextFloat();
        heuristic += (cross * heuristic);
        return heuristic;
    }

    protected double getManhatanHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
                                              final Map<String, Double> plist, final Map<String, Double> glist, long depth, double dFactor) {
        Double heuristic = 0.0;
        double res = 0.0;
        for (String str : axisNames) {
            res += Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0));
        }
        heuristic = dFactor * res;
        return heuristic;
    }

    protected double getMaxAxisHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
                                             final Map<String, Double> plist, final Map<String, Double> glist, long depth, double dFactor) {
        Double heuristic = 0.0;
        double res = 0.0;
        for (String str : axisNames) {
            res = Math.max(Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0)), res);
        }
        heuristic = dFactor * res;
        return heuristic;
    }

    protected double getDiagonalHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
                                              final Map<String, Double> plist, final Map<String, Double> glist, long depth, double dFactor) {

        Double heuristic = 0.0;
        double h_diagonal = 0.0;
        double h_straight = 0.0;
        for (String str : axisNames) {
            h_diagonal = Math.min(Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0)), h_diagonal);
            h_straight += Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0));
        }
        heuristic = (dFactor * 2) * h_diagonal + dFactor * (h_straight - 2 * h_diagonal);
        return heuristic;
    }

    protected double getEuclideanHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
                                               final Map<String, Double> plist, final Map<String, Double> glist, long depth, double dFactor) {
        Double heuristic = 0.0;
        double res = 0.0;
        for (String str : axisNames) {
            res += Math.pow(Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0)), 2);
        }
        heuristic = Math.sqrt(res);
        return heuristic;
    }

    protected double getEuclideanNoSQRHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
                                                    final Map<String, Double> plist, final Map<String, Double> glist, long depth, double dFactor) {
        Double heuristic = 0.0;
        double res = 0.0;
        for (String str : axisNames) {
            res += Math.pow(Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0)), 2);
        }
        heuristic = dFactor * res;
        return heuristic;
    }

    protected double getTieBreakingHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
                                                 final Map<String, Double> plist, final Map<String, Double> glist, long depth, double heuristic) {

        double res = 0.0;
        for (String str : axisNames) {
            res += Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0));
        }
        double cross = res;
        heuristic += (cross * 0.0001);
        return heuristic;
    }

    protected String[] stringArray(Object fromObject) {
        if (fromObject == null) {
            return new String[]{};
        }
        if (fromObject instanceof String) {
            String[] arr = fromObject.toString().replace("},{", " ,").split(",");
            return (arr);
        }
        if (fromObject instanceof Object) {
            return ((String[]) fromObject);
        }

        return new String[]{};
    }

    protected Boolean booleanOrDefault(Object fromObject, boolean defaultValue) {
        if (fromObject == null) {
            return defaultValue;
        }
        if (fromObject instanceof Boolean) {
            return (Boolean) fromObject;
        }
        if (fromObject instanceof String) {
            try {
                return Boolean.parseBoolean((String) fromObject);
            } catch (Exception e) {
            }
        }
        return defaultValue;
    }

    protected String stringOrDefault(Object fromObject, String defaultValue) {
        if (fromObject == null) {
            return defaultValue;
        }
        return (String) fromObject;
    }

    protected Integer integerOrDefault(Object fromObject, int defaultValue) {
        if (fromObject == null) {
            return defaultValue;
        }
        if (fromObject instanceof Number) {
            return ((Number) fromObject).intValue();
        }
        if (fromObject instanceof String) {
            try {
                return Integer.parseInt((String) fromObject);
            } catch (Exception e) {
            }
        }
        return defaultValue;
    }

    protected Long longOrDefault(Object fromObject, long defaultValue) {
        if (fromObject == null) {
            return defaultValue;
        }
        if (fromObject instanceof Number) {
            return ((Number) fromObject).longValue();
        }
        if (fromObject instanceof String) {
            try {
                return Long.parseLong((String) fromObject);
            } catch (Exception e) {
            }
        }
        return defaultValue;
    }

    protected Double doubleOrDefault(Object fromObject, double defaultValue) {
        if (fromObject == null) {
            return defaultValue;
        }
        if (fromObject instanceof Number) {
            return ((Number) fromObject).doubleValue();
        }
        if (fromObject instanceof String) {
            try {
                return Double.parseDouble((String) fromObject);
            } catch (Exception e) {
            }
        }
        return defaultValue;
    }


}