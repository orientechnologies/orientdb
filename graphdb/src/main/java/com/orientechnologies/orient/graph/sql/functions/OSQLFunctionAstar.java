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
package com.orientechnologies.orient.graph.sql.functions;

import java.awt.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.List;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.resource.OPartitionedObjectPool;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.sun.javafx.scene.layout.region.Margins;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * A*'s algorithm describes how to find the cheapest path from one node to another node in a directed weighted graph with husrestic function.
 * <p>
 * The first parameter is source record. The second parameter is destination record. The third parameter is a name of property that
 * represents 'weight' and fourth represnts the map of options.
 * <p>
 * If property is not defined in edge or is null, distance between vertexes are 0.
 *
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public class OSQLFunctionAstar extends OSQLFunctionHeuristicPathFinderAbstract {
    public static final String NAME = "astar";

    private String paramWeightFieldName = "weight";
    private long currentDepth = 0;
    protected Set<OrientVertex> closedSet = new HashSet<OrientVertex>();
    protected Map<OrientVertex, OrientVertex> cameFrom = new HashMap<OrientVertex, OrientVertex>();

    protected Map<OrientVertex, Double> gScore = new HashMap<OrientVertex, Double>();
    protected Map<OrientVertex, Double> fScore = new HashMap<OrientVertex, Double>();
    protected PriorityQueue<OrientVertex> open = new PriorityQueue<OrientVertex>(1, new Comparator<OrientVertex>() {

        public int compare(OrientVertex nodeA, OrientVertex nodeB) {
            return Double.compare(fScore.get(nodeA), fScore.get(nodeB));
        }
    });

    public OSQLFunctionAstar() {
        super(NAME, 3, 4);
    }

    public LinkedList<OrientVertex> execute(final Object iThis, final OIdentifiable iCurrentRecord, final Object iCurrentResult,
                                            final Object[] iParams, final OCommandContext iContext) {
        context = iContext;
        final OSQLFunctionAstar context = this;
        return OGraphCommandExecutorSQLFactory
                .runWithAnyGraph(new OGraphCommandExecutorSQLFactory.GraphCallBack<LinkedList<OrientVertex>>() {
                    @Override
                    public LinkedList<OrientVertex> call(final OrientBaseGraph graph) {

                        final ORecord record = iCurrentRecord != null ? iCurrentRecord.getRecord() : null;

                        Object source = iParams[0];
                        if (OMultiValue.isMultiValue(source)) {
                            if (OMultiValue.getSize(source) > 1)
                                throw new IllegalArgumentException("Only one sourceVertex is allowed");
                            source = OMultiValue.getFirstValue(source);
                        }
                        paramSourceVertex = graph.getVertex(OSQLHelper.getValue(source, record, iContext));

                        Object dest = iParams[1];
                        if (OMultiValue.isMultiValue(dest)) {
                            if (OMultiValue.getSize(dest) > 1)
                                throw new IllegalArgumentException("Only one destinationVertex is allowed");
                            dest = OMultiValue.getFirstValue(dest);
                        }
                        paramDestinationVertex = graph.getVertex(OSQLHelper.getValue(dest, record, iContext));

                        paramWeightFieldName = OIOUtils.getStringContent(iParams[2]);


                        if (iParams.length > 3) {
                            bindAdditionalParams(iParams[3], context);
                        }
                        iContext.setVariable("getNeighbors", 0);
                        return internalExecute(iContext);
                    }
                });
    }

    private LinkedList<OrientVertex> internalExecute(final OCommandContext iContext) {

        OrientVertex start = paramSourceVertex;
        OrientVertex goal = paramDestinationVertex;

        open.add(start);

        // The cost of going from start to start is zero.
        gScore.put(start, 0.0);
        // For the first node, that value is completely heuristic.
        fScore.put(start, getHeuristicCost(start, null, goal));

        while (!open.isEmpty()) {
            OrientVertex current = open.poll();

            // if start and goal vertex is equal so return current path from  cameFrom hash map
            if (current.getIdentity().equals(goal.getIdentity()) || currentDepth >= paramMaxDepth) {

                while (current != null) {
                    route.add(0, current);
                    current = cameFrom.get(current);
                }
                return getPath();
            }

            closedSet.add(current);
            for (OrientVertex neighbor : getNeighbors(current)) {

                // Ignore the neighbor which is already evaluated.
                if (closedSet.contains(neighbor)) {
                    continue;
                }
                // The distance from start to a neighbor
                double tentative_gScore = gScore.get(current) + getDistance(current, current, neighbor);
                boolean contains = open.contains(neighbor);

                if (!contains || tentative_gScore < gScore.get(neighbor)) {
                    gScore.put(neighbor, tentative_gScore);
                    fScore.put(neighbor, tentative_gScore + getHeuristicCost(neighbor, current, goal));

                    if (contains) {
                        open.remove(neighbor);
                    }
                    open.offer(neighbor);
                    cameFrom.put(neighbor, current);
                }
            }

            // Increment Depth Level
            currentDepth++;

        }

        return getPath();
    }

    private void bindAdditionalParams(Object additionalParams, OSQLFunctionAstar ctx) {
        if (additionalParams == null) {
            return;
        }
        Map<String, Object> mapParams = null;
        if (additionalParams instanceof Map) {
            mapParams = (Map) additionalParams;
        } else if (additionalParams instanceof OIdentifiable) {
            mapParams = ((ODocument) ((OIdentifiable) additionalParams).getRecord()).toMap();
        }
        if (mapParams != null) {
            ctx.paramEdgeTypeNames = stringArray(mapParams.get(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES));
            ctx.paramVertexAxisNames = stringArray(mapParams.get(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES));
            if(mapParams.get(OSQLFunctionAstar.PARAM_DIRECTION) != null){
                if (mapParams.get(OSQLFunctionAstar.PARAM_DIRECTION) instanceof  String){
                    ctx.paramDirection = Direction.valueOf(stringOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_DIRECTION), "OUT").toUpperCase());
                }
                else{
                    ctx.paramDirection = (Direction) mapParams.get(OSQLFunctionAstar.PARAM_DIRECTION);
                }
            }


            ctx.paramParallel = booleanOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_PARALLEL), false);
            ctx.paramMaxDepth = longOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_MAX_DEPTH), ctx.paramMaxDepth);
            ctx.paramTieBreaker = booleanOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_TIE_BREAKER), ctx.paramTieBreaker);
            ctx.paramDFactor = doubleOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_D_FACTOR), ctx.paramDFactor);
            if(mapParams.get(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA) !=null){
                if (mapParams.get(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA) instanceof  String){
                    ctx.paramHeuristicFormula = HeuristicFormula.valueOf(stringOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA), "MANHATAN").toUpperCase());
                }
                else{
                    ctx.paramHeuristicFormula = (HeuristicFormula)mapParams.get(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA);
                }
            }


            ctx.paramCustomHeuristicFormula = stringOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_CUSTOM_HEURISTIC_FORMULA), "");
        }
    }


    public String getSyntax() {
        return "astar(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<options>]) \n // options  : {direction:\"OUT\",edgeTypeNames:[] , vertexAxisNames:[] , parallel : false , tieBreaker:true,maxDepth:99999,dFactor:1.0,customHeuristicFormula:'custom_Function_Name_here'  }";
    }

    @Override
    public Object getResult() {
        return getPath();
    }

    @Override
    protected double getDistance(final OrientVertex node, final OrientVertex parent, final OrientVertex target) {
        final Iterator<Edge> edges = node.getEdges(target, paramDirection).iterator();
        if (edges.hasNext()) {
            final Edge e = edges.next();
            if (e != null) {
                final Object fieldValue = e.getProperty(paramWeightFieldName);
                if (fieldValue != null)
                    if (fieldValue instanceof Float)
                        return (Float) fieldValue;
                    else if (fieldValue instanceof Number)
                        return ((Number) fieldValue).doubleValue();
            }
        }
        return MIN;
    }

    @Override
    public boolean aggregateResults() {
        return false;
    }

    @Override
    protected double getHeuristicCost(final OrientVertex node, OrientVertex parent, final OrientVertex target) {
        double hresult = 0.0;

        if (paramVertexAxisNames.length == 0) {
            return hresult;
        } else if (paramVertexAxisNames.length == 1) {
            double n = doubleOrDefault(node.getProperty(paramVertexAxisNames[0]),0.0) ;
            double g = doubleOrDefault(target.getProperty(paramVertexAxisNames[0]),0.0);
            hresult = getSimpleHeuristicCost(n, g, paramDFactor);
        } else if (paramVertexAxisNames.length == 2) {
            if (parent == null) parent = node;
            double sx = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[0]),0);
            double sy = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[1]),0);
            double nx = doubleOrDefault(node.getProperty(paramVertexAxisNames[0]),0);
            double ny = doubleOrDefault(node.getProperty(paramVertexAxisNames[1]),0);
            double px = doubleOrDefault(parent.getProperty(paramVertexAxisNames[0]),0);
            double py = doubleOrDefault(parent.getProperty(paramVertexAxisNames[1]),0);
            double gx = doubleOrDefault(target.getProperty(paramVertexAxisNames[0]),0);
            double gy = doubleOrDefault(target.getProperty(paramVertexAxisNames[1]),0);

            switch (paramHeuristicFormula) {
                case MANHATAN:
                    hresult = getManhatanHeuristicCost(nx, ny, gx, gy, paramDFactor);
                    break;
                case MAXAXIS:
                    hresult = getMaxAxisHeuristicCost(nx, ny, gx, gy, paramDFactor);
                    break;
                case DIAGONAL:
                    hresult = getDiagonalHeuristicCost(nx, ny, gx, gy, paramDFactor);
                    break;
                case EUCLIDEAN:
                    hresult = getEuclideanHeuristicCost(nx, ny, gx, gy, paramDFactor);
                    break;
                case EUCLIDEANNOSQR:
                    hresult = getEuclideanNoSQRHeuristicCost(nx, ny, gx, gy, paramDFactor);
                    break;
                case CUSTOM:
                    hresult = getCustomHeuristicCost(paramCustomHeuristicFormula,paramVertexAxisNames,paramSourceVertex,paramDestinationVertex, node, parent,currentDepth, paramDFactor);
                    break;
            }
            if (paramTieBreaker) {
                hresult = getTieBreakingHeuristicCost(px, py, sx, sy, gx, gy, hresult);
            }

        } else {
            Map<String,Double> sList = new HashMap<String,Double>();
            Map<String,Double> cList = new HashMap<String,Double>();
            Map<String,Double> pList = new HashMap<String,Double>();
            Map<String,Double> gList = new HashMap<String,Double>();
            parent = parent == null ? node : parent;
            for (int i = 0; i < paramVertexAxisNames.length; i++) {
                Double s = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[i]),0);
                Double c = doubleOrDefault(node.getProperty(paramVertexAxisNames[i]),0);
                Double g = doubleOrDefault(target.getProperty(paramVertexAxisNames[i]),0);
                Double p = doubleOrDefault(parent.getProperty(paramVertexAxisNames[i]),0);
                if (s != null)
                    sList.put(paramVertexAxisNames[i],s);
                if (c != null)
                    cList.put(paramVertexAxisNames[i],s);
                if (g != null)
                    gList.put(paramVertexAxisNames[i],g);
                if (p != null)
                    pList.put(paramVertexAxisNames[i],p);
            }
            switch (paramHeuristicFormula) {
                case MANHATAN:
                    hresult = getManhatanHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth,paramDFactor);
                    break;
                case MAXAXIS:
                    hresult = getMaxAxisHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth,paramDFactor);
                    break;
                case DIAGONAL:
                    hresult = getDiagonalHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth,paramDFactor);
                    break;
                case EUCLIDEAN:
                    hresult = getEuclideanHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth,paramDFactor);
                    break;
                case EUCLIDEANNOSQR:
                    hresult = getEuclideanNoSQRHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth,paramDFactor);
                    break;
                case CUSTOM:
                    hresult = getCustomHeuristicCost(paramCustomHeuristicFormula,paramVertexAxisNames,paramSourceVertex,paramDestinationVertex, node, parent,currentDepth, paramDFactor);
                    break;
            }
            if (paramTieBreaker) {
                hresult = getTieBreakingHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth, hresult);
            }



        }


        return hresult;

    }


    @Override
    protected boolean isVariableEdgeWeight() {
        return true;
    }


}
