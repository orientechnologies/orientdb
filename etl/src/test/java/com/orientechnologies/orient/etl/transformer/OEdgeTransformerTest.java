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

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.etl.OETLBaseTest;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Tests ETL Field Transformer.
 *
 * @author Luca Garulli
 */
public class OEdgeTransformerTest extends OETLBaseTest {

  @Override
  public void setUp() {
    super.setUp();
    final OrientVertexType v1 = graph.createVertexType("V1");
    final OrientVertexType v2 = graph.createVertexType("V2");

    final OrientEdgeType edgeType = graph.createEdgeType("Friend");
    edgeType.createProperty("in", OType.LINK, v2);
    edgeType.createProperty("out", OType.LINK, v1);

    // ASSURE NOT DUPLICATES
    edgeType.createIndex("out_in", OClass.INDEX_TYPE.UNIQUE, "in", "out");

    graph.addVertex("class:V2").setProperty("name", "Luca");
    graph.commit();
  }

  @Test
  public void testNotLightweightEdge() {
    process("{source: { content: { value: 'name,surname,friend\nJay,Miner,Luca' } }, extractor : { csv: {} },"
        + " transformers: [{vertex: {class:'V1'}}, {edge:{class:'Friend',joinFieldName:'friend',lookup:'V2.name'}},"
        + "], loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph', useLightweightEdges:false } } }");

    assertEquals(1, graph.countVertices("V1"));
    assertEquals(1, graph.countVertices("V2"));
    assertEquals(1, graph.countEdges("Friend"));
  }

  @Test
  public void testLookupMultipleValues() {
    graph.addVertex("class:V2").setProperty("name", "Luca");
    graph.commit();

    process("{source: { content: { value: 'name,surname,friend\nJay,Miner,Luca' } }, extractor : { csv: {} },"
        + " transformers: [{vertex: {class:'V1'}}, {edge:{class:'Friend',joinFieldName:'friend',lookup:'V2.name'}},"
        + "], loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph', useLightweightEdges:false } } }");

    assertEquals(1, graph.countVertices("V1"));
    assertEquals(2, graph.countVertices("V2"));
    assertEquals(2, graph.countEdges("Friend"));
  }

  @Test
  public void testEdgeWithProperties() {
    process(
        "{source: { content: { value: 'id,name,surname,friendSince,friendId,friendName,friendSurname\n0,Jay,Miner,1996,1,Luca,Garulli' } }, extractor : { csv: {} },"
            + " transformers: [ {vertex: {class:'V1'}}, "
            + "{edge:{unresolvedLinkAction:'CREATE',class:'Friend',joinFieldName:'friendId',lookup:'V2.fid',targetVertexFields:{name:'${input.friendName}',surname:'${input.friendSurname}'},edgeFields:{since:'${input.friendSince}'}}},"
            + "{field:{fieldNames:['friendSince','friendId','friendName','friendSurname'],operation:'remove'}}"
            + "], loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph', useLightweightEdges:false } } }");

    assertEquals(1, graph.countVertices("V1"));
    assertEquals(2, graph.countVertices("V2"));
    assertEquals(1, graph.countEdges("Friend"));

    final Iterator<Vertex> v = graph.getVerticesOfClass("V2").iterator();
    assertTrue(v.hasNext());
    assertNotNull(v.next());
    assertTrue(v.hasNext());
    final Vertex v1 = v.next();
    assertNotNull(v1);

    final Set<String> v1Props = v1.getPropertyKeys();

    assertEquals(3, v1Props.size());
    assertEquals(v1.getProperty("name"), "Luca");
    assertEquals(v1.getProperty("surname"), "Garulli");
    assertEquals(v1.getProperty("fid"), 1);

    final Iterator<Edge> edge = v1.getEdges(Direction.IN).iterator();
    assertTrue(edge.hasNext());

    final Edge e = edge.next();
    assertNotNull(e);
    final Set<String> eProps = e.getPropertyKeys();
    assertEquals(1, eProps.size());
    assertEquals(e.getProperty("since"), 1996);

    final Vertex v0 = e.getVertex(Direction.OUT);
    assertNotNull(v0);

    final Set<String> v0Props = v0.getPropertyKeys();

    assertEquals(3, v0Props.size());
    assertEquals(v0.getProperty("name"), "Jay");
    assertEquals(v0.getProperty("surname"), "Miner");
    assertEquals(v0.getProperty("id"), 0);
  }

  @Test(expected = OETLProcessHaltedException.class)
  // @Test
  public void testErrorOnDuplicateVertex() {
    process("{source: { content: { value: 'name,surname,friend\nJay,Miner,Luca\nJay,Miner,Luca' } }, extractor : { csv: {} },"
        + " transformers: [{merge: {joinFieldName:'name',lookup:'V1.name'}}, {vertex: {class:'V1'}}, {edge:{class:'Friend',joinFieldName:'friend',lookup:'V2.name'}},"
        + "], loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph', useLightweightEdges:false } } }");

    assertEquals(1, graph.countVertices("V1"));

  }

  @Test
  public void testSkipDuplicateVertex() {
    process("{source: { content: { value: 'name,surname,friend\nJay,Miner,Luca\nJay,Miner,Luca' } }, extractor : { csv: {} },"
        + " transformers: [ {merge: {joinFieldName:'name',lookup:'V1.name'}}, {vertex: {class:'V1'}}, {edge:{class:'Friend',skipDuplicates:true, joinFieldName:'friend',lookup:'V2.name'}},"
        + "], loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph', useLightweightEdges:false } } }");

    assertEquals(1, graph.countVertices("V1"));
    assertEquals(1, graph.countVertices("V2"));
    assertEquals(1, graph.countEdges("Friend"));
  }

  @Test
  public void testVertexAndEdgesOnSeparatedFiles() {
    // IMPORT PERSON (VERTICES)
    process("{source: { content: { value: 'id,name\n1,Luigi\n2,Luca\n3,Enrico\n4,Franco\n5,Gianni' } }, extractor : { csv: {} },"
        + " transformers: [ {merge: {joinFieldName:'id',lookup:'PersonMF.id'}}, {vertex: {class:'PersonMF'}}"
        + "], loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph', classes: [{name:'PersonMF',extends:'V'}] } } }");

    assertEquals(5, graph.countVertices("PersonMF"));

    // IMPORT FRIEND (EDGES)
    process("{source: { content: { value: 'friend_from,friend_to,since\n" + "1,2,2005\n" + "1,3,2008\n" + "2,3,2008\n"
        + "1,4,2015\n" + "2,5,2008\n" + "3,5,2015\n" + "4,5,2015' } }, extractor : { csv: {} }," + " transformers: ["
        + "{merge: {joinFieldName:'friend_from',lookup:'PersonMF.id'}}," + "{vertex: {class:'PersonMF'}},"
        + "{edge:{class:'FriendMF',joinFieldName:'friend_to',lookup:'PersonMF.id',edgeFields:{since:'${input.since}'} }},"
        + "{field: {operation:'remove', fieldNames:['friend_from','friend_to','since']}}"
        + "], loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph', classes: [{name:'FriendMF',extends:'E'}] } } }");

    assertEquals(5, graph.countVertices("PersonMF"));
    assertEquals(7, graph.countEdges("FriendMF"));
  }

}
