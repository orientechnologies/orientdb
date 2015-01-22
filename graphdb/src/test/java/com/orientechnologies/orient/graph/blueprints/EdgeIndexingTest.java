package com.orientechnologies.orient.graph.blueprints;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 1/30/14
 */
public class EdgeIndexingTest {
	@Test
	public void testOutLinksUniqueness() {
		final String url = "memory:" + this.getClass().getSimpleName();
		OrientGraph graph = new OrientGraph(url);
		graph.drop();

		graph = new OrientGraph(url);
    graph.setUseLightweightEdges(true);
		graph.createEdgeType("link");
		graph.setAutoStartTx(false);

		OClass outVertexType = graph.createVertexType("IndexedOutVertex");
		outVertexType.createProperty("out_link", OType.LINKBAG);
		outVertexType.createIndex("uniqueLinkIndex", "unique", "out_link");

		graph.setAutoStartTx(true);


		Vertex vertexOutOne = graph.addVertex("class:IndexedOutVertex");

		Vertex vertexInOne = graph.addVertex(null);
		Vertex vertexInTwo = graph.addVertex(null);

		vertexOutOne.addEdge("link", vertexInOne);
		vertexOutOne.addEdge("link", vertexInTwo);
		graph.commit();

		Vertex vertexOutTwo = graph.addVertex("class:IndexedOutVertex");
		vertexOutTwo.addEdge("link", vertexInTwo);

		try {
			graph.commit();

			//in vertex can be linked by only one out vertex.
			Assert.fail();
		} catch (ORecordDuplicatedException e) {
		}

		graph.drop();
	}
}
