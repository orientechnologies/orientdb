package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Graph;
import org.hamcrest.core.IsEqual;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 2/6/14
 */
@RunWith(JUnit4.class)
public class OrientGraphClassicRemoteTest extends OrientGraphRemoteTest {
	@Before
	public void setUp() throws Exception {
		Assume.assumeThat(getEnvironment(), IsEqual.equalTo(OrientGraphTest.ENV.RELEASE));
		super.setUp();
	}

	public Graph generateGraph(final String graphDirectoryName) {
		this.currentGraph = (OrientGraph) super.generateGraph(graphDirectoryName);
		this.currentGraph.setUseLightweightEdges(false);
		this.currentGraph.setUseClassForEdgeLabel(false);
		this.currentGraph.setUseClassForVertexLabel(false);
		this.currentGraph.setUseVertexFieldsForEdgeLabels(false);
		return this.currentGraph;
	}
}