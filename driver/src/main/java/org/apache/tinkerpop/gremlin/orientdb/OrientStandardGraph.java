package org.apache.tinkerpop.gremlin.orientdb;

import com.google.common.collect.Maps;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.orientdb.io.OrientIoRegistry;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;

import java.util.Iterator;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.orientdb.OrientGraph.CONFIG_TRANSACTIONAL;

/**
 * Created by Enrico Risa on 30/08/2017.
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn("org.apache.tinkerpop.gremlin.orientdb.gremlintest.suite.OrientDBDebugSuite")
public class OrientStandardGraph implements OGraph {

  protected final ThreadLocal<OrientGraph> graphInternal = new ThreadLocal<>();
  private final   Map<Thread, OrientGraph> graphs        = Maps.newConcurrentMap();

  private final Configuration      config;
  private       OrientGraphFactory factory;
  private boolean transactional = true;
  private Transaction     tx;
  private OElementFactory elementFactory;

  protected OrientStandardGraph(OrientGraphFactory factory, Configuration config) {
    this.factory = factory;
    this.config = config;
    if (config.containsKey(CONFIG_TRANSACTIONAL)) {
      this.transactional = config.getBoolean(CONFIG_TRANSACTIONAL);
    }
    tx = new OrientStandardTransaction(this);
    elementFactory = new OElementFactory(this);
  }

  @Override
  public Vertex addVertex(Object... keyValues) {
    return graph().addVertex(keyValues);
  }

  @Override
  public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
    return graph().compute(graphComputerClass);
  }

  @Override
  public GraphComputer compute() throws IllegalArgumentException {
    return graph().compute();
  }

  @Override
  public Iterator<Vertex> vertices(Object... vertexIds) {
    return graph().vertices(vertexIds);
  }

  @Override
  public Iterator<Edge> edges(Object... edgeIds) {
    return graph().edges(edgeIds);
  }

  @Override
  public Transaction tx() {
    return tx;
  }

  @Override
  public void close() throws Exception {
    closeGraphs();
    factory.close();
  }

  @Override
  public Variables variables() {
    return graph().variables();
  }

  @Override
  public Features features() {
    if (transactional) {
      return ODBFeatures.OrientFeatures.INSTANCE_TX;
    } else {
      return ODBFeatures.OrientFeatures.INSTANCE_NOTX;
    }
  }

  @Override
  public Configuration configuration() {
    return config;
  }

  protected OrientGraph graph() {
    OrientGraph graph = graphInternal.get();
    if (graph == null) {
      if (transactional) {
        graph = factory.getTx();

      } else {
        graph = factory.getNoTx();
      }
      graph.setElementFactory(elementFactory);
      graphInternal.set(graph);
      graphs.put(Thread.currentThread(), graph);
    }
    return graph;
  }

  @Override
  public <I extends Io> I io(Io.Builder<I> builder) {
    return (I) OGraph.super.io(builder.onMapper(mb -> mb.addRegistry(OrientIoRegistry.getInstance())));
  }

  public void drop() {
    closeGraphs();
    factory.drop();
  }

  protected void closeGraphs() {
    graphInternal.set(null);
    graphs.forEach((k, v) -> v.close());
    graphs.clear();
  }

  @Override
  public String labelToClassName(String label, String prefix) {
    return graph().labelToClassName(label, prefix);
  }

  @Override
  public String classNameToLabel(String className) {
    return graph().classNameToLabel(className);
  }

  @Override
  public String createEdgeClass(String label) {
    return graph().createEdgeClass(label);
  }

  @Override
  public String createVertexClass(String label) {
    return graph().createVertexClass(label);
  }

  @Override
  public ODatabaseDocument getRawDatabase() {
    return graph().getRawDatabase();
  }

  @Override
  public OElementFactory elementFactory() {
    return graph().elementFactory();
  }

  public boolean isOpen() {
    return factory.isOpen();
  }

  @Override
  public String toString() {
    return graph().toString();
  }
}
