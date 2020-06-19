package com.orientechnologies.tinkerpop.server;

import java.util.Set;
import java.util.function.Function;
import javax.script.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.DefaultGraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph;

/** Created by Enrico Risa on 06/09/2017. */
public class OrientGremlinGraphManager implements GraphManager {

  private DefaultGraphManager delegate;

  public OrientGremlinGraphManager(Settings settings) {
    delegate = new DefaultGraphManager(settings);
  }

  @Override
  public Set<String> getGraphNames() {
    return delegate.getGraphNames();
  }

  @Override
  public Graph getGraph(String s) {
    return delegate.getGraph(s);
  }

  @Override
  public void putGraph(String s, Graph graph) {
    delegate.putGraph(s, graph);
  }

  @Override
  public Set<String> getTraversalSourceNames() {
    return delegate.getTraversalSourceNames();
  }

  @Override
  public TraversalSource getTraversalSource(String s) {
    return delegate.getTraversalSource(s);
  }

  @Override
  public void putTraversalSource(String s, TraversalSource traversalSource) {
    delegate.putTraversalSource(s, traversalSource);
  }

  @Override
  public TraversalSource removeTraversalSource(String s) {
    return delegate.removeTraversalSource(s);
  }

  @Override
  public Bindings getAsBindings() {
    return delegate.getAsBindings();
  }

  @Override
  public void rollbackAll() {
    delegate.rollbackAll();
  }

  @Override
  public void rollback(Set<String> set) {
    delegate.rollback(set);
  }

  @Override
  public void commitAll() {
    delegate.commitAll();
  }

  @Override
  public void commit(Set<String> set) {
    delegate.commit(set);
  }

  @Override
  public Graph openGraph(String s, Function<String, Graph> function) {
    return delegate.openGraph(s, function);
  }

  @Override
  public Graph removeGraph(String s) throws Exception {
    return delegate.removeGraph(s);
  }
}
