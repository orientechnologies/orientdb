package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class ODBFeatures {

  public static class OrientFeatures implements Features {

    static final OrientFeatures INSTANCE_NOTX = new OrientFeatures(false);
    static final OrientFeatures INSTANCE_TX = new OrientFeatures(true);

    private OrientGraphFeatures graphFeatures;

    private OrientFeatures(boolean transactionalGraph) {
      this.graphFeatures = new OrientGraphFeatures(transactionalGraph);
    }

    @Override
    public GraphFeatures graph() {
      return graphFeatures;
    }

    @Override
    public EdgeFeatures edge() {
      return OrientEdgeFeatures.INSTANCE;
    }

    @Override
    public VertexFeatures vertex() {
      return OrientVertexFeatures.INSTANCE;
    }

    @Override
    public String toString() {
      return StringFactory.featureString(this);
    }
  }

  public abstract static class OrientElementFeatures implements Features.ElementFeatures {

    @Override
    public boolean supportsAnyIds() {
      return false;
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean supportsNumericIds() {
      return false;
    }

    @Override
    public boolean supportsStringIds() {
      return false;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
      return false;
    }

    @Override
    public boolean supportsUuidIds() {
      return false;
    }

    @Override
    public boolean willAllowId(Object id) {
      return false;
    }
  }

  public static class OrientVertexFeatures extends OrientElementFeatures
      implements Features.VertexFeatures {

    static final OrientVertexFeatures INSTANCE = new OrientVertexFeatures();

    @Override
    public boolean supportsMultiProperties() {
      return false;
    }

    @Override
    public VertexPropertyFeatures properties() {
      return OrientVertexPropertyFeatures.INSTANCE;
    }
  }

  public static class OrientEdgeFeatures extends OrientElementFeatures
      implements Features.EdgeFeatures {

    static final OrientEdgeFeatures INSTANCE = new OrientEdgeFeatures();
  }

  public static class OrientGraphFeatures implements Features.GraphFeatures {

    protected final boolean transactionalGraph;

    private OrientGraphFeatures(boolean transactionalGraph) {
      this.transactionalGraph = transactionalGraph;
    }

    @Override
    public boolean supportsComputer() {
      return false;
    }

    @Override
    public boolean supportsTransactions() {
      return transactionalGraph;
    }

    @Override
    public boolean supportsThreadedTransactions() {
      return false;
    }

    @Override
    public VariableFeatures variables() {
      return OrientVariableFeatures.INSTANCE;
    }
  }

  public static class OrientVariableFeatures implements Features.VariableFeatures {

    static final OrientVariableFeatures INSTANCE = new OrientVariableFeatures();

    @Override
    public boolean supportsVariables() {
      return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
      return false;
    }

    @Override
    public boolean supportsBooleanValues() {
      return false;
    }

    @Override
    public boolean supportsByteArrayValues() {
      return false;
    }

    @Override
    public boolean supportsByteValues() {
      return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
      return false;
    }

    @Override
    public boolean supportsDoubleValues() {
      return false;
    }

    @Override
    public boolean supportsFloatArrayValues() {
      return false;
    }

    @Override
    public boolean supportsFloatValues() {
      return false;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
      return false;
    }

    @Override
    public boolean supportsIntegerValues() {
      return false;
    }

    @Override
    public boolean supportsLongArrayValues() {
      return false;
    }

    @Override
    public boolean supportsLongValues() {
      return false;
    }

    @Override
    public boolean supportsMapValues() {
      return false;
    }

    @Override
    public boolean supportsMixedListValues() {
      return false;
    }

    @Override
    public boolean supportsSerializableValues() {
      return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
      return false;
    }

    @Override
    public boolean supportsStringValues() {
      return false;
    }

    @Override
    public boolean supportsUniformListValues() {
      return false;
    }
  }

  public static class OrientVertexPropertyFeatures implements Features.VertexPropertyFeatures {

    static final OrientVertexPropertyFeatures INSTANCE = new OrientVertexPropertyFeatures();

    @Override
    public boolean supportsAnyIds() {
      return false;
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean supportsNumericIds() {
      return false;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
      return false;
    }

    @Override
    public boolean supportsUuidIds() {
      return false;
    }

    @Override
    public boolean willAllowId(Object id) {
      return false;
    }
  }
}
