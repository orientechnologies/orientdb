package com.orientechnologies.orient.graph.graphml;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.tool.ODatabaseImportException;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.ElementFactory;
import com.tinkerpop.blueprints.util.io.graphson.GraphElementFactory;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONTokens;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * GraphSONReader reads the data from a TinkerPop JSON stream to a graph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OGraphSONReader {
  private static final JsonFactory jsonFactory = new MappingJsonFactory();
  private final Graph graph;
  private OCommandOutputListener output;
  private long inputSize;

  /** @param graph the graph to populate with the JSON data */
  public OGraphSONReader(final Graph graph) {
    this.graph = graph;
  }

  /**
   * Input the JSON stream data into the graph. In practice, usually the provided graph is empty.
   *
   * @param jsonInputStream an InputStream of JSON data
   * @throws IOException thrown when the JSON data is not correctly formatted
   */
  public void inputGraph(final InputStream jsonInputStream) throws IOException {
    inputGraph(jsonInputStream, 1000);
  }

  /**
   * Input the JSON stream data into the graph. In practice, usually the provided graph is empty.
   *
   * @param filename name of a file of JSON data
   * @throws IOException thrown when the JSON data is not correctly formatted
   */
  public void inputGraph(final String filename) throws IOException {
    inputGraph(filename, 1000);
  }

  public void inputGraph(final InputStream jsonInputStream, int bufferSize) throws IOException {
    inputGraph(jsonInputStream, bufferSize, null, null);
  }

  public void inputGraph(final String filename, int bufferSize) throws IOException {
    inputGraph(filename, bufferSize, null, null);
  }

  /**
   * Input the JSON stream data into the graph. More control over how data is streamed is provided
   * by this method.
   *
   * @param filename name of a file of JSON data
   * @param bufferSize the amount of elements to hold in memory before committing a transactions
   *     (only valid for TransactionalGraphs)
   * @throws IOException thrown when the JSON data is not correctly formatted
   */
  public void inputGraph(
      final String filename,
      int bufferSize,
      final Set<String> edgePropertyKeys,
      final Set<String> vertexPropertyKeys)
      throws IOException {
    final File file = new File(filename);
    if (!file.exists()) throw new ODatabaseImportException("File '" + filename + "' not found");

    inputSize = file.length();
    final FileInputStream fis = new FileInputStream(filename);
    try {
      inputGraph(fis, bufferSize, edgePropertyKeys, vertexPropertyKeys);
    } finally {
      fis.close();
    }
  }

  /**
   * Input the JSON stream data into the graph. More control over how data is streamed is provided
   * by this method.
   *
   * @param jsonInputStream an InputStream of JSON data
   * @param bufferSize the amount of elements to hold in memory before committing a transactions
   *     (only valid for TransactionalGraphs)
   * @throws IOException thrown when the JSON data is not correctly formatted
   */
  public void inputGraph(
      final InputStream jsonInputStream,
      int bufferSize,
      final Set<String> edgePropertyKeys,
      final Set<String> vertexPropertyKeys)
      throws IOException {

    final JsonParser jp = jsonFactory.createJsonParser(jsonInputStream);

    // if this is a transactional localGraph then we're buffering
    final BatchGraph batchGraph = BatchGraph.wrap(graph, bufferSize);

    final ElementFactory elementFactory = new GraphElementFactory(batchGraph);
    OGraphSONUtility graphson =
        new OGraphSONUtility(
            GraphSONMode.NORMAL, elementFactory, vertexPropertyKeys, edgePropertyKeys);

    long importedVertices = 0;
    long importedEdges = 0;

    while (jp.nextToken() != JsonToken.END_OBJECT) {
      final String fieldname = jp.getCurrentName() == null ? "" : jp.getCurrentName();
      if (fieldname.equals(GraphSONTokens.MODE)) {
        jp.nextToken();
        final GraphSONMode mode = GraphSONMode.valueOf(jp.getText());
        graphson = new OGraphSONUtility(mode, elementFactory, vertexPropertyKeys, edgePropertyKeys);
      } else if (fieldname.equals(GraphSONTokens.VERTICES)) {
        jp.nextToken();
        while (jp.nextToken() != JsonToken.END_ARRAY) {
          final JsonNode node = jp.readValueAsTree();
          graphson.vertexFromJson(node);
          importedVertices++;
          printStatus(jp, importedVertices, importedEdges);

          if (importedVertices % 1000 == 0)
            ODatabaseRecordThreadLocal.instance().get().getLocalCache().invalidate();
        }
      } else if (fieldname.equals(GraphSONTokens.EDGES)) {
        jp.nextToken();
        while (jp.nextToken() != JsonToken.END_ARRAY) {
          final JsonNode node = jp.readValueAsTree();
          final Vertex inV =
              batchGraph.getVertex(
                  OGraphSONUtility.getTypedValueFromJsonNode(node.get(GraphSONTokens._IN_V)));
          final Vertex outV =
              batchGraph.getVertex(
                  OGraphSONUtility.getTypedValueFromJsonNode(node.get(GraphSONTokens._OUT_V)));
          graphson.edgeFromJson(node, outV, inV);
          importedEdges++;
          printStatus(jp, importedVertices, importedEdges);

          if (importedEdges % 1000 == 0)
            ODatabaseRecordThreadLocal.instance().get().getLocalCache().invalidate();
        }
      }
    }

    jp.close();

    batchGraph.commit();
  }

  public OCommandOutputListener getOutput() {
    return output;
  }

  public OGraphSONReader setOutput(final OCommandOutputListener output) {
    this.output = output;
    return this;
  }

  protected void printStatus(
      final JsonParser jp, final long importedVertices, final long importedEdges) {
    if (output != null && (importedVertices + importedEdges) % 50000 == 0) {
      final long parsed = jp.getCurrentLocation().getByteOffset();

      if (inputSize > 0)
        output.onMessage(
            String.format(
                "Imported %d graph elements: %d vertices and %d edges. Parsed %s/%s (uncompressed) (%s%%)",
                importedVertices + importedEdges,
                importedVertices,
                importedEdges,
                OFileUtils.getSizeAsString(parsed),
                "" + OFileUtils.getSizeAsString(inputSize),
                "" + parsed * 100 / inputSize));
      else
        output.onMessage(
            String.format(
                "Imported %d graph elements: %d vertices and %d edges. Parsed %s (uncompressed)",
                importedVertices + importedEdges,
                importedVertices,
                importedEdges,
                OFileUtils.getSizeAsString(parsed)));
    }
  }
}
