package com.orientechnologies.orient.graph.gremlin;


public class TestLoadGraph {
  private static final String INPUT_FILE = "target/test-classes/graph-example-2.xml";
  private static final String DBURL      = "local:target/databases/tinkerpop";
  private String              inputFile  = INPUT_FILE;
  private String              dbURL      = DBURL;

  public static void main(final String[] args) throws Exception {
//    new TestLoadGraph(args).testImport();
  }

  public TestLoadGraph() {
    inputFile = INPUT_FILE;
    dbURL = DBURL;
  }

  public TestLoadGraph(final String[] args) {
    inputFile = args.length > 0 ? args[0] : INPUT_FILE;
    dbURL = args.length > 1 ? args[1] : DBURL;
  }

//  @Test
//  public void testImport() throws IOException, FileNotFoundException {
//    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
//    OGraphDatabase db = new OGraphDatabase(DBURL);
//    ODatabaseHelper.deleteDatabase(db);
//    OrientBaseGraph g = new OrientBatchGraph(dbURL);
//
//    System.out.println("Importing graph from file '" + inputFile + "' into database: " + g + "...");
//
//    final long startTime = System.currentTimeMillis();
//
//    GraphMLReader.inputGraph(g, new FileInputStream(inputFile), 10000, null, null, null);
//
//    System.out.println("Imported in " + (System.currentTimeMillis() - startTime) + "ms. Vertexes: " + g.countVertices()
//        + ", Edges: " + g.countEdges("E"));
//
//    g.shutdown();
//  }
}
