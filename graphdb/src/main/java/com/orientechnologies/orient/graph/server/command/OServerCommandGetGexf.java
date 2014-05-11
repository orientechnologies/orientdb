package com.orientechnologies.orient.graph.server.command;

import it.uniroma1.dis.wsngroup.gexf4j.core.Edge;
import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import static com.orientechnologies.orient.core.serialization.serializer.OJSONWriter.writeValue;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OServerCommandGetGexf extends
		OServerCommandAuthenticatedDbAbstract {
	private static final String[] NAMES = { "GET|gexf/*" };
	
	
	public OServerCommandGetGexf() {
	  }

	  public OServerCommandGetGexf(final OServerCommandConfiguration iConfig) {
	  }
	
	@Override
	@SuppressWarnings("unchecked")
	public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse)
			throws Exception {

		String[] urlParts = checkSyntax(
				iRequest.url,
				4,
				"Syntax error: gexf/<database>/<language>/<query-text>[/<limit>][/<fetchPlan>][/<edgeType>].<br/>Limit is optional and is setted to 20 by default. Set expressely to 0 to have no limits.");

		final String language = urlParts[2];
		final String text = urlParts[3];
		final int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4])
				: 20;
		final String fetchPlan = urlParts.length > 5 ? urlParts[5] : null;
		
		/*edgeType possible values :
		 * 		undirected (default) - undirected graph
		 * 		directed			 - directed graph 
		 */		
		final String edgeType = urlParts.length > 6 ? urlParts[6] : null;

		iRequest.data.commandInfo = "Gexf";
		iRequest.data.commandDetail = text;

		final ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);

		final OrientGraph graph = OGraphCommandExecutorSQLFactory
				.getGraph(false);
		try {

			final Iterable<OrientVertex> vertices;
			if (language.equals("sql"))
				vertices = graph.command(
						new OSQLSynchQuery<OrientVertex>(text, limit)
								.setFetchPlan(fetchPlan)).execute();
			else if (language.equals("gremlin")) {
				List<Object> result = new ArrayList<Object>();
				OGremlinHelper.execute(graph, text, null, null, result, null,
						null);

				vertices = new ArrayList<OrientVertex>(result.size());

				for (Object o : result) {
					((ArrayList<OrientVertex>) vertices).add(graph
							.getVertex((OIdentifiable) o));
				}
			} else
				throw new IllegalArgumentException("Language '" + language
						+ "' is not supported. Use 'sql' or 'gremlin'");

			sendRecordsContent(iRequest, iResponse, vertices, fetchPlan,
					edgeType);

		} finally {
			if (graph != null)
				graph.shutdown();

			if (db != null)
				db.close();
		}

		return false;
	}

	protected void sendRecordsContent(final OHttpRequest iRequest,
			final OHttpResponse iResponse, Iterable<OrientVertex> iRecords,
			String iFetchPlan, String edgeType) throws IOException {
		final StringWriter buffer = new StringWriter();
		final StaxGraphWriter gexfWriter = new StaxGraphWriter();

		generateGraphDbOutput(iRecords, edgeType, gexfWriter, buffer);

		iResponse.send(OHttpUtils.STATUS_OK_CODE,
				OHttpUtils.STATUS_OK_DESCRIPTION, "text/xml",
				buffer.toString(), null);
	}

	protected void generateGraphDbOutput(
			final Iterable<OrientVertex> iVertices, String edgeType,
			final StaxGraphWriter gexfWriter, final StringWriter buffer) throws IOException {
		if (iVertices == null)
			return;

		// Create gexf graph
		Gexf gexf = new GexfImpl();
		Calendar date = Calendar.getInstance();

		gexf.getMetadata().setLastModified(date.getTime())
				.setCreator("OrientDB")
				.setDescription("OrientDB query network");
		gexf.setVisualization(true);

		Graph graph = gexf.getGraph();
		
		//Instantiate attrList of graph
		AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
		graph.getAttributeLists().add(attrList);

		if ("directed".equals(edgeType)) {

			graph.setDefaultEdgeType(EdgeType.DIRECTED);
		} else {

			graph.setDefaultEdgeType(EdgeType.UNDIRECTED);
		}

		// CREATE A SET TO SPEED UP SEARCHES ON VERTICES
		final Set<OrientVertex> vertexes = new HashSet<OrientVertex>();
		final Set<OrientEdge> edges = new HashSet<OrientEdge>();

		// This map is used to check if the attribute already exists into graph's attributes list
		final Map<String, Attribute> attributes = new HashMap<String, Attribute>();

		final Map<String, Node> nodes = new HashMap<String, Node>();
		int i=0;
		
		for (OrientVertex id : iVertices)
			vertexes.add(id);
		
		
		//Creating gexf nodes
		for (OrientVertex vertex : vertexes) {

			Node n = graph.createNode(vertex.getIdentity().toString());

			//Add attribute in attrList
			for (String field : vertex.getPropertyKeys()) {
				final Object v = vertex.getProperty(field);
				if (v != null) {

					Attribute attr;

					if (attributes.containsKey(field))
						attr = attributes.get(field);
					else {

						attr = attrList.createAttribute("" + i,
								AttributeType.STRING, field);
						i++;
					}
					n.getAttributeValues().addValue(attr,
							writeValue(v, v.getClass().getName()).replaceAll("\"", ""));
					attributes.put(field, attr);
				}

			}

			nodes.put(vertex.getIdentity().toString(), n);

			// ADD ALL THE EDGES
			for (com.tinkerpop.blueprints.Edge e : vertex
					.getEdges(Direction.BOTH))
				edges.add((OrientEdge) e);
		}
		
		//Creating gexf edges
		for (OrientEdge edge : edges) {
			// Retrieve vertices to create gexf edge
			if (nodes.containsKey(edge.getInVertex().getIdentity().toString())
					&& nodes.containsKey(edge.getOutVertex().getIdentity()
							.toString())) {

				Node destNode = nodes.get(edge.getInVertex().getIdentity()
						.toString());
				Node sourceNode = nodes.get(edge.getOutVertex().getIdentity()
						.toString());
				
				Edge e = sourceNode.connectTo(destNode);
				
				if ("directed".equals(edgeType)) {

					e.setEdgeType(EdgeType.DIRECTED);
				} else {

					e.setEdgeType(EdgeType.UNDIRECTED);
				}
				for (String field : edge.getPropertyKeys()) {

					final Object v = edge.getProperty(field);

					if (v != null) {

						Attribute attr;

						if (attributes.containsKey(field))
							attr = attributes.get(field);
						else {

							attr = attrList.createAttribute("" + i,
									AttributeType.STRING, field);
							i++;
							
						}
						e.getAttributeValues().addValue(attr,
								writeValue(v,v.getClass().getName()).replaceAll("\"", ""));

						attributes.put(field, attr);
						

					}

				}

			}

		}
		
	 //WRITING GEXF FILE
	 gexfWriter.writeToStream(gexf, buffer , "UTF-8");

	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
	

}