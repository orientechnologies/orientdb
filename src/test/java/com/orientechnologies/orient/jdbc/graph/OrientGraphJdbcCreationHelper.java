/*
 * Copyright 2011 TXT e-solutions SpA
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Authors:
 *      Salvatore Piccione (TXT e-solutions SpA)
 *
 * Contributors:
 *        Domenico Rotondi (TXT e-solutions SpA)
 */
package com.orientechnologies.orient.jdbc.graph;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.tinkerpop.blueprints.pgm.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientGraph;

public class OrientGraphJdbcCreationHelper {
	
	static final String URL_DB = /*"memory:test-graph-orient-jdbc";*/"remote:iot-at-work1/test-graph-orient-jdbc";
	
	private static final String ADMINISTRATOR_NAME = "root";
	
	private static final String ADMINISTRATOR_PWD = 
			/*OrientDB GraphEd 1.0rc7
			"099492FDD3D804847E29F9A3914226A7387F6134906FDFC12C7FB0226D024957";*/
			/*OrientDB 1.0rc8-SNAPSHOT*/
			"37BE21607CCB791388C531EC67EA20BEC9EFA211A6F564E2FBC41D03E11B775F";

	static final String USERNAME = "admin";
	
	static final String PASSWORD = "admin";

	static final String NAME = "name";

	static final String DESCRIPTION = "description";
	
	static final String CONTAINMENT_EDGE = "contains";
	
	static final String COMPONENT_EDGE = "has";

	static final String BINARY_DATA = "binary_data";

	static final String VALUE = "value";

	static final String CREATION_DATE_EDGE = "creation_date";

	static final String FILE_EDGE = "file";
	
	static final Map<String,Integer> SQL_TYPES = initExpectedSqlTypesMap();

	static final String FILE_NAME = "file.pdf";
	
	private static Map<String,Integer> initExpectedSqlTypesMap () {
		Map<String,Integer> map = Collections.synchronizedMap(new HashMap<String,Integer>());
		map.put(BINARY_DATA, Types.BLOB);
		map.put(DESCRIPTION, Types.JAVA_OBJECT);
		map.put(NAME, Types.JAVA_OBJECT);
		return map;
	}
	
		
	public static void createGraphDatabase () {
		OrientGraph graphDB = null;
		try {
			OServerAdmin adminTool = null;
			//I have to use a OServerAdmin instance to check the existance of a server
			//because the method ODatabaseXXX.exists() is not supported for remote database
			//In order to use such class, you have to use the root account
			adminTool = new OServerAdmin(URL_DB).connect(ADMINISTRATOR_NAME,
					ADMINISTRATOR_PWD);
			System.out.println("Successful connection to " + URL_DB);	
			if (adminTool.existsDatabase()) {
				System.out.println("The database already exists. It will be deleted and created again.");
				adminTool.dropDatabase();
			} 
			System.out.println("The database is going to be created.");
			adminTool.createDatabase("local");
			adminTool.close();
			System.out.println("Database created!");
			
			graphDB = new OrientGraph (URL_DB,USERNAME,PASSWORD);
			graphDB.setMaxBufferSize(0);
            graphDB.startTransaction();
            Vertex root = graphDB.addVertex(null);
            root.setProperty(NAME, "Plant");
            root.setProperty(DESCRIPTION, "This is the Plant");
            Vertex cell = graphDB.addVertex(null);
            cell.setProperty(NAME, "Cell 1");
            cell.setProperty(DESCRIPTION, "This is the Production Cell 1");
            graphDB.addEdge(null, root, cell, CONTAINMENT_EDGE);
           
            Vertex cellComponent = graphDB.addVertex(null);
            cellComponent.setProperty(NAME, "Cell Element A1");
            cellComponent.setProperty(DESCRIPTION, "This is an element of the production cell 1");
            graphDB.addEdge(null, cell, cellComponent, COMPONENT_EDGE);
            
            cell = graphDB.addVertex(null);
			cell.setProperty(NAME, "Cell 2");
			cell.setProperty(DESCRIPTION, "This is the Production Cell 2");
			graphDB.addEdge(null, root, cell, CONTAINMENT_EDGE);
			
			cellComponent = graphDB.addVertex(null);
			cellComponent.setProperty(NAME, "Cell Element B1");
			cellComponent.setProperty(DESCRIPTION, "This is an element of the production cell 2");
			graphDB.addEdge(null, cell, cellComponent, COMPONENT_EDGE);
			
			URI fileURI = ClassLoader.getSystemResource(FILE_NAME).toURI();
			
			Vertex binaryVertex = graphDB.addVertex(null);
			binaryVertex.setProperty(NAME, "NoSQL Definition (single binary record)");
			binaryVertex.setProperty(BINARY_DATA, loadFile(graphDB.getRawGraph(), fileURI));
			
			binaryVertex = graphDB.addVertex(null);
			binaryVertex.setProperty(NAME, "NoSQL Definition (multiple binary record)");
			binaryVertex.setProperty(BINARY_DATA, loadFile(graphDB.getRawGraph(), fileURI, 50000));
			
			root = graphDB.addVertex(null);
			root.setProperty(NAME, "SimpleVertex");
			
			Vertex prop = graphDB.addVertex(null);
			prop.setProperty(VALUE, new Date());
			graphDB.addEdge(null, root, prop, CREATION_DATE_EDGE);
			
			prop = graphDB.addVertex(null);
			prop.setProperty(VALUE, loadFile(graphDB.getRawGraph(), fileURI, 50000));
			graphDB.addEdge(null, root, prop, FILE_EDGE);
			
			prop = graphDB.addVertex(null);
			prop.setProperty(VALUE, "This is a simple vertex with three properties: this description, a file and a date");
			graphDB.addEdge(null, root, prop, DESCRIPTION);
			
			graphDB.stopTransaction(Conclusion.SUCCESS);
			graphDB.shutdown();
		} catch (Exception e) {
			System.err.println("An error occured during the creation of the database " + 
				URL_DB + ": " + e.getMessage());
			e.printStackTrace();
			if (graphDB != null)
				graphDB.stopTransaction(Conclusion.FAILURE);
		} finally {
			if (graphDB != null)
				graphDB.shutdown();
		}
	}
	
	/**
	 * Loads the file in a single instance of {@link ORecordBytes}
	 * 
	 * @param database
	 * @param fileURI
	 * @return
	 * @throws IOException 
	 */
	private static ORecordBytes loadFile (ODatabaseRecord database, URI fileURI) throws IOException {
		BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(new File (fileURI)));
		ORecordBytes record = new ORecordBytes(database);
		record.fromInputStream(inputStream);
		return record;
	}
	
	private static List<ORecordBytes> loadFile (ODatabaseRecord database, URI fileURI, int bufferSize) throws IOException {
		if (bufferSize > 0) {
			//declaring the file
			File binaryFile = new File (fileURI);
			//store the length of the file (in bytes)
			long binaryFileLength = binaryFile.length();
			//store the number of records
			int numberOfRecords = (int) (binaryFileLength / bufferSize);
			//store the remainder of the division above
			int remainder = (int) (binaryFileLength % bufferSize);
			//if the remainder is greater than zero, the number of records is incremented by one
			if (remainder > 0)
				numberOfRecords++;
			//declaring the chunks of binary data
			List<ORecordBytes> binaryChuncks = new ArrayList<ORecordBytes> (numberOfRecords);
			//defining file stream
			BufferedInputStream binaryStream = new BufferedInputStream(new FileInputStream(
				binaryFile));
			byte[] chunk;
			ORecordBytes recordChunck;
			for (int i = 0; i < numberOfRecords; i++) {
				if (i == numberOfRecords - 1)
					chunk = new byte[remainder];
				else
					chunk = new byte[bufferSize];
				//loading binary chunk
				binaryStream.read(chunk);
				//storing the binary chunk in the ORecordBytes
				recordChunck = new ORecordBytes(database,chunk);
				recordChunck.save();
				//database.save(recordChunck);
				binaryChuncks.add(recordChunck);
			}
			return binaryChuncks;
		} else
			return Arrays.asList(loadFile(database,fileURI));
	}
	
	public static void main (String[] args) {
		createGraphDatabase();
	}
}