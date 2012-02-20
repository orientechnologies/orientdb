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


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 * 
 */
public class OrientGraphJdbcQueryTest extends OrientGraphJdbcBaseTest {

	@Before
	public void setup() {
		new File("./working/").mkdir();

	}

	@After
	public void clean() {
		
	}

	@Test
	public void testQuery() {
		String query = "select * from OGraphVertex where description like '%Production Cell%'";
		System.out.println("Executing: " + query);
		try {

			Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);

			ResultSet result = statement.executeQuery(query);

			while (result.next()) {
				ResultSetMetaData resultMetaData = result.getMetaData();
				for (int i = 1; i <= resultMetaData.getColumnCount(); i++) {
					String columnName = resultMetaData.getColumnName(i);

					int columnType = resultMetaData.getColumnType(i);
					Integer expectedType = OrientGraphJdbcCreationHelper.SQL_TYPES.get(columnName);
					System.out.println("columnType:: " + columnType);
					System.out.println("expectedType:: " + expectedType);

					assertColumnType(columnName, expectedType, columnType);

					Object value = result.getObject(i);

					System.out.print(i + " - ");
					if (value instanceof Collection<?>) {
						Collection<?> list = (Collection<?>) value;
						System.out.println("----- collection");
						for (Object obj : list)
							System.out.println("\t" + obj + " class: " + obj.getClass().getName());
						System.out.println("----- end collection");
					} else System.out.println(resultMetaData.getColumnName(i) + ": " + value + " (" + value.getClass().getName() + ")");
				}
				System.out.println("--------------------------------");
			}

		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail("An error occured during the execution of the SQL Query.");
		}
	}

	private void assertColumnType(String columnName, int expectedType, int actualType) {
		Assert.assertEquals("Unexpected type of the column:: " + columnName, expectedType, actualType);
	}

	@Test
	public void shouldLoadSingleBinaryData() throws FileNotFoundException, IOException, SQLException, NoSuchAlgorithmException {
		File binary_file = new File("./working/output_binary.pdf");
		binary_file.delete();

		String digest = this.calculateMD5checksum(ClassLoader.getSystemResourceAsStream(OrientGraphJdbcCreationHelper.FILE_NAME));

		String query = "select * from OGraphVertex where name like '%single binary record%'";

		Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);

		ResultSet result = statement.executeQuery(query);

		assertTrue(result.next());
		byte[] bytes = result.getBytes("binary_data");

		new FileOutputStream(binary_file).write(bytes);
		assertTrue("The file '" + binary_file.getName() + "' does not exist", binary_file.exists());
		this.verifyMD5checksum(binary_file, digest);

	}

	@Test
	public void shouldLoadBlobFromMultipleBinaryData() throws FileNotFoundException, IOException, SQLException, NoSuchAlgorithmException {
		File binaryFile = new File("./working/output_blob.pdf");
		binaryFile.delete();

		String digest = this.calculateMD5checksum(ClassLoader.getSystemResourceAsStream(OrientGraphJdbcCreationHelper.FILE_NAME));

		String query = "select * from OGraphVertex where name like '%multiple binary record%'";

		Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);

		ResultSet result = statement.executeQuery(query);

		assertTrue(result.next());
		Blob blob = result.getBlob("binary_data");
		assertNotNull(blob);

		System.out.println("length of blob:: " + blob.length());
		new FileOutputStream(binaryFile).write(blob.getBytes(1, (int) blob.length()));
		assertTrue("The file '" + binaryFile.getName() + "' does not exist", binaryFile.exists());
		this.verifyMD5checksum(binaryFile, digest);

	}

	private void verifyMD5checksum(File fileToBeChecked, String digest) {
		try {
			assertEquals("The MD5 checksum of the file '" + fileToBeChecked.getAbsolutePath() + "' does not match the given one.", digest, calculateMD5checksum(new FileInputStream(fileToBeChecked)));
		} catch (NoSuchAlgorithmException e) {
			fail(e.getMessage());
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}

	private String calculateMD5checksum(InputStream fileStream) throws NoSuchAlgorithmException, IOException {
		MessageDigest md = MessageDigest.getInstance("MD5");

		try {
			fileStream = new DigestInputStream(fileStream, md);
			while (fileStream.read() != -1)
				;
		} finally {
			try {
				fileStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return new BigInteger(1, md.digest()).toString(16);
	}
}
