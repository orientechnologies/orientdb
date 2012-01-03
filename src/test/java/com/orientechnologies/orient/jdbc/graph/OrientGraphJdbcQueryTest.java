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
import java.sql.Types;
import java.util.Collection;

import junit.framework.Assert;

import org.junit.Test;

/**
 * 
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 *
 */
public class OrientGraphJdbcQueryTest extends OrientGraphJdbcBaseTest {
	
	@Test
	public void testQuery () {
		String query = "select * from OGraphVertex where description like '%Production Cell%'";
		System.out.println("Executing: " + query);
		try {
			
			Statement statement = conn.createStatement(
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			ResultSet result = statement.executeQuery(query);
			
			ResultSetMetaData resultMetaData = null;
			Object value = null;
			String columnName = null;
			while (result.next()) {
				resultMetaData = result.getMetaData();
				for (int i = 1; i <= resultMetaData.getColumnCount(); i++) {
					columnName = resultMetaData.getColumnName(i);
					this.assertColumnType(columnName, OrientGraphJdbcCreationHelper.SQL_TYPES.get(columnName), resultMetaData.getColumnType(i));
					value = result.getObject(i);
					
					System.out.print(i + " - ");
					if (value instanceof Collection<?>) {
						Collection<?> list = (Collection<?>) value;
						System.out.println("----- collection");
						for (Object obj: list)
							System.out.println("\t" + obj + " class: " + obj.getClass().getName());
						System.out.println("----- end collection");
					} else
						System.out.println(resultMetaData.getColumnName(i) + ": " +
								value + " (" + value.getClass().getName() + ")");
				}
				System.out.println("--------------------------------");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail("An error occured during the execution of the SQL Query.");
		}
	}
	
	private void assertColumnType (String columnName, int expectedType, int actualType) {
		Assert.assertEquals("Unexpected type of the column " + columnName, expectedType, actualType);
	}

	@Test
	public void testQuery_binaryData () throws FileNotFoundException, IOException {
		final String BLOB_OUTPUT_PATH = "D:\\GIT Repository\\orientdb-jdbc\\src\\test\\resources\\output_blob.pdf";
		File blob_file = new File(BLOB_OUTPUT_PATH);
		
		final String BINARY_OUTPUT_PATH = "D:\\GIT Repository\\orientdb-jdbc\\src\\test\\resources\\output_binary.pdf";
		File binary_file = new File (BINARY_OUTPUT_PATH);
		
		String digest = null;
		try {
			digest = this.calculateMD5checksum(
				ClassLoader.getSystemResourceAsStream(OrientGraphJdbcCreationHelper.FILE_NAME));
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
			Assert.fail(e1.getMessage());
		}
		
		String query = "select * from OGraphVertex where name like '%binary record%'";
		System.out.println("Executing: " + query);
		try {
			
			Statement statement = conn.createStatement(
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			ResultSet result = statement.executeQuery(query);
			
			ResultSetMetaData resultMetaData = null;
			while (result.next()) {
				resultMetaData = result.getMetaData();
				Blob data;
				byte[] bytes;
				for (int i = 1; i <= resultMetaData.getColumnCount(); i++) {
					if (resultMetaData.getColumnType(i) == Types.BLOB) {
						data = result.getBlob(i);
						System.out.println("Column name: " + resultMetaData.getColumnName(i) + " Output: " + 
								BLOB_OUTPUT_PATH);
							new FileOutputStream(blob_file).write((data.getBytes(1, (int) data.length())));
					} else if (resultMetaData.getColumnType(i) == Types.BINARY) {
						bytes = result.getBytes(i);
						System.out.println("Column name: " + resultMetaData.getColumnName(i) + " Output: " + 
								BINARY_OUTPUT_PATH);
							new FileOutputStream(binary_file).write(bytes);
					}
				}
			}
			//Check if a BINARY file has been created
			Assert.assertTrue("The file '" + BINARY_OUTPUT_PATH + "' does not exist", binary_file.isFile());
			//Check binary file checksum
			this.verifyMD5checksum(binary_file, digest);
			Assert.assertTrue("The file '" + BLOB_OUTPUT_PATH + "' does not exist", blob_file.isFile());
			//Check binary file checksum
			this.verifyMD5checksum(blob_file, digest);
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail("An error occured during the execution of the SQL Query.");
		}
	}
	
	private void verifyMD5checksum (File fileToBeChecked, String digest) {
		try {
			Assert.assertEquals("The MD5 checksum of the file '" + fileToBeChecked.getAbsolutePath() +
				"' does not match the given one.", digest, 
				this.calculateMD5checksum(new FileInputStream(fileToBeChecked)));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	private String calculateMD5checksum (InputStream fileStream) throws NoSuchAlgorithmException, IOException {
		MessageDigest md = MessageDigest.getInstance("MD5");
		
		try {
		  fileStream = new DigestInputStream(fileStream, md);
		  while (fileStream.read() != -1);
		}
		finally {
			  try {
				fileStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return new BigInteger(1,md.digest()).toString(16);
	}
}
