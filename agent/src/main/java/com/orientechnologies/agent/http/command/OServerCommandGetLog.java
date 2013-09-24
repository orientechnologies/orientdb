/*
 * Copyright 2010-2013 Orient Technologies LTD
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 */
package com.orientechnologies.agent.http.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

public class OServerCommandGetLog extends OServerCommandAuthenticatedServerAbstract {

	private static final String[]	NAMES					= { "GET|log/*" };

	private static final String		TAIL					= "tail";

	private static final String		FILE					= "file";

	private static final String		SEARCH				= "search";

	SimpleDateFormat							dateFormatter	= new SimpleDateFormat("yyyy-MM-dd");

	public OServerCommandGetLog(final OServerCommandConfiguration iConfiguration) {
		super(iConfiguration.pattern);
	}

	public OServerCommandGetLog() {
		super("server.log");
	}

	@Override
	public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

		final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: log/<type>/<value>");

		String type = urlParts[1];

		String value = urlParts[2];

		String orientdb_home = System.getenv("ORIENTDB_HOME");

		String logsDirectory = orientdb_home.concat("/log");

		File directory = new File(logsDirectory);
		// Reading directory contents

		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File directory, String fileName) {
				return !fileName.endsWith(".lck");
			}
		};

		File[] files = directory.listFiles(filter);    
		Arrays.sort(files);
		// List<String> lines = new ArrayList<String>();

		List<ODocument> subdocuments = new ArrayList<ODocument>();
		final ODocument result = new ODocument();

		String line = "";
		String dayToDoc = "";
		String hour = "";
		String typeToDoc = "";
		String info = "";

		ODocument doc = new ODocument();

		if (TAIL.equals(type)) {
			Integer valueInt = new Integer(value);
			File f = files[0];
			BufferedReader br = new BufferedReader(new FileReader(f));
			valueInt = new Integer(value);
			for (int i = 0; i < valueInt && line != null; i++) {
				line = br.readLine();
				if (line != null) {
					String[] split = line.split(" ");
					if (split != null) {
						Date day = null;

						try {

							day = dateFormatter.parse(split[0]);
							// trying to create a Date
							if (doc.field("day") != null) {
								doc.field("info", info);
								subdocuments.add(doc);
								doc = new ODocument();
							}

							// Created new Doc
							dayToDoc = split[0];
							hour = split[1];
							typeToDoc = split[2];

							if (doc.field("day") == null) {
								doc = new ODocument();
								addFieldToDoc(dayToDoc, hour, typeToDoc, doc);
							}
							line = line.replace(split[0], "");
							line = line.replace(split[1], "");
							line = line.replace(split[2], "");
							info = line;

						} catch (Exception e) {
							//stack trace
							info = info.concat(line);
						}
					}
				} else {
					if (doc.field("day") != null) {
						addFieldToDoc(dayToDoc, hour, typeToDoc, doc);
						doc.field("info", info);
						subdocuments.add(doc);
					}
				}
			}
			br.close();
		} else if (FILE.equals(type)) {
			Integer valueInt = new Integer(value);
			File f = files[new Integer(valueInt)];
			BufferedReader br = new BufferedReader(new FileReader(f));
			while (line != null) {
				line = br.readLine();
				if (line != null) {
					String[] split = line.split(" ");
					if (split != null) {
						Date day = null;

						try {

							day = dateFormatter.parse(split[0]);
							if (doc.field("day") != null) {
								doc.field("info", info);
								subdocuments.add(doc);
								doc = new ODocument();
							}

							dayToDoc = split[0];
							hour = split[1];
							typeToDoc = split[2];

							if (doc.field("day") == null) {
								doc = new ODocument();
								addFieldToDoc(dayToDoc, hour, typeToDoc, doc);
							}
							line = line.replace(split[0], "");
							line = line.replace(split[1], "");
							line = line.replace(split[2], "");
							info = line;

						} catch (Exception e) {
							info = info.concat(line);
						}
					}

				} else {
					if (doc.field("day") != null) {
						addFieldToDoc(dayToDoc, hour, typeToDoc, doc);
						doc.field("info", info);
						subdocuments.add(doc);
					}
				}

			}
			br.close();
		} else if (SEARCH.equals(type)) {

			for (int i = 0; i < files.length - 1; i++) {
				line = "";
				BufferedReader br = new BufferedReader(new FileReader(files[i]));
				while (line != null) {
					line = br.readLine();
					if (line != null) {
						String[] split = line.split(" ");
						if (split != null) {
							Date day = null;

							try {

								day = dateFormatter.parse(split[0]);
								if (doc.field("day") != null) {
									doc.field("info", info);
									if (info.contains(value)) {
										subdocuments.add(doc);
									}
									doc = new ODocument();
								}

								dayToDoc = split[0];
								hour = split[1];
								typeToDoc = split[2];

								if (doc.field("day") == null) {
									doc = new ODocument();
									addFieldToDoc(dayToDoc, hour, typeToDoc, doc);
								}
								line = line.replace(split[0], "");
								line = line.replace(split[1], "");
								line = line.replace(split[2], "");
								info = line;

							} catch (Exception e) {
								info = info.concat(line);
							}
						}

					} else {
						if (doc.field("day") != null) {
							addFieldToDoc(dayToDoc, hour, typeToDoc, doc);
							doc.field("info", info);
							if (info.contains(value)) {
								subdocuments.add(doc);
							}
						}
					}
				}

			}

		}

		iRequest.data.commandInfo = "Load log";

		result.field("logs", subdocuments);
		iResponse.writeRecord(result, null, "");

		return false;
	}

	private void addFieldToDoc(String dayToDoc, String hour, String typeToDoc, ODocument doc) {
		doc.field("day", dayToDoc);
		doc.field("hour", hour);
		doc.field("type", typeToDoc);
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

}
