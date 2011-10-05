package com.orientechnologies.orient.server.handler;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

public class OAutomaticBackup extends OServerHandlerAbstract {
	public enum VARIABLES {
		DBNAME, DATE
	}

	private long				delay							= -1;
	private String			targetDirectory		= "backup";
	private String			targetFileName;
	private Set<String>	includeDatabases	= new HashSet<String>();
	private Set<String>	excludeDatabases	= new HashSet<String>();

	@Override
	public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
		for (OServerParameterConfiguration param : iParams) {
			if (param.name.equalsIgnoreCase("enabled")) {
				if (!Boolean.parseBoolean(param.value))
					// DISABLE IT
					return;
			} else if (param.name.equalsIgnoreCase("delay"))
				delay = OIOUtils.getTimeAsMillisecs(param.value);
			else if (param.name.equalsIgnoreCase("target.directory"))
				targetDirectory = param.value;
			else if (param.name.equalsIgnoreCase("db.include") && param.value.trim().length() > 0)
				for (String db : param.value.split(","))
					includeDatabases.add(db);
			else if (param.name.equalsIgnoreCase("db.exclude") && param.value.trim().length() > 0)
				for (String db : param.value.split(","))
					excludeDatabases.add(db);
			else if (param.name.equalsIgnoreCase("target.fileName"))
				targetFileName = param.value;
		}

		if (delay <= 0)
			throw new OConfigurationException("Can't find mandatory parameter 'delay'");
		if (!targetDirectory.endsWith("/"))
			targetDirectory += "/";

		final File filePath = new File(targetDirectory);
		if (filePath.exists()) {
			if (!filePath.isDirectory())
				throw new OConfigurationException("Parameter 'path' points to a file, not a directory");
		} else
			// CREATE BACKUP FOLDER(S) IF ANY
			filePath.mkdirs();

		OLogManager.instance().info(this, "Automatic backup handler installed and active: delay=%dms, targetDirectory=%s", delay,
				targetDirectory);

		Orient.getTimer().schedule(new TimerTask() {

			@Override
			public void run() {
				final Map<String, String> databaseNames = OServerMain.server().getAvailableStorageNames();
				for (final Entry<String, String> dbName : databaseNames.entrySet()) {
					boolean include;

					if (includeDatabases.size() > 0)
						include = includeDatabases.contains(dbName.getKey());
					else
						include = true;

					if (excludeDatabases.contains(dbName.getKey()))
						include = false;

					if (include) {
						final String fileName = OVariableParser.resolveVariables(targetFileName, OSystemVariableResolver.VAR_BEGIN,
								OSystemVariableResolver.VAR_END, new OVariableParserListener() {
									@Override
									public String resolve(final String iVariable) {
										if (iVariable.equalsIgnoreCase(VARIABLES.DBNAME.toString()))
											return dbName.getKey();
										else if (iVariable.startsWith(VARIABLES.DATE.toString())) {
											return new SimpleDateFormat(iVariable.substring(VARIABLES.DATE.toString().length() + 1)).format(new Date());
										}

										// NOT FOUND
										throw new IllegalArgumentException("Variable '" + iVariable + "' wasn't found");
									}
								});

						final String exportFilePath = targetDirectory + fileName;
						final ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbName.getValue());

						try {
							db.setProperty(ODatabase.OPTIONS.SECURITY.toString(), Boolean.FALSE);
							db.open("admin", "aaa");

							new ODatabaseExport(db, exportFilePath, new OCommandOutputListener() {
								@Override
								public void onMessage(final String iText) {

								}
							}).exportDatabase();

						} catch (IOException e) {
							OLogManager.instance().error(this,
									"[OAutomaticBackup] Error on exporting database '" + dbName.getValue() + "' to file: " + exportFilePath, e);
						} finally {
							db.close();
						}
					}
				}
			}
		}, delay, delay);
	}

	@Override
	public String getName() {
		return "automaticBackup";
	}
}
