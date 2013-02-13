package com.orientechnologies.common.log;

import java.util.logging.Level;
import java.util.logging.Logger;

public class OJulLogger implements OLogger {

	@Override
	public void log(String name, Level iLevel, String iMessage, Throwable iException,
			Object... iAdditionalArgs) {
		Logger log = Logger.getLogger(name);
		if (log.isLoggable(iLevel)) {
			final String msg = String.format(iMessage, iAdditionalArgs);
			if (iException != null)
				log.log(iLevel, msg, iException);
			else
				log.log(iLevel, msg);
		}
	}

}
