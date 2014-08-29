package com.orientechnologies.common.log;

import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSlf4JLogger implements OLogger {

	@Override
	public void log(String name, Level iLevel, String iMessage,
			Throwable iException, Object... iAdditionalArgs) {
		Logger logger = LoggerFactory.getLogger(name);

		if (canLog(logger, iLevel)) {
			String msg = String.format(iMessage, iAdditionalArgs);
			logger.info(msg, iException);

		}

	}

	private boolean canLog(Logger logger, Level iLevel) {
		return (Level.INFO == iLevel && logger.isInfoEnabled()
				|| Level.WARNING == iLevel && logger.isWarnEnabled()
				|| Level.SEVERE == iLevel && logger.isErrorEnabled() 
				|| (Level.FINE == iLevel || Level.FINER == iLevel || Level.FINEST == iLevel) && logger.isDebugEnabled());
	}
}
