package com.orientechnologies.common.log;

import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSlf4JLogger implements OLogger {

	@Override
	public void log(String name, Level iLevel, String iMessage,
			Throwable iException, Object... iAdditionalArgs) {
		Logger logger = LoggerFactory.getLogger(name);

		if (Level.INFO == iLevel) {
			if (logger.isInfoEnabled()) {
				String msg = String.format(iMessage, iAdditionalArgs);
				logger.info(msg, iException);
			}
		} else if (Level.WARNING == iLevel) {
			if (logger.isWarnEnabled()) {
				String msg = String.format(iMessage, iAdditionalArgs);
				logger.info(msg, iException);
			}
		} else if (Level.SEVERE == iLevel) {
			if (logger.isErrorEnabled()) {
				String msg = String.format(iMessage, iAdditionalArgs);
				logger.info(msg, iException);
			}
		} else if (Level.FINE == iLevel || Level.FINER == iLevel
				|| Level.FINEST == iLevel) {
			if (logger.isDebugEnabled()) {
				String msg = String.format(iMessage, iAdditionalArgs);
				logger.info(msg, iException);
			}
		}

	}

}
