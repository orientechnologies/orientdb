package com.orientechnologies.common.log;

import java.util.logging.Level;

public interface OLogger {


	void log(final String name, final Level iLevel, String iMessage,
			final Throwable iException, final Object... iAdditionalArgs);

}
