package com.orientechnologies.common.log;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class OLogFormatter extends Formatter {

	@Override
	public String format(final LogRecord record) {
		return record.getMessage();
	}

}
