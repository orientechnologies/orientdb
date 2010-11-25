package com.orientechnologies.common.log;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class OLogFormatter extends Formatter {

	@Override
	public String format(final LogRecord record) {
		if (record.getThrown() == null)
			return record.getMessage();

		// FORMAT THE STACK TRACE
		final StringBuilder buffer = new StringBuilder();
		buffer.append(record.getMessage());

		Throwable current = record.getThrown();

		while (current != null) {
			buffer.append("\n" + current.getMessage());

			for (StackTraceElement stackTraceElement : record.getThrown().getStackTrace()) {
				buffer.append("\n-> ");
				buffer.append(stackTraceElement.toString());
			}
			current = current.getCause();
		}

		return buffer.toString();
	}
}
