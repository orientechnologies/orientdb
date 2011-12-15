package com.orientechnologies.common.log;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class OLogFormatter extends Formatter {

	private static final DateFormat	dateFormat	= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");

	/**
	 * The end-of-line character for this platform.
	 */
	private static final String			EOL					= System.getProperty("line.separator");

	@Override
	public String format(final LogRecord record) {
		if (record.getThrown() == null) {
			return customFormatMessage(record);
		}

		// FORMAT THE STACK TRACE
		final StringBuilder buffer = new StringBuilder();
		buffer.append(record.getMessage());

		Throwable current = record.getThrown();

		while (current != null) {
			buffer.append(EOL).append(current.getMessage());

			for (StackTraceElement stackTraceElement : record.getThrown().getStackTrace()) {
				buffer.append(EOL).append("-> ");
				buffer.append(stackTraceElement.toString());
			}
			current = current.getCause();
		}

		return buffer.toString();
	}

	private String customFormatMessage(final LogRecord iRecord) {
		Level iLevel = iRecord.getLevel();
		String iMessage = iRecord.getMessage();
		Object[] iAdditionalArgs = iRecord.getParameters();
		String iRequester = getSourceClassSimpleName(iRecord.getSourceClassName());

		final StringBuilder buffer = new StringBuilder();
		buffer.append(EOL);
		synchronized (dateFormat) {
			buffer.append(dateFormat.format(new Date()));
		}
		buffer.append(' ');
		buffer.append(iLevel.getName().substring(0, 4));
		if (iRequester != null) {
			buffer.append(" [");
			buffer.append(iRequester);
			buffer.append(']');
		}
		buffer.append(' ');

		// FORMAT THE MESSAGE
		try {
			buffer.append(String.format(iMessage, iAdditionalArgs));
		} catch (Exception e) {
			buffer.append(iMessage);
		}

		return buffer.toString();
	}

	private String getSourceClassSimpleName(final String iSourceClassName) {
		return iSourceClassName.substring(iSourceClassName.lastIndexOf(".") + 1);
	}
}
