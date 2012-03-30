package com.orientechnologies.orient.server.clustering;

import java.util.logging.Level;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;

public class OClusterLogger {
	public enum TYPE {
		CLUSTER, REPLICATION
	}

	public enum DIRECTION {
		NONE, IN, OUT
	}

	private String										node;
	private String										database;

	private final static OLogManager	logger	= OLogManager.instance();

	public void log(final Object iRequester, final Level iLevel, final TYPE iType, final DIRECTION iDirection, final String iMessage,
			final Object... iAdditionalArgs) {
		log(iRequester, iLevel, iType, iDirection, iMessage, null, iAdditionalArgs);
	}

	public void log(final Object iRequester, final Level iLevel, final TYPE iType, final DIRECTION iDirection, final String iMessage,
			final Throwable iException, final Object... iAdditionalArgs) {
		if (logger.isLevelEnabled(iLevel))
			logger.log(iRequester, iLevel, formatMessage(iType, iDirection, iMessage), iException, iAdditionalArgs);
	}

	public void error(final Object iRequester, final TYPE iType, final DIRECTION iDirection, final String iMessage,
			final Throwable iException, final Class<? extends OException> iExceptionClass, final Object... iAdditionalArgs) {
		logger.error(iRequester, formatMessage(iType, iDirection, iMessage), null, iExceptionClass, iAdditionalArgs);
	}

	public String getNode() {
		return node;
	}

	public OClusterLogger setNode(String node) {
		this.node = node;
		return this;
	}

	public String getDatabase() {
		return database;
	}

	public OClusterLogger setDatabase(String database) {
		this.database = database;
		return this;
	}

	protected String formatMessage(final TYPE iType, final DIRECTION iDirection, final String iMessage) {
		final StringBuilder buffer = new StringBuilder();
		if (iType == TYPE.CLUSTER)
			buffer.append("CLUS");
		else if (iType == TYPE.REPLICATION)
			buffer.append("REPL");

		if (node != null) {
			if (iDirection == DIRECTION.IN)
				buffer.append("<-");
			else if (iDirection == DIRECTION.OUT)
				buffer.append("->");
			else
				buffer.append("--");

			if (node != null) {
				buffer.append('{');
				buffer.append(node);
				buffer.append('}');
			}
		}

		if (database != null) {
			buffer.append(" (");
			buffer.append(database);
			buffer.append(')');
		}

		buffer.append(' ');
		buffer.append(iMessage);
		return buffer.toString();
	}
}
