package com.orientechnologies.orient.kv.network.protocol.http.partitioned;

import com.orientechnologies.common.exception.OException;

@SuppressWarnings("serial")
public class ODistributedException extends OException {

	public ODistributedException() {
		super();
	}

	public ODistributedException(String message, Throwable cause) {
		super(message, cause);
	}

	public ODistributedException(String message) {
		super(message);
	}

	public ODistributedException(Throwable cause) {
		super(cause);
	}
}
