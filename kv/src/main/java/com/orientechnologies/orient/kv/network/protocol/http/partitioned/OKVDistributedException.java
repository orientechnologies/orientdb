package com.orientechnologies.orient.kv.network.protocol.http.partitioned;

import com.orientechnologies.common.exception.OException;

@SuppressWarnings("serial")
public class OKVDistributedException extends OException {

	public OKVDistributedException() {
		super();
	}

	public OKVDistributedException(String message, Throwable cause) {
		super(message, cause);
	}

	public OKVDistributedException(String message) {
		super(message);
	}

	public OKVDistributedException(Throwable cause) {
		super(cause);
	}
}
