package com.orientechnologies.orient.core.db.object;

@SuppressWarnings("serial")
public class OObjectNotDetachedException extends RuntimeException {

	public OObjectNotDetachedException() {
		super();
	}

	public OObjectNotDetachedException(String message, Throwable cause) {
		super(message, cause);
	}

	public OObjectNotDetachedException(String message) {
		super(message);
	}

	public OObjectNotDetachedException(Throwable cause) {
		super(cause);
	}

}
