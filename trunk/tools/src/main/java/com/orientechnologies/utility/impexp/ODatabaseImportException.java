package com.orientechnologies.utility.impexp;

@SuppressWarnings("serial")
public class ODatabaseImportException extends RuntimeException {

	public ODatabaseImportException() {
		super();
	}

	public ODatabaseImportException(String message, Throwable cause) {
		super(message, cause);
	}

	public ODatabaseImportException(String message) {
		super(message);
	}

	public ODatabaseImportException(Throwable cause) {
		super(cause);
	}
}
