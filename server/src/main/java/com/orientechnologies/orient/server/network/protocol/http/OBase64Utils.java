package com.orientechnologies.orient.server.network.protocol.http;

import java.util.HashMap;

public class OBase64Utils extends java.util.prefs.AbstractPreferences {
	private HashMap<String, String>	encodedStore	= new HashMap<String, String>();

	public OBase64Utils(java.util.prefs.AbstractPreferences prefs, java.lang.String string) {
		super(prefs, string);
	}

	public java.lang.String encodeBase64(java.lang.String key, java.lang.String raw) throws java.io.UnsupportedEncodingException {
		java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
		java.io.PrintWriter pw = new java.io.PrintWriter(new java.io.OutputStreamWriter(baos, "UTF8"));
		pw.write(raw.toCharArray());
		pw.flush();// ya know
		byte[] rawUTF8 = baos.toByteArray();
		this.putByteArray(key, rawUTF8);

		return this.encodedStore.get(key);
	}

	public java.lang.String encodeBase64(java.lang.String raw) throws java.io.UnsupportedEncodingException {

		java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
		java.io.PrintWriter pw = new java.io.PrintWriter(new java.io.OutputStreamWriter(baos, "UTF8"));
		pw.write(raw.toCharArray());
		pw.flush();// ya know
		byte[] rawUTF8 = baos.toByteArray();
		this.putByteArray(raw, rawUTF8);

		return this.encodedStore.get(raw);
	}

	public java.lang.String decodeBase64(java.lang.String key, java.lang.String base64String)
			throws java.io.UnsupportedEncodingException, java.io.IOException {
		byte[] def = { (byte) 'D', (byte) 'E', (byte) 'F' };// not used at any point
		this.encodedStore.put(key, base64String);
		char[] platformChars = null;
		byte[] byteResults = null;
		byteResults = this.getByteArray(key, def);
		java.io.InputStreamReader isr = new java.io.InputStreamReader(new java.io.ByteArrayInputStream(byteResults), "UTF8");
		platformChars = new char[byteResults.length];
		isr.read(platformChars);

		return new java.lang.String(platformChars);
	}

	// intercept key lookup and return our own base64 encoded string to super
	@Override
	public String get(String key, String def) {
		return this.encodedStore.get(key);
	}

	// intercepts put captures the base64 encoded string and returns it
	@Override
	public void put(String key, String value) {
		this.encodedStore.put(key, value);// save the encoded string
	}

	// dummy implementation as AbstractPreferences is extended to get acces to protected
	// methods and to overide put(String,String) and get(String,String)
	@Override
	protected java.util.prefs.AbstractPreferences childSpi(String name) {
		return null;
	}

	@Override
	protected String[] childrenNamesSpi() throws java.util.prefs.BackingStoreException {
		return null;
	}

	@Override
	protected void flushSpi() throws java.util.prefs.BackingStoreException {
	}

	@Override
	protected String getSpi(String key) {
		return null;
	}

	@Override
	protected String[] keysSpi() throws java.util.prefs.BackingStoreException {
		return null;
	}

	@Override
	protected void putSpi(String key, String value) {
	}

	@Override
	protected void removeNodeSpi() throws java.util.prefs.BackingStoreException {
	}

	@Override
	protected void removeSpi(String key) {
	}

	@Override
	protected void syncSpi() throws java.util.prefs.BackingStoreException {
	}
}