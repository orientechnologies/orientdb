package com.orientechnologies.orient.core.compression.impl;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
/**
 * @author giastfader@github
 * @since 21.04.2015
 */

public class OEncryptedCompression extends OAbstractEncryptedCompression{

	public static final OEncryptedCompression INSTANCE = new OEncryptedCompression();
	public static final String  NAME  = "key-encrypted";
   
	@Override
	public String name() {
		return NAME;
	}

	protected OEncryptedCompression() {
		super();    
		String key = OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getValueAsString();
		String charset = OGlobalConfiguration.STORAGE_ENCRYPTION_KEY_CHARSET.getValueAsString();
		if (!Charset.isSupported(charset)){
			super.setInitialized(false);
			throw new IllegalArgumentException(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY_CHARSET.getValueAsString() + " is not a valid charset for this platform");
		}
		try {
			super.init(
					OGlobalConfiguration.STORAGE_ENCRYPTION_TRANSFORMATION.getValueAsString(),
					OGlobalConfiguration.STORAGE_ENCRYPTION_ALGORITHM.getValueAsString(),
					OGlobalConfiguration.STORAGE_ENCRYPTION_SECRET_KEY_FACTORY_ALGORITHM.getValueAsString(),
					key.getBytes(charset)
			);
		} catch (UnsupportedEncodingException e) {
			//this is to make happy the Java compiler. We already done this check earlier
			throw new IllegalArgumentException(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY_CHARSET.getValueAsString() + " is not a valid charset for this platform"); 
		}
		super.setInitialized(true);
	}


}
