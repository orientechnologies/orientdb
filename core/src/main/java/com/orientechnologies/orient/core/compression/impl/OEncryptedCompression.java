package com.orientechnologies.orient.core.compression.impl;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
/**
 * @author giastfader@github
 * @since 21.04.2015
 */

public class OEncryptedCompression extends OAbstractEncryptedCompression{

	public static final OEncryptedCompression INSTANCE = new OEncryptedCompression();
	public static final String  NAME  = "encrypted";
   
	@Override
	public String name() {
		return NAME;
	}

	protected OEncryptedCompression() {
		super();    
		super.init(
				OGlobalConfiguration.STORAGE_ENCRYPTION_TRANSFORMATION.getValueAsString(),
				OGlobalConfiguration.STORAGE_ENCRYPTION_ALGORITHM.getValueAsString(),
				OGlobalConfiguration.STORAGE_ENCRYPTION_SECRET_KEY_ALGORITHM.getValueAsString(),
				OGlobalConfiguration.STORAGE_ENCRYPTION_PASSWORD.getValueAsString(),
				OGlobalConfiguration.STORAGE_ENCRYPTION_SALT.getValueAsString(),
				OGlobalConfiguration.STORAGE_ENCRYPTION_ITERATIONS.getValueAsInteger(),
				OGlobalConfiguration.STORAGE_ENCRYPTION_KEYSIZE.getValueAsInteger()
		);
	}


}
