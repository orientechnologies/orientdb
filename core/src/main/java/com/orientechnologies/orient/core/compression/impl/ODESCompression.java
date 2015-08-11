package com.orientechnologies.orient.core.compression.impl;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.serialization.OBase64Utils;

/***
 * @see https://github.com/orientechnologies/orientdb/issues/89
 * 
 * @see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html
 * 
 * @author giastfader
 *
 */
public class ODESCompression extends OAbstractEncryptedCompression {
	private byte[] key;
	
	//@see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
	private final String  transformation="DES/ECB/PKCS5Padding"; ////we use ECB because we cannot store the Initialization Vector 							 
	private final String secretKeyFactoryAlgorithmName="DES";
	private DESKeySpec desKeySpec;
	private SecretKeyFactory keyFactory;
	private SecretKey theKey;
	private boolean initialized;
	
	public static final ODESCompression INSTANCE = new ODESCompression();
	public static final String  NAME  = "des-encrypted";
   
	@Override
	public String name() {
		return NAME;
	}
	
	protected ODESCompression(){
		super();
	}
	
	protected void init(){
		initialized = false;
		try {
			key = OBase64Utils.decode(OGlobalConfiguration.STORAGE_ENCRYPTION_DES_KEY.getValueAsString());
			this.desKeySpec = new DESKeySpec(key);
			this.keyFactory = SecretKeyFactory.getInstance(secretKeyFactoryAlgorithmName);
			this.theKey = keyFactory.generateSecret(desKeySpec);
		} catch (InvalidKeyException e) {
			throw new OSecurityException("Invalid DES key.",e);
		} catch (NoSuchAlgorithmException e) {
			throw new OSecurityException("The DES alghorithm is not available on this platform",e);
		} catch (InvalidKeySpecException e) {
			throw new OSecurityException(e.getMessage(),e);
		}
		this.initialized=true;
	}

	public   byte[] encryptOrDecrypt(int mode, byte[] input, int offset, int length) throws Throwable {
		if (!initialized) throw new OSecurityException("des-encrypted compression is not available");
		
		Cipher cipher = Cipher.getInstance(transformation); 
		cipher.init(mode,theKey);
		
		byte[] content;
        if (offset==0 && length==input.length){
        	content=input;
        }else{
        	content = new byte[length];
	        System.arraycopy(input,offset,content,0,length);
        }
		byte[] output=cipher.doFinal(content);
		return output;
	}


	
}
