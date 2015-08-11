package com.orientechnologies.orient.core.compression.impl;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

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
public class OAESCompression extends OAbstractEncryptedCompression {
	private byte[] key;
	
	//@see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
	private final String  transformation="AES/ECB/PKCS5Padding"; //we use ECB because we cannot store the Initialization Vector 
	private final String  algorithmName="AES";
	

	private boolean initialized;

    
    public static final OAESCompression INSTANCE = new OAESCompression();
	public static final String  NAME  = "aes-encrypted";
   
	@Override
	public String name() {
		return NAME;
	}
	
	protected OAESCompression(){
		super();
	}
	
	protected void init()  {
		initialized=false;
		key = OBase64Utils.decode(OGlobalConfiguration.STORAGE_ENCRYPTION_AES_KEY.getValueAsString());
		SecretKeySpec ks = new SecretKeySpec(key, algorithmName); //AES
		try {
			Cipher cipher = Cipher.getInstance(transformation);
			cipher.init(Cipher.ENCRYPT_MODE, ks);
		} catch (NoSuchAlgorithmException e) {
			throw new OSecurityException("The AES alghorithm is not available on this platform",e);
		} catch (NoSuchPaddingException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (InvalidKeyException e) {
			throw new OSecurityException("Invalid AES key.",e);
		}
		this.initialized=true;
	}

	public   byte[] encryptOrDecrypt(int mode, byte[] input, int offset, int length) throws Throwable {
		if (!initialized) throw new OSecurityException("aes-encrypted compression is not available");
		
		SecretKeySpec ks = new SecretKeySpec(key, algorithmName); 
		Cipher cipher = Cipher.getInstance(transformation); 
		cipher.init(mode, ks);
		
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
