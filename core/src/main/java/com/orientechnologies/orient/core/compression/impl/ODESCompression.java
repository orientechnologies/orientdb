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
public class ODESCompression extends OAbstractCompression {

	
	//@see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
	protected  String  transformation="DES/ECB/PKCS5Padding"; 										 
	private String secretKeyFactoryAlgorithmName="DES";
	byte[] key = OBase64Utils.decode(OGlobalConfiguration.STORAGE_ENCRYPTION_DES_KEY.getValueAsString());
	private DESKeySpec desKeySpec;
	private SecretKeyFactory keyFactory;
	private SecretKey theKey;
	private boolean initialized = false;
	
	public static final ODESCompression INSTANCE = new ODESCompression();
	public static final String  NAME  = "des-encrypted";
   
	@Override
	public String name() {
		return NAME;
	}
	
	protected ODESCompression(){
		this.init();
	}
	
	protected void init(){
		try {
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

	
	@Override
	public  byte[] compress(byte[] content, int offset, int length){
        try {
	        if (!initialized) throw new OSecurityException("des-encrypted compression is not available");
	        System.out.println("** ENCRYPTION **");
	        System.out.println("** content: " + Arrays.toString(content));
	        System.out.println("** offset: " + offset);
	        System.out.println("** length: " + length);
	        
	        byte[] encriptedContent = encryptOrDecrypt(Cipher.ENCRYPT_MODE,content, offset,  length);
	        System.out.println("** encriptedContent: " + Arrays.toString(encriptedContent));
	        
	        return encriptedContent;
        } catch (Throwable e) {
			throw new OSecurityException(e.getMessage(),e);
		} 
	};

	@Override
	public  byte[] uncompress(byte[] content, int offset, int length){
        try {
	        System.out.println("** DECRYPTION **");
	        System.out.println("** content: " + Arrays.toString(content));
	        System.out.println("** offset: " + offset);
	        System.out.println("** length: " + length);
	        
	        byte[] decriptedContent = encryptOrDecrypt(Cipher.DECRYPT_MODE,content, offset,  length);
	        System.out.println("** decriptedContent: " + Arrays.toString(decriptedContent));

	        return decriptedContent;
        } catch (Throwable e) {
			throw new OSecurityException(e.getMessage(),e);
		} 
	};

	public   byte[] encryptOrDecrypt(int mode, byte[] input, int offset, int length) throws Throwable {
		if (!initialized) throw new OSecurityException("des-encrypted compression is not available");
		Cipher cipher = Cipher.getInstance(transformation); // DES/ECB/PKCS5Padding for SunJCE
		cipher.init(mode,theKey);
		
		byte[] content;
        if (offset==0 && length==input.length){
        	content=input;
        }else{
        	content = new byte[length];
	        System.arraycopy(input,offset,content,0,length);
	        System.out.println("** content: " + Arrays.toString(content));
        }
		byte[] output=cipher.doFinal(content);
		return output;
	}


	
}
