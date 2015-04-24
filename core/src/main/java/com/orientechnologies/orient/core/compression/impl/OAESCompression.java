package com.orientechnologies.orient.core.compression.impl;

import java.util.Arrays;

import javax.crypto.Cipher;
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
public class OAESCompression extends OAbstractCompression {
	
	
	//@see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
	protected  String  transformation="AES/ECB/PKCS5Padding";
	protected  String  algorithmName="AES";
	byte[] key = OBase64Utils.decode(OGlobalConfiguration.STORAGE_ENCRYPTION_AES_KEY.getValueAsString());
	
    private  boolean isInitialized=false;

    
    public static final OAESCompression INSTANCE = new OAESCompression();
	public static final String  NAME  = "aes-encrypted";
   
	@Override
	public String name() {
		return NAME;
	}
	
    protected boolean isInitialized() {
		return isInitialized;
	}

	protected void setInitialized(boolean isInitialized) {
		this.isInitialized = isInitialized;
	}
	
	protected OAESCompression(){
		this.init();
	}
	
	protected void init()  {

	}

	
	
	@Override
	public  byte[] compress(byte[] content, int offset, int length){
        try {
	        
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
		SecretKeySpec ks = new SecretKeySpec(key, algorithmName); //AES
		//SecretKey desKey = skf.generateSecret(ks.getEncoded()); 
		Cipher cipher = Cipher.getInstance(transformation); // AES/CBC/PKCS5Padding for SunJCE
		cipher.init(mode, ks);
		
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
