package com.orientechnologies.orient.core.compression.impl;

import java.io.UnsupportedEncodingException;
import java.security.AlgorithmParameters;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSecurityException;

/***
 * @see https://github.com/orientechnologies/orientdb/issues/89
 * 
 * @see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html
 * 
 * @author giastfader
 *
 */
public abstract class OAbstractEncryptedCompression extends OAbstractCompression {

	//these variables are initialized by the init() method
	protected PBEKeySpec spec;
	
	//@see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#ciphertrans
	
	//@see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
	protected String secretKeyFactoryAlgorithmName;		// es: "PBKDF2WithHmacSHA1"
	protected String transformation; 					// es: "AES/CBC/PKCS5Padding"

	//@see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
	protected String algorithmName;						// es: "AES" 
	
	protected SecretKeyFactory factory;
    protected SecretKey secretKey;
    
    private byte[] iv;
	
	//loaded from OGlobalConfiguration
	private  String password;;
	private  String salt;
	private  int  pswdIterations;
    
	    
    //http://stackoverflow.com/questions/24907530/java-security-invalidkeyexception-illegal-key-size-or-default-parameters-in-and
    //http://www.javamex.com/tutorials/cryptography/unrestricted_policy_files.shtml
    private  int keySize;
    
	protected OAbstractEncryptedCompression(){}
	
	/***
	 * Subclasses have to invoke this method next the constructor
	 * @param spec This object contains the password, the salt and the keysize. It is computed by subclasses
	 * @param transformation Example: AES/CBC/PKCS5Padding @see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#ciphertrans
	 * @param alghorithm Example: AES @see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
	 * @param secretKeyFactoryAlgorithmName Example: PBKDF2WithHmacSHA1 @see https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
	 */
	protected void init(
			String transformation,
			String algorithmName, 
			String secretKeyFactoryAlgorithmName,
			String password,
			String salt,
			int pswdIterations,
			int keySize){
		
		this.transformation=transformation;
		this.algorithmName=algorithmName;
		
		try {
			this.password = password;
			if (password==null) password = "";
			
			this.salt = salt;
			byte[] saltBytes = salt.getBytes("UTF-8");
			
			this.pswdIterations = pswdIterations;
			this.keySize = keySize;
			
			PBEKeySpec spec = new PBEKeySpec(
	                password.toCharArray(), 
	                saltBytes, 
	                pswdIterations, 
	                keySize
	                );
			this.spec = spec;
			this.factory = SecretKeyFactory.getInstance(secretKeyFactoryAlgorithmName);
			this.secretKey = factory.generateSecret(spec);
			
			SecretKeySpec secret = getSecretKeySpec();
			Cipher cipher = getCipher();
	        cipher.init(Cipher.ENCRYPT_MODE, secret);
			AlgorithmParameters params = cipher.getParameters();
	        iv = params.getParameterSpec(IvParameterSpec.class).getIV();
	        
		} catch (NoSuchAlgorithmException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (InvalidKeySpecException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (NoSuchPaddingException e) {
			throw new OSecurityException("The given 'salt' could be wrong (" + e.getMessage() + ")",e);
		} catch (InvalidParameterSpecException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (InvalidKeyException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (UnsupportedEncodingException e) { //UTF-8 not supported?... :\
			throw new IllegalArgumentException(e.getMessage(),e);
		}
	}
	
	protected Cipher getCipher() throws NoSuchAlgorithmException, NoSuchPaddingException{
		return Cipher.getInstance(transformation);
	}
	
	protected SecretKeySpec getSecretKeySpec(){
		return new SecretKeySpec(secretKey.getEncoded(),algorithmName);
	}
	
	
	@Override
	public  byte[] compress(byte[] content, int offset, int length){
        try {
	         
	        SecretKeySpec secret = getSecretKeySpec();
			Cipher cipher = getCipher();
	        cipher.init(Cipher.ENCRYPT_MODE, secret,new IvParameterSpec(iv));
	        
	        byte[] contentToEncrypt;
	        if (offset==0 && length==content.length){
	        	contentToEncrypt=content;
	        }else{
		        contentToEncrypt = new byte[length];
		        System.arraycopy(content,offset,contentToEncrypt,0,length);
	        }
	        byte[] encryptedTextBytes = cipher.doFinal(contentToEncrypt);
	        return encryptedTextBytes;
	        
		} catch (NoSuchAlgorithmException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (NoSuchPaddingException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (InvalidKeyException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (IllegalBlockSizeException e) {
			throw new IllegalArgumentException(e.getMessage(),e);
		} catch (BadPaddingException e) {
			throw new OSecurityException("The given 'salt' could be wrong (" + e.getMessage() + ")",e);
		} catch (InvalidAlgorithmParameterException e) {
			throw new OSecurityException(e.getMessage(),e);
		} 
	};

	@Override
	public  byte[] uncompress(byte[] content, int offset, int length){
        SecretKeySpec secret = getSecretKeySpec();

        try {
	        Cipher cipher = getCipher();
	        cipher.init(Cipher.DECRYPT_MODE, secret,new IvParameterSpec(iv));
	     
	        byte[] contentToDecrypt;
	        if (offset==0 && length==content.length){
	        	contentToDecrypt=content;
	        }else{
	        	contentToDecrypt = new byte[length];
		        System.arraycopy(content,offset,contentToDecrypt,0,length);
	        }
        	return cipher.doFinal(contentToDecrypt);
        } catch (NoSuchAlgorithmException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (NoSuchPaddingException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (InvalidKeyException e) {
			throw new OSecurityException(e.getMessage(),e);
		} catch (IllegalBlockSizeException e) {
			throw new IllegalArgumentException(e.getMessage(),e);
		} catch (BadPaddingException e) {
			throw new OSecurityException("The given 'salt' could be wrong (" + e.getMessage() + ")",e);
		}  catch (InvalidAlgorithmParameterException e) {
			throw new OSecurityException(e.getMessage(),e);
		}
	};

	
	@Override
	public abstract String name();

	
}
