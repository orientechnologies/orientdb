package com.orientechnologies.orient.core.compression.impl;

import javax.crypto.Cipher;

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
	
	@Override
	public abstract String name();
	
	protected OAbstractEncryptedCompression(){
		this.init();
	}

	protected abstract void init();

	
	@Override
	public  byte[] compress(byte[] content, int offset, int length){
        try {
	        byte[] encriptedContent = encryptOrDecrypt(Cipher.ENCRYPT_MODE,content, offset,  length);
	        return encriptedContent;
        } catch (Throwable e) {
			throw new OSecurityException(e.getMessage(),e);
		} 
	};

	@Override
	public  byte[] uncompress(byte[] content, int offset, int length){
        try {
	        byte[] decriptedContent = encryptOrDecrypt(Cipher.DECRYPT_MODE,content, offset,  length);
	        return decriptedContent;
        } catch (Throwable e) {
			throw new OSecurityException(e.getMessage(),e);
		} 
	};

	/***
	 * 
	 * @param mode it can be Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE
	 * @param input
	 * @param offset
	 * @param length
	 * @return
	 * @throws Throwable
	 */
	public abstract byte[] encryptOrDecrypt(int mode, byte[] input, int offset, int length) throws Throwable;

}
