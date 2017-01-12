package com.orientechnologies.orient.core.security.symmetrickey;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKey;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author S. Colin Leister
 * 
 */
public class OSymmetricKeyTest {

  private void command(ODatabaseDocumentTx db, String sql, Object ... params) {
    db.command(new OCommandSQL(sql)).execute(params);
  }

  @Test
  public void shouldTestDefaultConstructor() throws Exception {
  	 OSymmetricKey sk = new OSymmetricKey();

    String msgToEncrypt = "Please, encrypt this!";

  	 String magic = sk.encrypt(msgToEncrypt);
  	 
  	 String decryptedMsg = sk.decryptAsString(magic);
  	 
  	 assertThat(msgToEncrypt).isEqualTo(decryptedMsg);
  }

  @Test
  public void shouldTestSpecificAESKey() throws Exception {
  	 OSymmetricKey sk = new OSymmetricKey("AES", "8BC7LeGkFbmHEYNTz5GwDw==");

    String msgToEncrypt = "Please, encrypt this!";

  	 String magic = sk.encrypt("AES/CBC/PKCS5Padding", msgToEncrypt);
  	 
  	 String decryptedMsg = sk.decryptAsString(magic);
  	 
  	 assertThat(msgToEncrypt).isEqualTo(decryptedMsg);
  }

  @Test
  public void shouldTestGeneratedAESKey() throws Exception {
  	 OSymmetricKey sk = new OSymmetricKey("AES", "AES/CBC/PKCS5Padding", 128);

    String key = sk.getBase64Key();

    String msgToEncrypt = "Please, encrypt this!";

  	 String magic = sk.encrypt(msgToEncrypt);
  	 
  	 OSymmetricKey sk2 = new OSymmetricKey("AES", key);
  	 
  	 String decryptedMsg = sk2.decryptAsString(magic);
  	 
  	 assertThat(msgToEncrypt).isEqualTo(decryptedMsg);
  }

  @Test
  public void shouldTestOSymmetricKeySecurity() throws Exception {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + OSymmetricKeyTest.class.getSimpleName());
    
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    final String user = "test";

    command(db, "insert into OUser set name=?, password='password', status='ACTIVE', roles=(SELECT FROM ORole WHERE name = ?)", user, "admin");
    command(db, "update OUser set properties={'@type':'d', 'key':'8BC7LeGkFbmHEYNTz5GwDw==','keyAlgorithm':'AES'} where name = ?", user);
    
    db.close();
    
    db.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSymmetricKeySecurity.class);
    
    OSymmetricKey sk = new OSymmetricKey("AES", "8BC7LeGkFbmHEYNTz5GwDw==");
    
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    db.open(user, sk.encrypt("AES/CBC/PKCS5Padding", user));
    db.close();
  }
}
