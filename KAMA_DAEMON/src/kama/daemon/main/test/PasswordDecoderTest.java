package kama.daemon.main.test;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class PasswordDecoderTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
		encryptor.setPassword("pwkey");
		
		System.out.println(encryptor.decrypt("2CqMBEKV6hNypkYo3UgaIaWn2O3sh1eN"));
		 
	}

}
