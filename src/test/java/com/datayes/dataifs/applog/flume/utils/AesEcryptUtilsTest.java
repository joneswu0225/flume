package com.datayes.dataifs.applog.flume.utils;

import org.junit.Test;

/**
 * @author qihai.su
 */
public class AesEcryptUtilsTest {
	@Test
	public void test(){
		String source = "www.test.com";
		String ecrypted = AesEcryptUtils.encrypt(source);
		assert(!ecrypted.equals(source));
		
		String decrypted = AesEcryptUtils.decrypt(ecrypted);
		assert(source.equals(decrypted));
	}
}
