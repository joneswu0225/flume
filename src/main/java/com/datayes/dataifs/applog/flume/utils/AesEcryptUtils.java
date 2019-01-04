/** 
 * 通联数据机密
 * --------------------------------------------------------------------
 * 通联数据股份公司版权所有 © 2013-1016
 * 
 * 注意：本文所载所有信息均属于通联数据股份公司资产。本文所包含的知识和技术概念均属于
 * 通联数据产权，并可能由中国、美国和其他国家专利或申请中的专利所覆盖，并受商业秘密或
 * 版权法保护。
 * 除非事先获得通联数据股份公司书面许可，严禁传播文中信息或复制本材料。
 * 
 * DataYes CONFIDENTIAL
 * --------------------------------------------------------------------
 * Copyright © 2013-2016 DataYes, All Rights Reserved.
 * 
 * NOTICE: All information contained herein is the property of DataYes 
 * Incorporated. The intellectual and technical concepts contained herein are 
 * proprietary to DataYes Incorporated, and may be covered by China, U.S. and 
 * Other Countries Patents, patents in process, and are protected by trade 
 * secret or copyright law. 
 * Dissemination of this information or reproduction of this material is 
 * strictly forbidden unless prior written permission is obtained from DataYes.
 */
package com.datayes.dataifs.applog.flume.utils;

import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qihai.su
 * 加解密工具类
 */
public class AesEcryptUtils {
	private static final Logger LOG = LoggerFactory.getLogger(AesEcryptUtils.class);
	private static final String KEY = "datayes@";
	private static final String algorithmStr="AES";  
    
	private static KeyGenerator keyGen;  
      
	private static Cipher cipher;  
      
    private static boolean isInited=false;  
      
    //初始化  
    private static void init()  
    {  
          
        //初始化keyGen  
//        try {  
//            keyGen=KeyGenerator.getInstance("AES");  
//            keyGen.init(128, new SecureRandom(KEY.getBytes()));  
//            cipher=Cipher.getInstance(algorithmStr);  
//            isInited=true;  
//        } catch (Exception e){
//        	LOG.error("", e);
//        	e.printStackTrace();
//        }  
    }  
      
    private static byte[] genKey()  
    {  
        if(!isInited)//如果没有初始化过,则初始化  
        {  
            init();  
        }  
        return keyGen.generateKey().getEncoded();  
    }  
      
    public static String encrypt(String content){
    	try{
	    	KeyGenerator kgen = KeyGenerator.getInstance("AES");  
	    	SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG" );  
            secureRandom.setSeed(KEY.getBytes());
			kgen.init(128, secureRandom);
	        SecretKey secretKey = kgen.generateKey();  
	        byte[] enCodeFormat = secretKey.getEncoded();  
	        SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");  
	        Cipher cipher = Cipher.getInstance("AES");// 创建密码器   
	        cipher.init(Cipher.ENCRYPT_MODE, key);// 初始化  
	        byte[] result = cipher.doFinal(content.getBytes("UTF-8"));
	        return Base64.getEncoder().encodeToString(result);  
    	}catch(Exception e){
    		LOG.error("", e);
    	}
    	return null;
    }
    
    private static String encryptA(String content)  
    {    
        if(!isInited)//为初始化  
        {  
            init();  
        }  

        try {  
        	byte[] enCodeFormat = genKey();
        	SecretKeySpec key=new SecretKeySpec(enCodeFormat, "AES");  
            cipher.init(Cipher.ENCRYPT_MODE, key);  
            byte[] encryptedText=cipher.doFinal(content.getBytes("UTF-8"));
            return Base64.getEncoder().encodeToString(encryptedText);
        } catch (Exception e) {  
            LOG.error("", e); 
            e.printStackTrace();
        }  
          
        return null;  
    }  
      
    //解密为byte[]  
    private static String decryptA(String content){  
        if(!isInited){  
            init();  
        }  
        try {  
            byte[] enCodeFormat = genKey();
        	SecretKeySpec key=new SecretKeySpec(enCodeFormat,"AES");  
            cipher.init(Cipher.DECRYPT_MODE, key);  
            byte[] originBytes=cipher.doFinal(Base64.getDecoder().decode(content));
            return new String(originBytes);
        } catch (Exception e) {  
            LOG.error("", e);
            e.printStackTrace();
        }  
          
        return null;  
    }
    
	public static String decrypt(String content) {
		try {
			KeyGenerator kgen = KeyGenerator.getInstance("AES");
			SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG" );  
            secureRandom.setSeed(KEY.getBytes());
			kgen.init(128, secureRandom);
			SecretKey secretKey = kgen.generateKey();
			byte[] enCodeFormat = secretKey.getEncoded();
			SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");
			Cipher cipher = Cipher.getInstance("AES");// 创建密码器
			cipher.init(Cipher.DECRYPT_MODE, key);// 初始化
			byte[] result = cipher.doFinal(Base64.getDecoder().decode(content));
			return new String(result, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("", e);
		}
		return null;
	}
    
	public static void main(String[] args){
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long now = System.currentTimeMillis();
//		System.out.println(format.format(new Date(1487907000000L)));
		System.out.println(now);
	}
}
