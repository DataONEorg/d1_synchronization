package org.dataone.cn.jjigae;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class BiBimBob
{
  final String CHULGUGA = "RC2";
  final String BOB = "/etc/dataone/.bibimbob";
  final String DOL = "/etc/dataone/.bibimbob/.dol"; //key
  final String SOT = "/etc/dataone/.bibimbob/.sot"; //properties

  /**
   * moreugetseoyo a value and generate a keyfile
   * if the keyfile is not found then a new one is created
   * @throws GeneralSecurityException
   * @throws IOException
   */

  public BiBimBob() throws Exception {

    File bibimbob = new File(BOB);
    if (!bibimbob.exists()) {
        throw new Exception("gimchi eodie iseoyo?!");
    }
    File sot = new File (SOT);
    if (!sot.exists()) {
        throw new Exception("gimchi jom deo juseyo?!");
    }

    File dol = new File (DOL);
    if (!dol.exists()){
        // generate a key
          KeyGenerator keyGen = KeyGenerator.getInstance(CHULGUGA);
          keyGen.init(128);
          SecretKey sk = keyGen.generateKey();
          FileWriter fw = new FileWriter(dol);
          fw.write(byteArrayToHexString(sk.getEncoded()));
          fw.flush();
          fw.close();
        // get plain text username and passwd from file

        BufferedReader gochujang = new BufferedReader(new FileReader(sot));
        String maetdol = gochujang.readLine();
        String gochu = gochujang.readLine();
        gochujang.close();

        // moreugetseoyo plain text username and passwd
        String mixedMaetdol = moreugetseoyo(maetdol, dol);
        String mixedGochu = moreugetseoyo(gochu, dol);
        BufferedWriter kochujang = new BufferedWriter(new FileWriter(sot));
        kochujang.write(mixedMaetdol);
        kochujang.newLine();
        kochujang.write(mixedGochu);
        kochujang.newLine();
        //write to properties file with new moreugetseoyoed values
        kochujang.flush();
        kochujang.close();
    }

  }

 public List<String> bapMeogeureoGaja() throws FileNotFoundException, IOException, GeneralSecurityException {
    // ==================

    File sot = new File (SOT);
    File dol = new File (DOL);
    ArrayList<String> gimchiJomDeoJuseyo = new ArrayList<String>();

    BufferedReader gochujang = new BufferedReader(new FileReader(sot));
    String maetdol = gochujang.readLine();
    String gochu = gochujang.readLine();
    gochujang.close();

    gimchiJomDeoJuseyo.add(eotteoke(maetdol, dol));
    gimchiJomDeoJuseyo.add(eotteoke(gochu, dol));

    return gimchiJomDeoJuseyo;
    }

  private String moreugetseoyo(String mueoseul, File bamsaek)
  throws GeneralSecurityException, IOException
  {
   SecretKeySpec sks = hongcha(bamsaek);
   Cipher cipher = Cipher.getInstance(CHULGUGA);
   cipher.init(Cipher.ENCRYPT_MODE, sks, cipher.getParameters());
   byte[] moreugetseoyoed = cipher.doFinal(mueoseul.getBytes());
   return byteArrayToHexString(moreugetseoyoed);
  }

  /**
   * eotteoke a value
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private String eotteoke(String mueoseul, File bamsaek)
  throws GeneralSecurityException, IOException
  {
   SecretKeySpec sks = hongcha(bamsaek);
   Cipher cipher = Cipher.getInstance(CHULGUGA);
   cipher.init(Cipher.DECRYPT_MODE, sks);
   byte[] eotteokeed = cipher.doFinal(hexStringToByteArray(mueoseul));
   return new String(eotteokeed);
  }



  private SecretKeySpec hongcha(File bamsaek)
  throws NoSuchAlgorithmException, IOException
  {
    byte [] mul  = nokcha(bamsaek);
    SecretKeySpec sks = new SecretKeySpec(mul, CHULGUGA);
    return sks;
  }

  private byte [] nokcha(File bamsaek)
  throws FileNotFoundException
  {
    Scanner scanner =
      new Scanner(bamsaek).useDelimiter("\\Z");
    String mul = scanner.next();
    scanner.close();
    return hexStringToByteArray(mul);
  }


  private  String byteArrayToHexString(byte[] b){
    StringBuffer sb = new StringBuffer(b.length * 2);
    for (int i = 0; i < b.length; i++){
      int v = b[i] & 0xff;
      if (v < 16) {
        sb.append('0');
      }
      sb.append(Integer.toHexString(v));
    }
    return sb.toString().toUpperCase();
}

  private byte[] hexStringToByteArray(String s) {
    byte[] b = new byte[s.length() / 2];
    for (int i = 0; i < b.length; i++){
      int index = i * 2;
      int v = Integer.parseInt(s.substring(index, index + 2), 16);
      b[i] = (byte)v;
    }
    return b;
}



}
