package com.tpin.utils;

import java.io.*;

/**
 * Created by Administrator on 2016/9/7 0007.
 */
public class CSVProcess {
    public static FileWriter openCsvFileHandle(String path,Boolean append) {
        FileWriter new_fw = null;
        try {

            //annotation of david:为了安全起见，使用append选项
            new_fw = new FileWriter(path, append);//考虑到编码格式

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new_fw;
    }

    public static void closeCsvFileHandle(FileWriter fw) {
        try {
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void saveAsCsvFile(FileWriter fw,String str) {
        try {
            fw.write(str + "\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static BufferedReader getFileReader(String path) {
        try {
            InputStreamReader read1 = null;//考虑到编码格式

            read1 = new InputStreamReader(
                    new FileInputStream(path));

            BufferedReader bufferedReader1 = new BufferedReader(read1);
           return bufferedReader1;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public  static String readToString(String fileName) {
        String encoding = "utf-8";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            System.err.println("The OS does not support " + encoding);
            e.printStackTrace();
            return null;
        }
    }
}
