package com.cloudyoung.bigdata.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *   文件操作
 */
public class FileUtils {

    /**
     *  读取文件所有内容
     * @param fileName
     * @return
     */
    public static byte[] readFile(String fileName){
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] fileContent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(fileContent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileContent;
    }

}
