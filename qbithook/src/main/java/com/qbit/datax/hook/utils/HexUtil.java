package com.qbit.datax.hook.utils;

import java.util.Formatter;

/**
 * @author litao
 */
public class HexUtil {
    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        Formatter formatter = new Formatter(sb);
        for (byte b : bytes) {
            formatter.format("%02x", b);
        }
        //完成16进制编码
        return sb.toString();
    }
}
