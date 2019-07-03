package com.ally.db.util;

import lombok.extern.slf4j.Slf4j;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Slf4j
public final class HashUtil {

    private static final String SHA_256 = "SHA-256";

    public static String getSHA256Hash(byte[] data) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(SHA_256);
            byte[] hash = digest.digest(data);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            log.error(e.getMessage());
            System.exit(-1);
        }

        return null;
    }

    private static String bytesToHex(byte[] hash) {
        return DatatypeConverter.printHexBinary(hash);
    }

}
