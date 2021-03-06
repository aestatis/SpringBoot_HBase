package com.springboot.hbase.service;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import javafx.util.Pair;
import org.assertj.core.internal.Bytes;
import org.junit.jupiter.api.Test;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class DateBaseInitTest {

    @Test
    public void testGeoHash() {
        GeoHash geoHash = GeoHash.withBitPrecision(31.271127, 120.941541, 60);
        System.out.println("bits: " + geoHash.toBinaryString());
        System.out.println("base32: " + geoHash.toBase32());

        GeoHash geoHash2 = GeoHash.withBitPrecision(30.889603, 121.874054, 60);
        System.out.println("bits: " + geoHash2.toBinaryString());
        System.out.println("base32: " + geoHash2.toBase32());
    }

    @Test
    public void testGenerateGeoHashFromString() {
        String str = "wtw3930";
        GeoHash geoHash = GeoHash.fromGeohashString(str);
        System.out.println(geoHash.toBinaryString());
    }

    @Test
    public void test() {
        for (int i = 0; i < 10; i++) {
            Pair<Double, Double> pair = DateBaseInit.getAGeoCorrd();
            System.out.println("longitude: " + pair.getKey() + " and latitude: " + pair.getValue());
        }
    }

    @Test
    public void testInsertIntoAdvertisement() throws Exception{
        Pair coord = DateBaseInit.getAGeoCorrd();
        GeoHash geoHash = GeoHash.withBitPrecision((Double) coord.getKey(), (Double) coord.getValue(), 60);
        System.out.println("纬度值为: " + (double) coord.getKey());
        System.out.println("经度值为: " + (double) coord.getValue());
        System.out.println("Base32: " + geoHash.toBase32());
        Random random = new Random();
        int index = random.nextInt();
        String conditionSubStr1 = DateBaseInit.getMediaTypeByRandom(index);
        index = random.nextInt();
        String conditionSubStr2 = DateBaseInit.getProTypeByRandom(index);
        System.out.println("MediaConditionString: " + conditionSubStr1);
        System.out.println("ProConditionString: " + conditionSubStr2);
        System.out.println("Condition:value : " + conditionSubStr1 + conditionSubStr2);
        byte[] imgData = DateBaseInit.generateImgData("D:\\tempImg/generateImg/OIP.jpg");
    }

    @Test
    public void testImg() throws Exception{
        byte[] imgData = DateBaseInit.generateImgData("D:\\tempImg/generateImg/OIP.jpg");
        String img = new String(imgData, Charset.forName("ISO-8859-1"));
        byte[] ne = img.getBytes(Charset.forName("ISO-8859-1"));
        assertEquals(true, Arrays.equals(img.getBytes(Charset.forName("ISO-8859-1")), imgData));
    }
}