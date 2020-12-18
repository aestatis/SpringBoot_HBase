package com.springboot.hbase.service;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import javafx.util.Pair;
import org.junit.jupiter.api.Test;

import java.sql.SQLSyntaxErrorException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class DateBaseInitTest {

    @Test
    public void testGeoHash(){
        GeoHash geoHash = GeoHash.withBitPrecision(31.271127,120.941541, 60);
        System.out.println("bits: " + geoHash.toBinaryString());
        System.out.println("base32: " + geoHash.toBase32());

        GeoHash geoHash2 = GeoHash.withBitPrecision(30.889603,121.874054, 60);
        System.out.println("bits: " + geoHash2.toBinaryString());
        System.out.println("base32: " + geoHash2.toBase32());
    }

    @Test
    public void testGenerateGeoHashFromString(){
        String str = "wtw3930";
        GeoHash geoHash = GeoHash.fromGeohashString(str);
        System.out.println(geoHash.toBinaryString());
    }

    @Test
    public void test(){
        for (int i = 0; i < 10; i++){
            Pair<Double, Double> pair = DateBaseInit.getAGeoCorrd();
            System.out.println("longitude: " + pair.getKey() +" and latitude: " + pair.getValue());
        }
    }

}