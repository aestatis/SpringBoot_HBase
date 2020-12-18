package com.springboot.hbase.utli;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

class GeoUtilityTest {

    @Test
    void testGetGeoHashSet() {
        HashSet<String> geoSet;
        geoSet = GeoUtility.getGeoHashSet(31.271127, 30.889603,
                121.874054,120.941541, 40);

        System.out.println(geoSet.size());

        for (String str : geoSet){
            System.out.println(str);
        }
    }
}