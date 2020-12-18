package com.springboot.hbase.utli;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.util.BoundingBoxGeoHashIterator;
import ch.hsr.geohash.util.TwoGeoHashBoundingBox;

import java.util.HashSet;

/*
* author: ygp
*
* @Description: 用来提供一些GeoHash原来没有的功能。
 */
public class GeoUtility {

    /*
    * author: ygp
    *
    * @param maxLat 矩形区域内纬度的最大值
    * @param maxLng 矩形区域内经度的最大值
    * @param minLat 矩形区域内纬度的最小值
    * @param minLng 矩形区域内经度的最小值
    * @param precision 表示精度大小，设置来处理地图放大缩小。注意precision的取值最大是60，最小是0，且必须是5的倍数，建议从40开始取，不然集合会很大。
    *
    * @Description: 通过给的经纬度区域，得到该区域内所有的GeoHash值。注意所有的参数都是Double类型，小数点后6位
     */
    public static HashSet<String> getGeoHashSet(double maxLat, double minLat, double maxLng,
                                                double minLng, int precision){

        // 左下角 和 右上角
        GeoHash southWestCorner = GeoHash.withBitPrecision(minLat, minLng, precision);
        GeoHash northEastCorner = GeoHash.withBitPrecision(maxLat, maxLng, precision);


        //使用两个geoHash构建一个外接盒型
        TwoGeoHashBoundingBox twoGeoHashBoundingBox = new TwoGeoHashBoundingBox(southWestCorner,
                northEastCorner);

        BoundingBoxGeoHashIterator iterator = new BoundingBoxGeoHashIterator(twoGeoHashBoundingBox);

        HashSet<String> geoHashSet = new HashSet<>();
        GeoHash geoHash;

        while (iterator.hasNext()){
            geoHash = iterator.next();
            geoHashSet.add(geoHash.toBase32());
        }

        return geoHashSet;
    }
}
