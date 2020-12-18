package com.springboot.hbase.entity;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/*
* @author ygp
* 广告实体
 */

public class Advertisement {
    String advId = null;
    // ConditionKey列簇下的 value的值赋给 ConditionKey
    Integer ConditionKey = null;
    // AdverInfo 列簇下的值。
    String name = null;
    String theme = null;
    String master = null;
    String price = null;
    String date = null;
    // img 的值是 图片的url
    String img = null;
    // location 表示一般人能看懂的具体的地点，像是某某村，某某街道，某某号而不是一个Geohash字符串。
    String location = null;


    public Advertisement(){}

    public Advertisement(String advId, Integer ConditionKey, String name, String theme, String master,
                         String price, String date, String img, String location){
        this.advId = advId;
        this.ConditionKey = ConditionKey;
        this.name = name;
        this.theme = theme;
        this.master = master;
        this.price = price;
        this.date = date;
        this.location = location;
    }

    /*
    * @author ygp
    * @param Result 参数表示从数据库中拿到的一行数据。
    * @Description 从一个Result对象中获取信息，转化成一个Advertisement
     */
    public static Advertisement parseAdvertisement(Result result) throws IOException {
        String advId = null;
        Integer ConditionKey = null;
        String name = null;
        String theme = null;
        String master = null;
        String price = null;
        String date = null;
        String img = null;
        String location =null;


        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()){
            Cell cell = cellScanner.current();

            String columnFamily = new String(CellUtil.cloneFamily(cell), "utf-8");
            if (columnFamily.equals("ConditionKey")){

                if (advId == null){
                    String rowKey = new String(CellUtil.cloneRow(cell), "utf-8");
                    String[] partialKey = rowKey.split("_");
                    advId = partialKey[1];
                }

                // 事实上，ConditionKey列族下的 Qualifier 只有value一个
                String qualifier = new String(CellUtil.cloneQualifier(cell), "utf-8");
                if (qualifier.equals("value")){
                    String condition = new String(CellUtil.cloneValue(cell), "utf-8");
                    // 转换成2进制的数字。
                    ConditionKey = Integer.valueOf(condition, 2);
                }
            }else if (columnFamily.equals("AdverInfo")){
                String qualifier = new String(CellUtil.cloneQualifier(cell), "utf-8");

                switch (qualifier){
                    case "name":
                        name = new String(CellUtil.cloneValue(cell), "utf-8");
                        break;
                    case "theme":
                        theme = new String(CellUtil.cloneValue(cell), "utf-8");
                        break;
                    case "master":
                        master = new String(CellUtil.cloneValue(cell), "utf-8");
                        break;
                    case "price":
                        price = new String(CellUtil.cloneValue(cell), "utf-8");
                        break;
                    case "date":
                        date = new String(CellUtil.cloneValue(cell), "utf-8");
                        break;
                    case "img":

                        // 用advId作为图片url的一部分。
                        if (advId == null)
                        {
                            String rowKey = new String(CellUtil.cloneRow(cell), "utf-8");
                            String[] partialKey = rowKey.split("_");
                            advId = partialKey[1];
                        }

                        /*
                         * path 是存放图片的文件夹的路径。
                         * path待实现一个具体的值
                         */
                        String path = "" + advId;

                        File file = new File(path);

                        // 文件不存在，就读取并创建文件。
                        if (!file.exists()){
                            // 从HBase中取出的是一个字节数组，需要把它转化成为一个文件，保存起来，然后得到url。
                            byte[] bs = CellUtil.cloneValue(cell);
                            FileOutputStream fos = new FileOutputStream(file);
                            fos.write(bs);
                            fos.close();
                        }

                        img = path;

                        break;
                    case "location":
                        location = new String(CellUtil.cloneValue(cell), "utf-8");
                        break;
                    default:
                        System.out.println("出错，qualifier : " + qualifier);
                        break;
                }
            }
        }

        return new Advertisement(advId, ConditionKey, name, theme, master, price, date, img, location);
    }

    // getter

    public String getAdvId() {
        return advId;
    }

    public Integer getConditionKey() {
        return ConditionKey;
    }

    public String getName() {
        return name;
    }

    public String getTheme() {
        return theme;
    }

    public String getMaster() {
        return master;
    }

    public String getPrice() {
        return price;
    }

    public String getDate() {
        return date;
    }

    public String getImg() {
        return img;
    }

    public String getLocation() {
        return location;
    }

    // setter

    public void setAdvId(String advId) {
        this.advId = advId;
    }

    public void setConditionKey(Integer conditionKey) {
        ConditionKey = conditionKey;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTheme(String theme) {
        this.theme = theme;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setImg(String img) {
        this.img = img;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}
