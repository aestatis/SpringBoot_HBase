package com.springboot.hbase.service;

import com.springboot.hbase.utli.HBaseConfig;
import javafx.util.Pair;
import org.apache.hadoop.hbase.client.Table;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/*
* author: ygp
* 该Class用来作HBase数据库的初始化。
*
 */
public class DateBaseInit {

    /*
    * 根据设计方案，添加存储广告信息的Advertisement Table
    *            添加存储媒体信息的Media Table
    *            添加存储行业信息的Profession Table
    * Media Table 和 Profession Table 是用作条件，去从取出的Advertisement构成的集合中，
    * 筛选满足相应性质的Advertisement。
    *
    *
     */
    public void createDesignedTables(){
        HBaseService hBaseService = new HBaseConfig().getHbaseService();
        List<String> tableColumnFamily = new LinkedList<String>();
        tableColumnFamily.add("ConditionKey");
        tableColumnFamily.add("AdverInfo");
        hBaseService.creatTable("Advertisement", tableColumnFamily);

        // 调用完了上面的createTable之后，Admin是被关闭了吧，这里是要重新创建 HBaseService?
        hBaseService = new HBaseConfig().getHbaseService();
        tableColumnFamily = new LinkedList<String>();
        tableColumnFamily.add("MediaRefer");
        tableColumnFamily.add("MediaCategory");
        hBaseService.creatTable("Media", tableColumnFamily);

        //
        hBaseService = new HBaseConfig().getHbaseService();
        tableColumnFamily = new LinkedList<String>();
        tableColumnFamily.add("ProRefer");
        tableColumnFamily.add("ProCategory");
        hBaseService.creatTable("Profession", tableColumnFamily);

        // 给Media表导入数据。
        HashMap<String, String[]> refer = new HashMap<String, String[]>(){{
            put("城市干道", new String[]{"00000001"});
            put("餐饮酒店", new String[]{"00000010"});
            put("楼宇", new String[]{"00000011"});
            put("商业中心", new String[]{"00000100"});
            put("高速", new String[]{"00000101"});
            put("火车站", new String[]{"00000110"});
            put("机场", new String[]{"00000111"});
            put("公交", new String[]{"00001000"});
            put("地铁", new String[]{"00001001"});
        }};

        for (Map.Entry<String, String[]> entry : refer.entrySet()){
            hBaseService.putData("Media", entry.getKey(),
                    "MediaRefer", new String[]{"value"}, entry.getValue());
        }

        HashMap<String, Pair<String[], String[]>> category = new HashMap<String, Pair<String[], String[]>>() {{
            put("城市干道", new Pair<>(new String[]{"大牌","灯箱","框架/海报","道闸"},
                    new String[]{"00000001","00000010","00000011","00000100"}));

            put("餐饮酒店", new Pair<>(new String[]{"框架/海报"},
                    new String[]{"00000001"}));

            put("楼宇", new Pair<>(new String[]{"大牌","灯箱","框架/海报","刷屏机","道闸"},
                    new String[]{"00000001","00000010","00000011","00000100", "00000101"}));

            put("商业中心", new Pair<>(new String[]{"大牌", "LED"},
                    new String[]{"00000001","00000010"}));

            put("高速", new Pair<>(new String[]{"大牌"},
                    new String[]{"00000001"}));

            put("火车站", new Pair<>(new String[]{"大牌","灯箱"},
                    new String[]{"00000001","00000010"}));

            put("机场", new Pair<>(new String[]{"灯箱"},
                    new String[]{"00000001"}));

            put("公交", new Pair<>(new String[]{"灯箱", "车身"},
                    new String[]{"00000001","00000010"}));

            put("地铁", new Pair<>(new String[]{"灯箱"},
                    new String[]{"00000001"}));
        }};
        for (Map.Entry<String, Pair<String[], String[]>> entry: category.entrySet()){
            hBaseService.putData("Media", entry.getKey(), "MediaCategory",
                    entry.getValue().getKey(), entry.getValue().getValue());
        }

        // 给Profession Table 导入数据。

        HashMap<String, String[]> pRefer = new HashMap<String, String[]>(){{
            put("房地产", new String[]{"00000001"});
            put("互联网APP", new String[]{"00000010"});
            put("医疗保健", new String[]{"00000011"});
            put("服务业", new String[]{"00000100"});
            put("商超百货", new String[]{"00000101"});
            put("建筑材料及服务", new String[]{"00000110"});
            put("家居用品", new String[]{"00000111"});
            put("家电", new String[]{"00001000"});
            put("数码电器", new String[]{"00001001"});
            put("教育培训", new String[]{"00001010"});
            put("金融投资", new String[]{"00001011"});
            put("邮电通讯", new String[]{"00001100"});
            put("娱乐休闲", new String[]{"00001101"});
            put("酒水饮料", new String[]{"00001110"});
            put("食品", new String[]{"00001111"});
            put("交通", new String[]{"00010000"});
            put("个护化妆", new String[]{"00010001"});
            put("服饰", new String[]{"00010010"});
            put("办公", new String[]{"00010011"});
            put("工业", new String[]{"00010100"});
            put("农业", new String[]{"00010101"});
        }};

        for (Map.Entry<String, String[]> entry : pRefer.entrySet()){
            hBaseService.putData("Profession", entry.getKey(),
                    "ProRefer", new String[]{"value"}, entry.getValue());
        }


        HashMap<String, Pair<String[], String[]>> pCategory = new HashMap<String, Pair<String[], String[]>>(){{
            put("房地产", new Pair<>(new String[]{"商品房", "工业厂房及园区", "招商/招租", "房产中介", "物业服务", "设计施工"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110"}));

            put("互联网APP", new Pair<>(new String[]{"搜索引擎", "信息门户", "社交/通讯", "购物类", "招聘类", "影音类",
            "生活服务类", "工具类"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000"}));

            put("医疗保健", new Pair<>(new String[]{"综合医院", "专科医院", "医美整型", "体验中心", "月子中心", "医疗器械",
            "保健品", "保健设备及服务", "诊所/病房", "药品"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000", "00001001", "00001010"}));

            put("服务业", new Pair<>(new String[]{"展览展会", "工商服务", "咨询服务", "法律服务", "技术服务", "广告/策划",
            "餐饮服务", "家政服务", "摄影服务", "洗涤服务", "婚庆婚介服务", "丧葬服务", "环保", "宠物服务"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000", "00001001", "00001010", "00001011", "00001100", "00001101",
                            "00001110"}));

            put("商超百货", new Pair<>(new String[]{"超市/购物中心", "家电卖场", "家居建材", "美容化妆", "零食店"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101"}));

            put("建筑材料及服务", new Pair<>(new String[]{"装饰装修", "油漆涂料墙纸", "瓷业", "木业", "管业", "采暖器材",
            "新风系统", "建材五金", "橱柜", "电梯", "卫浴洁具"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000", "00001001", "00001010", "00001011"}));

            put("家居用品", new Pair<>(new String[]{"布艺", "灯具", "床上用品", "家具", "厨具/餐具", "装饰/收藏品",
            "家用清洁"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111"}));

            put("家电", new Pair<>(new String[]{"厨房电器", "卫浴电器", "空调电扇冰箱", "洗衣干衣设备", "电视音响及配件",
            "净化设备", "除尘打扫", "小家电产品"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000"}));

            put("数码电器", new Pair<>(new String[]{"电脑", "电脑配件", "存储设备", "3C产品"},
                    new String[]{"00000001", "00000010", "00000011", "00000100"}));

            put("教育培训", new Pair<>(new String[]{"学前教育", "幼儿园", "中小学培训", "留学/国际教育", "成人教育",
            "语言类培训", "技能培训", "兴趣培养", "学习机", "学习软件"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000", "00001001", "00001010"}));

            put("金融投资", new Pair<>(new String[]{"银行", "保险", "证券", "投资理财", "典当/拍卖", "彩票", "金融租赁"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111"}));

            put("邮电通讯", new Pair<>(new String[]{"手机", "手机配件", "电信服务", "通讯设备"},
                    new String[]{"00000001", "00000010", "00000011", "00000100"}));

            put("娱乐休闲", new Pair<>(new String[]{"旅游服务", "影视/赛事", "运动/健身", "演出/活动", "文化娱乐场所",
            "酒店", "玩具游戏", "乐器乐行", "图书音像制品"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000", "00001001"}));

            put("酒水饮料", new Pair<>(new String[]{"白酒", "啤酒", "葡萄酒", "含酒精饮料", "碳酸饮料", "功能饮料",
            "乳制饮品", "水", "果汁", "咖啡/茶饮料", "茶叶"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000", "00001001", "00001010", "00001011"}));

            put("食品", new Pair<>(new String[]{"方便食品", "膨化食品", "传统食品", "宠物食品", "冰雪食品", "烘烤食品",
            "食用油", "调味品", "蔬菜水果", "生鲜", "米面杂粮", "糖果/零食", "奶粉"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000", "00001001", "00001010", "00001011", "00001100", "00001101"}));

            put("交通", new Pair<>(new String[]{"非机动车", "轿车/客车", "货车/工程车辆", "飞机游艇", "汽车设备及配件",
            "燃油润滑", "出租/客运服务", "汽车销售租赁及养护", "航空/船舶服务", "物流/快递"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000", "00001001", "00001010"}));

            put("个护化妆", new Pair<>(new String[]{"护肤品", "彩妆", "美发美容美体", "口腔护理", "洗发护发", "香水",
            "母婴用品", "女性用品", "男士护理", "个人清洁", "计生用品"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000", "00001001", "00001010", "00001011"}));

            put("服饰", new Pair<>(new String[]{"内衣", "男装", "珠宝首饰", "女装", "童装", "皮衣皮革", "鞋",
            "配饰", "腕表", "箱包", "眼镜", "专卖店"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110",
                            "00000111", "00001000", "00001001", "00001010", "00001011", "00001100"}));

            put("办公", new Pair<>(new String[]{"办公家具", "办公设备及耗材", "文具", "办公软件"},
                    new String[]{"00000001", "00000010", "00000011", "00000100"}));

            put("工业", new Pair<>(new String[]{"机械设备及配件", "原材料", "电源", "仪器仪表", "线缆"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101"}));

            put("农业", new Pair<>(new String[]{"化肥", "农兽药", "饲料", "农林渔牧物资", "烟草", "农场"},
                    new String[]{"00000001", "00000010", "00000011", "00000100", "00000101", "00000110"}));
        }};

        for (Map.Entry<String, Pair<String[], String[]>> entry : pCategory.entrySet()){
            hBaseService.putData("Profession", entry.getKey(),
            "ProCategory", entry.getValue().getKey(), entry.getValue().getValue());
        }
    }

    /*
    * @Description 按索引获取一个String，String的值来代表Media字段。字段的前8位代表Media类型，后8位代表Media的具体类目。
    * @Description 设计这个函数的初衷是为了随机生成数据时，给一个Advertisement随机生成一个ConditionKey的 前 16字段。
     */
    public static String getMediaTypeByRandom(int index){

        String[] MediaConditions = {
                //城市干道,后8位全0表示全选。
                "0000000100000001",
                "0000000100000010",
                "0000000100000011",
                "0000000100000100",

                //餐饮酒店
                "0000001000000001",

                //楼宇
                "0000001100000001",
                "0000001100000010",
                "0000001100000011",
                "0000001100000100",
                "0000001100000101",

                //商业中心
                "0000010000000001",
                "0000010000000010",

                //高速
                "0000010100000001",

                //火车站
                "0000011000000001",
                "0000011000000010",

                //机场
                "0000011100000001",

                //公交
                "0000100000000001",
                "0000100000000010",

                //地铁
                "0000100100000001",
        };
        if (index < 0){
            index = -index;
        }

        index = index % MediaConditions.length;
        return MediaConditions[index];
    }

    /*
    * @Description 按一个值获取一个String，该String代表Profession字段。字段的前8位代表Pro类型，后8位是具体类型下的具体类目。
    * @Description 设计这个函数的初衷是为了随机生成数据时，给一个Advertisement随机生成一个ConditionKey的 后 16字段。
     */
    public static String getProTypeByRandom(int index){

        String[] ProCondition = {
                // 房地产
                "0000000100000001", "0000000100000010", "0000000100000011",
                "0000000100000100", "0000000100000101", "0000000100000110",

                // 互联网APP
                "0000001000000001", "0000001000000010", "0000001000000011",
                "0000001000000100", "0000001000000101", "0000001000000110",
                "0000001000000111", "0000001000001000",

                // 医疗保健
                "0000001100000001", "0000001100000010",
                "0000001100000011", "0000001100000100", "0000001100000101",
                "0000001100000110", "0000001100000111", "0000001100001000",
                "0000001100001001", "0000001100001010",

                // 服务业
                "0000010000000001", "0000010000000010",
                "0000010000000011", "0000010000000100", "0000010000000101",
                "0000010000000110", "0000010000000111", "0000010000001000",
                "0000010000001001", "0000010000001010", "0000010000001011",
                "0000010000001100", "0000010000001101", "0000010000001110",

                // 商超百货
                "0000010100000001", "0000010100000010",
                "0000010100000011", "0000010100000100", "0000010100000101",

                // 建筑材料及服务
                "0000011000000001", "0000011000000010",
                "0000011000000011", "0000011000000100", "0000011000000101",
                "0000011000000110", "0000011000000111", "0000011000001000",
                "0000011000001001", "0000011000001010", "0000011000001011",

                // 家居用品
                "0000011100000001", "0000011100000010",
                "0000011100000011", "0000011100000100", "0000011100000101",
                "0000011100000110", "0000011100000111",

                // 家电
                "0000100000000001", "0000100000000010",
                "0000100000000011", "0000100000000100", "0000100000000101",
                "0000100000000110", "0000100000000111", "0000100000001000",

                // 数码电器
                "0000100100000001", "0000100100000010",
                "0000100100000011", "0000100100000100",

                // 教育培训
                "0000101000000001", "0000101000000010",
                "0000101000000011", "0000101000000100", "0000101000000101",
                "0000101000000110", "0000101000000111", "0000101000001000",
                "0000101000001001", "0000101000001010",

                // 金融投资
                "0000101100000001", "0000101100000010",
                "0000101100000011", "0000101100000100", "0000101100000101",
                "0000101100000110", "0000101100000111",

                // 邮电通讯
                "0000110000000001", "0000110000000010",
                "0000110000000011", "0000110000000100",

                // 娱乐休闲
                "0000110100000001", "0000110100000010",
                "0000110100000011", "0000110100000100", "0000110100000101",
                "0000110100000110", "0000110100000111", "0000110100001000",
                "0000110100001001",

                // 酒水饮料
                "0000111000000001", "0000111000000010",
                "0000111000000011", "0000111000000100", "0000111000000101",
                "0000111000000110", "0000111000000111", "0000111000001000",
                "0000111000001001", "0000111000001010", "0000111000001011",

                // 食品
                "0000111100000001", "0000111100000010",
                "0000111100000011", "0000111100000100", "0000111100000101",
                "0000111100000110", "0000111100000111", "0000111100001000",
                "0000111100001001", "0000111100001010", "0000111100001011",
                "0000111100001100", "0000111100001101", "0000111100001110",

                // 交通
                "0001000000000001", "0001000000000010",
                "0001000000000011", "0001000000000100", "0001000000000101",
                "0001000000000110", "0001000000000111", "0001000000001000",
                "0001000000001001", "0001000000001010",

                // 个护化妆
                "0001000100000001", "0001000100000010",
                "0001000100000011", "0001000100000100", "0001000100000101",
                "0001000100000110", "0001000100000111", "0001000100001000",
                "0001000100001001", "0001000100001010", "0001000100001011",

                // 服饰
                "0001001000000001", "0001001000000010",
                "0001001000000011", "0001001000000100", "0001001000000101",
                "0001001000000110", "0001001000000111", "0001001000001000",
                "0001001000001001", "0001001000001010", "0001001000001011",
                "0001001000001100",

                // 办公
                "0001001100000001", "0001001100000010",
                "0001001100000011", "0001001100000100",

                // 工业
                "0001010000000001", "0001010000000010",
                "0001010000000011", "0001010000000100", "0001010000000101",

                // 农业
                "0001010100000001", "0001010100000010",
                "0001010100000011", "0001010100000100", "0001010100000101",
                "0001010100000110"
        };
        if (index < 0){
            index = -index;
        }

        index = index % ProCondition.length;
        return ProCondition[index];
    }

    /*
    * @param imgPath 图片的地址
    * @Description 从图片地址得到一个存储了图片信息的 byte[],后续可以将该byte[] 存入数据库。
     */
    public static byte[] generateImgData(String imgPath) throws IOException {
        FileInputStream fis = new FileInputStream(imgPath);
        byte[] imgByte = new byte[fis.available()];
        fis.read(imgByte);
        fis.close();

        return imgByte;
    }

    /*
    * @author ygp
    * @Description 随机获取一个在上海市范围内的地理坐标，返回Pair对象，getKey()获取纬度值，getValue()获取经度值。
     */
    public static Pair<Double, Double> getAGeoCorrd(){
        Random random1 = new Random();
        Random random2 = new Random();

        double latDec = random1.nextDouble();
        while (!(0.200000 <= latDec && latDec <= 0.866666)){
            latDec = random1.nextDouble();
        }

        int latInt = (random2.nextInt() % 3) + 120;
        while (!(120 <= latInt && latInt <= 122)){
            latInt = (random2.nextInt() % 3) + 120;
        }

        double lngDec = random1.nextDouble();
        while (!(0.666666 <= lngDec && 0.883333 <= lngDec)){
            lngDec = random1.nextDouble();
        }

        int lngInt = (random2.nextInt() % 2) + 30;
        while (!(30 <= lngInt && lngInt <= 31)){
            lngInt = (random2.nextInt() % 2) + 30;
        }

        double latitude = (double)latInt + latDec;
        double longitude = (double)lngInt + lngDec;

        String lat = String.format("%.6f", latitude);
        String lng = String.format("%.6f", longitude);

        latitude = Double.parseDouble(lat);
        longitude = Double.parseDouble(lng);

        return new Pair<Double, Double>(longitude, latitude);
    }
}
