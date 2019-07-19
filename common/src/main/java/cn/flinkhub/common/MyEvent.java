package cn.flinkhub.common;


import lombok.Getter;
import lombok.Setter;

import java.util.Random;
import java.util.UUID;


public class MyEvent {
    @Setter
    @Getter
    private String id;

    @Setter
    @Getter
    private Integer rank;

    @Setter
    @Getter
    private String group;

    @Setter
    @Getter
    private Long createTime;

    @Setter
    @Getter
    private Long updateTime;


    private static Random R = new Random();
    private static String[] G = new String[]{"天津","北京","上海","南京","成都","武汉","深圳","广州"};
    public static MyEvent createEvent(){
        MyEvent my = new MyEvent();
        my.setId(UUID.randomUUID().toString());
        my.setRank(R.nextInt(10));
        my.setGroup(G[R.nextInt(G.length-1)]);
        my.setCreateTime(System.currentTimeMillis());
        return my;
    }
}
