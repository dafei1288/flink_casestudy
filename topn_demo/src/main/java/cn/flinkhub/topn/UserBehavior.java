package cn.flinkhub.topn;

import lombok.Getter;
import lombok.Setter;

public class UserBehavior {
    @Setter @Getter
    private long userId;         // 用户ID

    @Setter @Getter
    private long itemId;         // 商品ID

    @Setter @Getter
    private int categoryId;      // 商品类目ID

    @Setter @Getter
    private String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")

    @Setter @Getter
    private long timestamp;      // 行为发生的时间戳，单位秒

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
