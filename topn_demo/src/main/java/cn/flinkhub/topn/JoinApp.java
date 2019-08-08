package cn.flinkhub.topn;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;
//https://blog.csdn.net/whr_yy/article/details/79887275
//https://blog.csdn.net/aA518189/article/details/84032660
//https://blog.csdn.net/xianzhen376/article/details/89810958
//https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/joining.html

public class JoinApp {
    public static void main(String[] args) throws Exception {
        // UserBehavior.csv 的本地文件路径
        URL fileUrl = App.class.getClassLoader().getResource("UserBehavior.txt");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));

        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);

        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};

        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath,pojoType,fieldOrder);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<UserBehavior> dataSource = env.createInput(csvInput, pojoType);
        DataStream<UserBehavior> dataSourceb = env.createInput(csvInput, pojoType);


        dataSource.join(dataSourceb).where(new KeySelector<UserBehavior,Long>(){
            @Override
            public Long getKey(UserBehavior value) throws Exception {
                return value.getUserId();
            }
        }).equalTo(new KeySelector<UserBehavior,Long>(){
            @Override
            public Long getKey(UserBehavior value) throws Exception {
                return value.getUserId();
            }
        }).window(EventTimeSessionWindows.withGap(Time.minutes(10)))
                .apply(new JoinFunction<UserBehavior, UserBehavior, String>() {
                    @Override
                    public String join(UserBehavior first, UserBehavior second) throws Exception {
                        return first.getUserId()+" joined "+second.getUserId();
                    }
                }).print();

        env.execute("join !!!");
    }
}
