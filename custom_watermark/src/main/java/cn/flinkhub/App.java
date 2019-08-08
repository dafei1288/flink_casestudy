package cn.flinkhub;


import cn.flinkhub.common.MyEvent;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

public class App {
    public static void main(String[] args) {
//        DataStream<MyEvent> stream = ...
//
//        DataStream<MyEvent> withTimestampsAndWatermarks =
//                stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MyEvent>(Time.seconds(10)) {
//
//                    @Override
//                    public long extractTimestamp(MyEvent element) {
//                        return element.getCreationTime();
//                    }
//                });
    }
}
