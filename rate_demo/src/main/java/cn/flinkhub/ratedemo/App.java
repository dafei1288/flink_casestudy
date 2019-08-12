package cn.flinkhub.ratedemo;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map properties= new HashMap();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
//        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("topicOrder", "order");
        properties.put("topicRate", "rate");

        ParameterTool parameterTool = ParameterTool.fromMap(properties);

        FlinkKafkaConsumer010 consumer010Rate = new FlinkKafkaConsumer010(
                parameterTool.getRequired("topicRate"), new DeserializationSchema() {
            @Override
            public TypeInformation getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple3<Long,String,Integer>>(){});
                //return TypeInformation.of(new TypeHint<Tuple>(){});
            }

            @Override
            public Tuple3<Long,String,Integer> deserialize(byte[] message) throws IOException {
                String[] res = new String(message).split(",");
                Long timestamp = Long.valueOf(res[0]);
                String dm = res[1];
                Integer value = Integer.valueOf(res[2]);
                return Tuple3.of(timestamp,dm,value);
            }

            @Override
            public boolean isEndOfStream(Object nextElement) {
                return false;
            }
        }, parameterTool.getProperties());

        FlinkKafkaConsumer010 consumer010Order = new FlinkKafkaConsumer010(
                parameterTool.getRequired("topicOrder"), new DeserializationSchema() {
            @Override
            public TypeInformation getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple5<Long,String,Integer,String,Integer>>(){});
            }

            @Override
            public Tuple5<Long,String,Integer,String,Integer> deserialize(byte[] message) throws IOException {
                //%d,%s,%d,%s,%d
                String[] res = new String(message).split(",");
                Long timestamp = Long.valueOf(res[0]);
                String catlog = res[1];
                Integer subcat = Integer.valueOf(res[2]);
                String dm = res[3];
                Integer value = Integer.valueOf(res[4]);
                return Tuple5.of(timestamp,catlog,subcat,dm,value);
            }

            @Override
            public boolean isEndOfStream(Object nextElement) {
                return false;
            }
        }, parameterTool.getProperties());

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple3<Long,String,Integer>> rateStream = env.addSource(consumer010Rate);
        DataStream<Tuple5<Long,String,Integer,String,Integer>> oraderStream = env.addSource(consumer010Order);
        long delay = 1000;
        DataStream<Tuple3<Long,String,Integer>> rateTimedStream = rateStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long,String,Integer>>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                return (Long)element.getField(0);
            }
        });
        DataStream<Tuple5<Long,String,Integer,String,Integer>> oraderTimedStream = oraderStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long,String,Integer,String,Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple5 value) {

                return (Long)value.getField(0);
            }
        });
        DataStream<Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer>> joinedStream = oraderTimedStream.join(rateTimedStream).where(new KeySelector<Tuple5<Long,String,Integer,String,Integer>,String>(){
                @Override
                public String getKey(Tuple5<Long,String,Integer,String,Integer> value) throws Exception {
                    return value.getField(3).toString();
                }
        }).equalTo(new KeySelector<Tuple3<Long,String,Integer>,String>(){
            @Override
            public String getKey(Tuple3<Long,String,Integer> value) throws Exception {
                return value.getField(1).toString();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple5<Long,String,Integer,String,Integer>, Tuple3<Long,String,Integer>,Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer>>() {
                    @Override
                    public Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer> join( Tuple5<Long,String,Integer,String,Integer> first, Tuple3<Long,String,Integer>second) throws Exception {
                        Integer res = (Integer)second.getField(2)*(Integer)first.getField(4);

                        return Tuple9.of(first.f0,first.f1,first.f2,first.f3,first.f4,second.f0,second.f1,second.f2,res);
                    }
                });





        joinedStream.print();
        env.execute("done!");
    }
}
