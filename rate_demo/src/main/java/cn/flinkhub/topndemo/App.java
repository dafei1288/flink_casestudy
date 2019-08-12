package cn.flinkhub.topndemo;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

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

        DataStream<Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer>> joinedTimedStream = joinedStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer> element) {
                return element.f0;
            }
        });

        DataStream<OrderView> windowedData  = joinedTimedStream.keyBy(new KeySelector<Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer>,String>(){
            @Override
            public String getKey(Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer> value) throws Exception {
                return value.f1+value.f2;
            }
        }).timeWindow(Time.seconds(30), Time.seconds(10))
                .aggregate(new SumAgg(), new WindowResultFunction());

        DataStream<String> topNHots = windowedData
                .keyBy("windowEnd")
                .process(new TopNHot(5));

        topNHots.print();
        env.execute("done!");
    }

    public static class SumAgg implements AggregateFunction<Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer> value, Long acc) {
            return acc + value.f8;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    /** 用于输出窗口的结果 */
    //IN, OUT, KEY, W extends Window
    public static class WindowResultFunction implements WindowFunction<Long, OrderView, String, TimeWindow> {

        @Override
        public void apply(
                String key,  // 窗口的主键
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果
                Collector<OrderView> collector  // 输出类型为 OrderView
        ) throws Exception {
            Long count = aggregateResult.iterator().next();
            collector.collect(OrderView.of(key, window.getEnd(), count));
        }
    }


    public static class OrderView {
        public String itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long allsum;  // 商品的销售量

        public static OrderView of(String itemId, long windowEnd, long allsum) {
            OrderView result = new OrderView();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.allsum = allsum;
            return result;
        }

        @Override
        public String toString() {
            return "OrderView{" +
                    "itemId='" + itemId + '\'' +
                    ", windowEnd=" + windowEnd +
                    ", viewCount=" + allsum +
                    '}';
        }
    }

    public static class TopNHot extends KeyedProcessFunction<Tuple, OrderView, String> {

        private final int topSize;

        public TopNHot(int topSize) {
            this.topSize = topSize;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private ListState<OrderView> orderState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态的注册
            ListStateDescriptor<OrderView> itemsStateDesc = new ListStateDescriptor<>(
                    "orderState-state",
                    OrderView.class);
            orderState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(
                OrderView input,
                Context context,
                Collector<String> collector) throws Exception {

            // 每条数据都保存到状态中
            orderState.add(input);
            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            context.timerService().registerEventTimeTimer(input.windowEnd + 1);
        }

        @Override
        public void onTimer(
        long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品销售量
            List<OrderView> allItems = new ArrayList<>();
            orderState.get().forEach(it->allItems.add(it));
            // 提前清除状态中的数据，释放空间
            orderState.clear();
            // 按照销售量从大到小排序
            allItems.sort((x1,x2)-> (int) (x1.allsum - x2.allsum));
            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
            for (int i=0;i<topSize && i<allItems.size();i++) {
                OrderView currentItem = allItems.get(i);
                // No1:  商品ID=12224  销售额=2413
                result.append("No").append(i+1).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  销售额=").append(currentItem.allsum)
                        .append("\n");
            }
            result.append("====================================\n\n");

            out.collect(result.toString());
        }
    }
}
