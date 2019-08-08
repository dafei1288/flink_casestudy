package dummy;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class RateWriter {
    public static final String[] HBDM = {"BEF","CNY","DEM","EUR","HKD","USD","ITL"};
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map prop = new HashMap();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("topic", "rate");
        ParameterTool parameterTool = ParameterTool.fromMap(prop);
        DataStream<String> messageStream = env.addSource(new SourceFunction<String>() {
            private Random r = new Random();
            private static final long serialVersionUID = 1L;
            boolean running = true;
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while(running) {
                    Thread.sleep(r.nextInt(3) * 1000);
                    ctx.collect(String.format("%d,%s,%d", System.currentTimeMillis(), HBDM[r.nextInt(HBDM.length)], r.nextInt(20)));
                }
            }
            @Override
            public void cancel() {
                running = false;
            }
        });

        DataStreamSink<String> airQualityVODataStreamSink = messageStream.addSink(new FlinkKafkaProducer010<>(parameterTool.getRequired("bootstrap.servers"),
                parameterTool.getRequired("topic"),
                new SimpleStringSchema()));
        messageStream.print();
        env.execute("write rate to kafka !!!");
    }
}
