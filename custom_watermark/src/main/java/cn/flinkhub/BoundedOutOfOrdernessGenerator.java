package cn.flinkhub;

import cn.flinkhub.common.MyEvent;
import lombok.extern.java.Log;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

@Log
public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<MyEvent> {

    private final long maxOutOfOrderness = 3500; // 3.5 seconds

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
        long timestamp = element.getCreateTime();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        log.info(" currentMaxTimestamp =  " + currentMaxTimestamp + " , timestamp" + timestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        log.info("getCurrentWatermark ....");
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}

