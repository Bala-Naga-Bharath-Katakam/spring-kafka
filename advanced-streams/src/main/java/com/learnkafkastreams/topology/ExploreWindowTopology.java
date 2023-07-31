package com.learnkafkastreams.topology;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.*;

@Slf4j
public class ExploreWindowTopology {

    public static final String WINDOW_WORDS = "windows-words";

    public static Topology tumblingWindows(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> kStream = streamsBuilder.stream(WINDOW_WORDS, Consumed.with(Serdes.String(), Serdes.String()));

        Duration windowSize=Duration.ofSeconds(5);

        TimeWindows timeWindow=TimeWindows.ofSizeWithNoGrace(windowSize);

        TimeWindowedKStream<String, String> timeWindowedKStream = kStream.groupByKey().windowedBy(timeWindow);

        KTable<Windowed<String>, Long> count = timeWindowedKStream.count();

        count.toStream().peek((k,v)->log.info("key and values are : {} {} ",k,v ));

        return streamsBuilder.build();
    }

    public static Topology hoppingWindow(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> kStream = streamsBuilder.stream(WINDOW_WORDS, Consumed.with(Serdes.String(), Serdes.String()));

        Duration windowSize=Duration.ofSeconds(5);

        Duration advancedSize=Duration.ofSeconds(5);

        TimeWindows timeWindow=TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advancedSize);

        TimeWindowedKStream<String, String> timeWindowedKStream = kStream.groupByKey().windowedBy(timeWindow);

        KTable<Windowed<String>, Long> count = timeWindowedKStream.count();

        count.toStream().peek((k,v)->log.info("key and values are : {} {} ",k,v ));

        return streamsBuilder.build();
    }

    public static Topology slidingWindow(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> kStream = streamsBuilder.stream(WINDOW_WORDS, Consumed.with(Serdes.String(), Serdes.String()));

        Duration windowSize=Duration.ofSeconds(5);

        SlidingWindows timeWindow=SlidingWindows.ofTimeDifferenceWithNoGrace(windowSize);

        TimeWindowedKStream<String, String> timeWindowedKStream = kStream.groupByKey().windowedBy(timeWindow);

        KTable<Windowed<String>, Long> count = timeWindowedKStream.count();

        count.toStream().peek((k,v)->log.info("key and values are : {} {} ",k,v ));

        return streamsBuilder.build();
    }


    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();

        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

}
