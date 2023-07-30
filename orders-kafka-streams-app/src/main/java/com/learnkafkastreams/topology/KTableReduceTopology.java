package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class KTableReduceTopology {

    public static final String K_TABLE = "k-table-reduce";

    public static Topology buildTopology(){

        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String, String> stream = streamsBuilder
                .stream("AGGREGATE", Consumed.with(Serdes.String(), Serdes.String()));

        // grouped based on value
        KGroupedStream<String, String> groupedStream =
                stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        groupedStream.reduce((value1, value2) -> {
            log.info("values are : {} {} ",value1,value2);
            return value1+value2;
        });
        return streamsBuilder.build();
    }


}
