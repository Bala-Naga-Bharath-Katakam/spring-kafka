package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class KTableCountTopology {

    public static final String K_TABLE = "k-table-aggregation";

    public static Topology buildTopology(){

        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String, String> stream = streamsBuilder
                .stream("orders", Consumed.with(Serdes.String(), Serdes.String()));

        KGroupedStream<String, String> groupedStream =
                stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        groupedStream.count(Named.as("count"))
                .toStream()
                .peek((k,v)->log.info("key and value are : {} {} ",k,v));
        return streamsBuilder.build();
    }




}
