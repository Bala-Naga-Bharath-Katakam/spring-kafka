package com.learnkafkastreams.topology;

import com.learnkafkastreams.serdes.JSONSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

@Slf4j
public class KTableTopology {

    public static final String K_TABLE = "k-table-store";

    public static Topology buildTopology(){

        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KTable<String, String> orders = streamsBuilder
                .table("orders", Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as(K_TABLE));

        orders.toStream()
                .peek((k,v)->log.info("key and value are {} {} ",k,v));

        return streamsBuilder.build();
    }


}
