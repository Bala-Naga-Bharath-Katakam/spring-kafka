package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Alphabet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

@Slf4j
public class ExploreJoinsOperatorsTopology {


    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple


    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> kStream = streamsBuilder.stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, String> table = streamsBuilder.table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()),Materialized.as("alphabet-stre"));

        ValueJoiner<String, String, Alphabet> valueJoiner=Alphabet::new;

        KStream<String, Alphabet> join = kStream.join(table, valueJoiner);

        join.peek((k,v)->log.info("key and values are : {} {} ",k,v));

        return streamsBuilder.build();
    }

    public static Topology kTableWithKTable(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<String, String> kStream = streamsBuilder.table(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()),Materialized.as("alphabet-abs-store"));

        KTable<String, String> table = streamsBuilder.table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()),Materialized.as("alphabet-store"));

        ValueJoiner<String, String, Alphabet> valueJoiner=Alphabet::new;

        KTable<String, Alphabet> join = kStream.join(table, valueJoiner);

        join.toStream().peek((k,v)->log.info("key and values are : {} {} ",k,v));

        return streamsBuilder.build();
    }

    public static Topology kStreamWithKStream(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> kStream = streamsBuilder.stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> table = streamsBuilder.stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));

        ValueJoiner<String, String, Alphabet> valueJoiner=Alphabet::new;

        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30));

        StreamJoined<String,String,String> streamJoined= StreamJoined.with(Serdes.String(),Serdes.String(),Serdes.String());


        KStream<String, Alphabet> join = kStream.join(table, valueJoiner,joinWindows,streamJoined);

        join.peek((k,v)->log.info("key and values are : {} {} ",k,v));

        return streamsBuilder.build();
    }

}
