package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.serdes.JSONSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";

    public static final String GENERAL_ORDERS = "general-orders";

    public static final String GENERAL_REVENUES = "general-revenues";
    public static final String STORES = "stores";

    static Predicate<String, Order> generalPredicate = (key, value) -> value.orderType().equals(OrderType.GENERAL);

    static Predicate<String, Order> restaurantPredicate = (key, value) -> value.orderType().equals(OrderType.RESTAURANT);

    static ValueMapper<Order,Revenue> valueMapper = value -> new Revenue(value.locationId(),value.finalAmount());
    public static Topology buildTopology(){
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String, Order> orderKStream = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), JSONSerde.orderFactory()));

        orderKStream.peek((key,value)->{
           log.info("key and value are :{} {} ",key,value);
        });

        orderKStream
                .split(Named.as("general-restaurant-stream"))
                .branch(generalPredicate,Branched.withConsumer(generalStream->{
                    generalStream
                            .mapValues(value->valueMapper.apply(value))
                            .to(GENERAL_REVENUES,Produced.with(Serdes.String(), JSONSerde.revenueFactory()));
                }));





        return streamsBuilder.build();
    }

    public static Topology ordersTopology(){
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String, Order> orderKStream = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), JSONSerde.orderFactory()));

        orderKStream.peek((key,value)->{
            log.info("key and value are :{} {} ",key,value);
        });

        orderKStream
                .split(Named.as("general-restaurant-stream"))
                .branch(generalPredicate,Branched.withConsumer(generalStream->{
                    generalStream
                            .map((k,v)-> KeyValue.pair(v.locationId(),v))
                            .groupByKey(Grouped.with(Serdes.String(), JSONSerde.orderFactory()))
                            .count(Named.as("general-orders"))
                            .toStream()
                            .peek((k,v)->log.info("key and value are : {} {} ",k,v));
                }));





        return streamsBuilder.build();
    }
}
