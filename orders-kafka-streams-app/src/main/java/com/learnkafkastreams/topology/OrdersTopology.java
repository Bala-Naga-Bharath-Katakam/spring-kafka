package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.serdes.JSONSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";

    public static final String GENERAL_ORDERS = "general-orders";
    public static final String STORES = "stores";

    static Predicate<String, Order> generalPredicate = (key, value) -> value.orderType().equals(OrderType.GENERAL);

    static Predicate<String, Order> restaurantPredicate = (key, value) -> value.orderType().equals(OrderType.RESTAURANT);
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
                            .to("general-orders",Produced.with(Serdes.String(), JSONSerde.orderFactory()));
                }));





        return streamsBuilder.build();
    }
}
