package com.learnkafkastreams.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

@Slf4j
public class AggregateProducer {


    public static void main(String[] args) throws InterruptedException {

        var key = "A";

        var word = "Apple";
        var word1 = "Alligator";
        var word2 = "Ambulance";

        var recordMetaData = publishMessageSync("AGGREGATE", key,word);
        log.info("Published the alphabet message : {} ", recordMetaData);

        var recordMetaData1 = publishMessageSync("AGGREGATE", key,word1);
        log.info("Published the alphabet message : {} ", recordMetaData1);

        var recordMetaData2 = publishMessageSync("AGGREGATE", key,word2);
        log.info("Published the alphabet message : {} ", recordMetaData2);

        var bKey = "B";

        var bWord1 = "Bus";
        var bWord2 = "Baby";
        var recordMetaData3 = publishMessageSync("AGGREGATE", bKey,bWord1);
        log.info("Published the alphabet message : {} ", recordMetaData2);

        var recordMetaData4 = publishMessageSync("AGGREGATE", bKey,bWord2);
        log.info("Published the alphabet message : {} ", recordMetaData2);

    }

    public static RecordMetadata publishMessageSync(String topicName, String key, String message ){

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topicName, key, message);
        RecordMetadata recordMetadata=null;

        try {
            log.info("producerRecord : " + producerRecord);
            recordMetadata = ProducerUtil.producer.send(producerRecord).get();
        } catch (InterruptedException e) {
            log.error("InterruptedException in  publishMessageSync : {}  ", e.getMessage(), e);
        } catch (ExecutionException e) {
            log.error("ExecutionException in  publishMessageSync : {}  ", e.getMessage(), e);
        }catch(Exception e){
            log.error("Exception in  publishMessageSync : {}  ", e.getMessage(), e);
        }
        return recordMetadata;
    }



}
