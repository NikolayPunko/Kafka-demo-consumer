package com.kafka.demo.service;

import com.kafka.demo.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaListenerExample {

//    @KafkaListener(topics = "topic-1", groupId = "group1")
//    void listener(String data) {
//        log.info("Received message [{}] in group1!", data);
//    }



//    @KafkaListener(topics = {"topic-1", "topic-2"}, groupId = "group1") //подписались на две темы
//    void listener(@Payload String data,
//                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
//                  @Header(KafkaHeaders.OFFSET) int offset) {
//        log.info("Received message2 [{}] from group1, partition-{} with offset-{}",
//                data,
//                partition,
//                offset);
//    }


//    @KafkaListener(groupId = "group2", topicPartitions = @TopicPartition(topic = "topic-2",
//                    partitionOffsets = {
//                            @PartitionOffset(partition = "0", initialOffset = "0"), //партиция со смещением 0 (т.е. сначала)
//                            @PartitionOffset(partition = "2", initialOffset = "-4")})) //партиция 2 со смещением текущий OFFSET-4
//                                                                         //партиции которые не указаны не читаются
//    public void listenToPartition(
//            @Payload String message,
//            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
//            @Header(KafkaHeaders.OFFSET) int offset) {
//        log.info("Received Message3 [{}] from partition-{} with offset-{}",
//                message,
//                partition,
//                offset);
//    }


    @KafkaListener(topics = "topic-3", groupId = "user-group",
            containerFactory = "userKafkaListenerContainerFactory") // если не указывать будет использоваться строковый дефолтный Deserializer
    void listenerWithMessageConverter(User user) {
        log.info("Received message through MessageConverterUserListener [{}]", user);
    }


    @KafkaListener(topics = "topic-1", groupId = "group1")
    void listener(@Payload String message,
                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                  @Header(KafkaHeaders.GROUP_ID) String group)  {
        log.info("Received message [{}] from partition-{} in {}!", message, partition, group);
    }

    @KafkaListener(topics = "topic-1", groupId = "group2")
    void listener3(@Payload String message,
                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                   @Header(KafkaHeaders.GROUP_ID) String group) {
        log.info("Received message [{}] from partition-{} in {}!", message, partition, group);
    }

}
