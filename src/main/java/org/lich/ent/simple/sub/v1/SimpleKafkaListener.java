package org.lich.ent.simple.sub.v1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class SimpleKafkaListener {

    private static final Logger log =
            LogManager.getLogger(SimpleKafkaListener.class);

    @KafkaListener(topics = "simple.msg.topic.v1")
    public void onMessageV1(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("Received Kafka message");
        log.info("Topic     : {}", record.topic());
        log.info("Partition : {}", record.partition());
        log.info("Offset    : {}", record.offset());
        log.info("Timestamp : {}", record.timestamp());
        log.info("Key       : {}", record.key());
        log.info("Message   : {}", record.value());
//        System.out.println(" test :> " + record.offset());

        acknowledgment.acknowledge();

        log.info("Message acknowledged. Offset: {}",
                record.offset());
    }
}
