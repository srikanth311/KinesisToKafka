package com.srikanth.aws.kafka;

import com.srikanth.aws.config.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.concurrent.ExecutionException;

public class KafkaProducerManager {
    private Producer<Long, byte[]> producer;
    private final static Log LOGGER = LogFactory.getLog(KafkaProducerManager.class);

    public KafkaProducerManager() {
        producer = KafkaProducerCreator.createProducer();
    }

    public void pushDataToKafka(byte [] dataFromKinesis) {
        ProducerRecord<Long, byte[]> record = new ProducerRecord<Long, byte[]>(Config.kafka_topic_name,
                dataFromKinesis);
        try {
            LOGGER.info("Message from Kinesis stream is : " + dataFromKinesis.toString());
            RecordMetadata metadata = producer.send(record).get();
            LOGGER.info("Record sent with key to partition " + metadata.partition()
                    + " with offset " + metadata.offset());
        } catch (ExecutionException e) {
            LOGGER.error(String.format("Error in sending record to Kafka "));
        } catch (InterruptedException e) {
            LOGGER.error(String.format("Error in sending record to Kafka"));
            LOGGER.error(e);
        }
        catch (Exception e)
        {
            LOGGER.error("Error ::: General Exception in KafkaProducerManager class.");
            LOGGER.error(e);
        }
    }
}

