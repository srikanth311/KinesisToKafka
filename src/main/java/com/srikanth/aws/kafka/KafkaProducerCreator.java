package com.srikanth.aws.kafka;

import com.srikanth.aws.config.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;

public class KafkaProducerCreator
{
    private final static Log LOGGER = LogFactory.getLog(KafkaProducerCreator.class);
    public static Producer<Long, byte[]> createProducer() {

        Properties props = new Properties();
        try {
            //System.out.println("Kafka - Producer : " + Config.kafka_brokers);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.kafka_brokers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, Config.kafka_producer_id);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        }
        catch (Exception e)
        {
            LOGGER.info("ERROR - In KafkaProducerCreator class");
            LOGGER.info(e);
        }
        return new KafkaProducer<Long, byte[]>(props);
    }
}
