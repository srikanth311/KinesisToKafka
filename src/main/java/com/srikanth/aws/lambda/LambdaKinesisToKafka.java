package com.srikanth.aws.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.srikanth.aws.config.Config;
import com.srikanth.aws.kafka.KafkaProducerManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

public class LambdaKinesisToKafka implements RequestHandler<KinesisEvent, Object>
{
    private final static Log LOGGER = LogFactory.getLog(LambdaKinesisToKafka.class);
    KafkaProducerManager kafkaProducerManager = new KafkaProducerManager();
    static
    {
        Config.init();
    }
    @Override
    public Object handleRequest(KinesisEvent input, Context context) {
        long startTime = System.currentTimeMillis();

        //RetryControl ret = new RetryControl();
        if (input.getRecords() != null) {
            LOGGER.info(String.format("Processing Started. Batch Size: [%s]", input.getRecords().size()));

            try {
                //this.retryControl = ret;
                try {
                    AtomicInteger count = new AtomicInteger(0);
                    ForkJoinPool forkJoinPool = new ForkJoinPool(Config.thread_size);
                    forkJoinPool.submit(() -> {
                        input.getRecords().parallelStream().forEach(record -> {

                            int cnt = count.addAndGet(1);
                            String sequenceNo = record.getKinesis().getSequenceNumber();
                            String partitionKey = record.getKinesis().getPartitionKey();
                            byte[] data = record.getKinesis().getData().array();
                            String shardId = record.getEventID().split(":")[0];
                            String streamName = record.getEventSourceARN().split(":")[5].split("/")[1];
                            LOGGER.info("Kinesis processing delay - " + (System.currentTimeMillis() - record.getKinesis().getApproximateArrivalTimestamp().getTime()) + "ms");
                            kafkaProducerManager.pushDataToKafka(data);
                            //dataProcess(streamName, shardId, sequenceNo, partitionKey, data, record.getKinesis().getApproximateArrivalTimestamp());
                            LOGGER.info(String.format("%s items end stream name [%s], shard ID [%s], sequence number [%s], partition key [%s]", cnt, streamName, shardId, sequenceNo, partitionKey));
                        });
                    }).get();

                } catch (Exception e) {
                    LOGGER.info("An unexpected error occurred. , Exception information : " + e.getMessage().toString());
                }
            }catch (Exception e) {
                LOGGER.info("An unexpected error occurred. , Exception information : " + e.getMessage().toString());
            }
        }
        else {
            LOGGER.info("No Records to process");
        }

        long stopTime = System.currentTimeMillis();
        LOGGER.info(String.format("Processing finished. Batch Size: [%s], Time take to process is : [%s]", input.getRecords().size(), stopTime-startTime));
        return null;
    }

    protected <T> T getException(Throwable t, Class<T> target) {
        if (t.getClass().equals(target)) return (T) t;
        if (t.getCause() != null) return getException(t.getCause(), target);
        return null;
    }

}
