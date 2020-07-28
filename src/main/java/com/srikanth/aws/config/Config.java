package com.srikanth.aws.config;

public class Config
{
    public static int thread_size;
    public static String kafka_brokers;
    //public static int kafka_message_count;
    public static String kafka_topic_name;
    public static String kafka_producer_id;
    public static String OFFSET_RESET_LATEST;
    public static String OFFSET_RESET_EARLIER;
    public static Integer MAX_POLL_RECORDS;
    public static String s3_retry_region;
    public static String s3_retry_directory;
    public static String s3_retry_bucket;

    public static void init()
    {
        thread_size = Integer.parseInt(System.getenv("lambda_thread_size"));
        kafka_brokers = System.getenv("kafka_brokers");
        //kafka_message_count = Integer.parseInt(System.getenv("kafka_message_count"));
        kafka_topic_name = System.getenv("kafka_topic_name");
        kafka_producer_id = System.getenv("kafka_producer_id");
        OFFSET_RESET_LATEST="latest";
        OFFSET_RESET_EARLIER="earliest";
        MAX_POLL_RECORDS=1;
        s3_retry_region = System.getenv("s3_retry_region");
        s3_retry_directory = System.getenv("s3_retry_directory");
        s3_retry_bucket = System.getenv("s3_retry_bucket");

    }
}
