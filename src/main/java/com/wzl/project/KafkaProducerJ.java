package com.wzl.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class KafkaProducerJ {
    public static void main(String[] args) throws Exception{

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.157.157:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "pktest";

        while(true){
            StringBuilder sb = new StringBuilder();
            sb.append("Mark").append("\t");
            sb.append("CN").append("\t");
            sb.append(getLevels()).append("\t");
            sb.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t");
            sb.append(getIps()).append("\t");
            sb.append(getDomains()).append("\t");
            sb.append(getTraffics()).append("\t");

            System.out.println(sb.toString());

            producer.send(new ProducerRecord<String, String>(topic, sb.toString()));

            Thread.sleep(2000);
        }
    }

    private static long getTraffics() {
        return new Random().nextInt(1000);
    }

    private static String getDomains() {
        String[] domains = new String[]{
                "v1.go2yd.com",
                "v2.go2yd.com",
                "v3.go2yd.com",
                "v4.go2yd.com",
                "vmi.go2yd.com",
        };
        return domains[new Random().nextInt(domains.length)];
    }

    private static String getIps() {
        return "" + new Random().nextInt(255) + "."
                + new Random().nextInt(255) + "."
                + new Random().nextInt(255) + "."
                + new Random().nextInt(255);
    }

    public static String getLevels(){
        String[] levels = new String[]{"M", "E"};
        return levels[new Random().nextInt(levels.length)];
    }
}
