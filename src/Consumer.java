import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
	
	public static void  main (String[] args) throws IOException {
		Properties config = new Properties();
		//config.
		config.setProperty("client.id", InetAddress.getLocalHost().getHostName());
		config.setProperty("bootstrap.servers","cp-kafka-image-uploader.apps.ltospsbx.osp.ngco.com:9092");
		config.put("group.id", "all");
		config.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		config.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		KafkaConsumer<String, String> consumer = new KafkaConsumer(config);
		consumer.subscribe(Collections.singletonList("pageviews"));
		 while (true) {
			 ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
			 if (consumerRecords.count() == 0) {
				 System.out.println("no record");
				 break;
			 }else
			 {
				 consumerRecords.forEach(record -> {
		              System.out.println("Record Key " + record.key());
		              System.out.println("Record value " + record.value());
		              System.out.println("Record partition " + record.partition());
		              System.out.println("Record offset " + record.offset());
		           });
				 
			 }
			 
		 }
		
		
		
		 
	}

}
