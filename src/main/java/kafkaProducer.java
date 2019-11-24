import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
import java.util.Random;

public class kafkaProducer {
    public static void main(String[] args) {

        //创建一个配置文件参数
        Properties props = new Properties();
        props.put("metadata.broker.list", "luo2:9092,luo3:9092");//指定broker两个以上的地址
        //          kafka.serializer.StringEncoder
        props.put("serializer.class", "kafka.serializer.StringEncoder");//指定文件的系列化类型
        props.put("zookeeper.connect", "luo1:2181,luo2:2181,luo3:2181");//zk集群

        ProducerConfig config = new ProducerConfig(props);
        //创建一个produce
        Producer<String, String> producer = new Producer<>(config);

        //读取日志文件
        try {

            BufferedReader reader = new BufferedReader(new FileReader(new File("D:\\input\\flumeLoggerapp4.log.20170412")));
            String line = null;
            while (null != (line = reader.readLine())) {

                System.out.println(line);
                KeyedMessage<String, String> message = new KeyedMessage<String, String>("jsonData",new Random().nextInt(3)+"",line);
                Thread.sleep(300);
                producer.send(message);
            }

            producer.close();
            reader.close();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
