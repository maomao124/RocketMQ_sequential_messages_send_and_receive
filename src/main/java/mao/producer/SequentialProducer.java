package mao.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Project name(项目名称)：RocketMQ_顺序消息的发送与接收
 * Package(包名): mao.producer
 * Class(类名): SequentialProducer
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/5
 * Time(创建时间)： 13:34
 * Version(版本): 1.0
 * Description(描述)： 顺序消息-生产者
 */

public class SequentialProducer
{
    /**
     * 简单日期格式
     */
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");

    public static void main(String[] args)
            throws MQClientException, MQBrokerException, RemotingException, InterruptedException
    {
        //生产者
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("mao_group");
        //设置nameserver地址
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        //启动生产者
        defaultMQProducer.start();
        //发送300条顺序消息
        for (int i = 0; i < 300; i++)
        {
            //消息体
            String messageBody = simpleDateFormat.format(new Date()) + " --> 消息" + i;
            //消息对象
            Message message = new Message("test_topic", messageBody.getBytes(StandardCharsets.UTF_8));
            defaultMQProducer.send(message, new MessageQueueSelector()
            {
                /**
                 * 选择发送的队列
                 *
                 * @param list    列表
                 * @param message 消息
                 * @param o       o
                 * @return {@link MessageQueue}
                 */
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o)
                {
                    System.out.println(messageBody + "  发送到0号队列，队列总数：" + list.size());
                    return list.get(0);
                }
            }, i);
        }
        //关闭
        defaultMQProducer.shutdown();
    }
}
