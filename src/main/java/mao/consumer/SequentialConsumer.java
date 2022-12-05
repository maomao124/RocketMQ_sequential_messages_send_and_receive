package mao.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Project name(项目名称)：RocketMQ_顺序消息的发送与接收
 * Package(包名): mao.consumer
 * Class(类名): SequentialConsumer
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/5
 * Time(创建时间)： 13:57
 * Version(版本): 1.0
 * Description(描述)： 消费者-顺序消息
 */

public class SequentialConsumer
{
    /**
     * 得到int随机
     *
     * @param min 最小值
     * @param max 最大值
     * @return int
     */
    public static int getIntRandom(int min, int max)
    {
        if (min > max)
        {
            min = max;
        }
        return min + (int) (Math.random() * (max - min + 1));
    }

    public static void main(String[] args) throws MQClientException
    {
        //消费者
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("mao_group");
        //设置nameserver地址
        defaultMQPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        //设置消费模式-负载均衡
        defaultMQPushConsumer.setMessageModel(MessageModel.CLUSTERING);
        //设置消费的主题
        defaultMQPushConsumer.subscribe("test_topic", "*");
        //注册监听器
        defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderly()
        {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext)
            {
                //线程
                Thread currentThread = Thread.currentThread();
                //打印，每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
                for (MessageExt messageExt : list)
                {
                    System.out.println("当前线程：" + currentThread.getName() + "   " + "队列id：" + messageExt.getQueueId()
                            + "      " + "消息：" + new String(messageExt.getBody(),
                            StandardCharsets.UTF_8));
                    try
                    {
                        Thread.sleep(getIntRandom(100, 500));
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
                //返回成功
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        //启动
        defaultMQPushConsumer.start();
    }
}
