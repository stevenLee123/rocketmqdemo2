package com.steven.mqorderdemo2;

import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @Author: StevenLee
 * @Email: 1144873128@qq.com
 * @Description:
 * @Date: create in 10:06 2019/5/26
 * @Modified By: 顺序消息发送者
 */
public class OrderProducer {

    private static final String namesrvAddr = "192.168.175.102:9876";
    public static void main(String[] args) {
        //创建DefaultMQProducer
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("steven_group");
        //设置namesrvAddr
        defaultMQProducer.setNamesrvAddr(namesrvAddr);
        try {
            defaultMQProducer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        Message message = null;
        //创建消息
        try {
            message = new Message("topic-demo",//主题
                    "tags",//消息过滤标志
                    "keys_5",//消息唯一值
                    "message5!".getBytes(RemotingHelper.DEFAULT_CHARSET)//消息body
            );
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        //发送消息
        try {
//            SendResult  result = defaultMQProducer.send(message);
            SendResult  result = defaultMQProducer.send(message,//消息
                   (List<MessageQueue> list, Message msg, Object o)-> { //指定消息Queue
                    Integer index = (Integer) o;
                    return list.get(index);
                },
                    1
            );
            System.out.println(result);
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //关闭生产者
        defaultMQProducer.shutdown();
    }
}
