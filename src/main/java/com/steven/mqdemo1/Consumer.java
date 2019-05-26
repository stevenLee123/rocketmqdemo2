package com.steven.mqdemo1;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @Author: StevenLee
 * @Email: 1144873128@qq.com
 * @Description:
 * @Date: create in 10:52 2019/5/26
 * @Modified By: rocketmq 消费者
 */
public class Consumer {

    private static final String nameSrvAddr = "192.168.175.102:9876";
    public static void main(String[] args) {

//        创建 消费者 DefaultMQPushConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("steven_group");
//        设置namesrv地址
        consumer.setNamesrvAddr(nameSrvAddr);
//        设置消息拉取上限
        consumer.setConsumeMessageBatchMaxSize(10);
//        设置subscribe，这里主要读取主题信息
        try {
            consumer.subscribe("topic-demo",
                    "tags"
                    );
        } catch (MQClientException e) {
            e.printStackTrace();
        }
//        创建消息监听MessageListener
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //        获取消息信息
                //迭代消息
                for (MessageExt msg:list) {
                    System.out.println(msg.getTopic());
                    try {
                        System.out.println(new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                    System.out.println(msg.getTags());
                }
                //        返回消息读取状态
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
//        开启consumer
        try {
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }


//
    }
}
