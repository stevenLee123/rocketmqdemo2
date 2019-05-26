package com.steven.mqtransactiondemo3;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * @Author: StevenLee
 * @Email: 1144873128@qq.com
 * @Description:
 * @Date: create in 10:06 2019/5/26
 * @Modified By: 消息发送者
 */
public class TransactionProducer {

    private static final String namesrvAddr = "192.168.175.102:9876";
    public static void main(String[] args) {
        //创建DefaultMQProducer
//        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("steven_group");
        //使用transectionMQProducer
        TransactionMQProducer producer = new TransactionMQProducer("transaction-steven_group");

        //设置namesrvAddr
        producer.setNamesrvAddr(namesrvAddr);
        //指定消息监听对象，用户执行本地事务和消息回查
        TransactionListener transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);

        //创建线程池
        ExecutorService executorService = new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(
                        2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check");
                        return thread;
                    }
                }
        );
        producer.setExecutorService(executorService);
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        Message message = null;
        //创建消息
        try {
            message = new Message("topic-demo2",//主题
                    "tags",//消息过滤标志
                    "keys_2",//消息唯一值
                    "transaction message2!".getBytes(RemotingHelper.DEFAULT_CHARSET)//消息body
            );
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        //发送消息
        try {
            TransactionSendResult result = producer.sendMessageInTransaction(message,
                    "hello-transaction");
            System.out.println(result);
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        //关闭生产者
        producer.shutdown();
    }
}
