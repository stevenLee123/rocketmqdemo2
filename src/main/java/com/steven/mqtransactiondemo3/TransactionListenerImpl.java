package com.steven.mqtransactiondemo3;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: StevenLee
 * @Email: 1144873128@qq.com
 * @Description:
 * @Date: create in 11:58 2019/5/26
 * @Modified By:
 */
public class TransactionListenerImpl implements TransactionListener {
    //存储对应事务的状态信息， key 事务id ，value：当前事务执行的状态
    private ConcurrentHashMap<String,Integer> localTrans = new ConcurrentHashMap<String,Integer>();
    /**
     * 本地事务
     * @param message
     * @param o
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        String transationId = message.getTransactionId();
//        用0代表执行中
        localTrans.put(transationId,0);

        //业务执行，本地事务
        System.out.println("transaction message ");

        try {
            System.out.println("=======开始执行");
            Thread.sleep(10000);
            System.out.println("============结束执行-=========");
            localTrans.put(transationId,1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            localTrans.put(transationId,2);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }

    /**
     * 消息回查
     * @param messageExt
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        //获取事务id状态
        String transactionId = messageExt.getTransactionId();

        Integer status = localTrans.get(transactionId);
        System.out.println("==============消息回查===============transactionId"+transactionId+" status:"+status);
        switch (status){
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.UNKNOW;
    }
}
