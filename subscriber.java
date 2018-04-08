package jmsproject;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import javax.jms.JMSException;

import javax.jms.Message;

import javax.jms.MessageConsumer;

import javax.jms.MessageListener;

import javax.jms.Queue;

import javax.jms.QueueSession;

import javax.jms.Session;

import javax.jms.TextMessage;

import javax.naming.Context;

import javax.naming.NamingException;

public class subscriber implements MessageListener {

	public static void main(String[] args) throws JMSException, NamingException, InterruptedException {
Connection connection = null;
		
		try {
			Context context = ContextUtil.getInitialContext();
			ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("ConnectionFactory");
			connection = connectionFactory.createConnection();
			Session session = connection.createSession(false,QueueSession.AUTO_ACKNOWLEDGE);
			Queue queue = (Queue) context.lookup("/queue/CouponQueue");
			connection.start();
			MessageConsumer consumer = session.createConsumer(queue);
			
			while(true)
			{
            consumer.setMessageListener(new subscriber()); 
            Thread.sleep(2000);
			}

        } finally {

            if (connection != null) {

                System.out.println("close the connection");

                connection.close();

            }

        }

    }



    public void onMessage(Message message) {

        try {

            System.out.println("Coupon recommendation received");

            System.out.println(((TextMessage) message).getText());

        } catch (JMSException e) {

            e.printStackTrace();

        }

    }


		// TODO Auto-generated method stub

	}


