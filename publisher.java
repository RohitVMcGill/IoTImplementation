package jmsproject;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import javax.jms.Message;

import javax.jms.MessageProducer;

import javax.jms.Queue;

import javax.jms.QueueSession;

import javax.jms.Session;

import javax.naming.Context;

import javax.naming.NamingException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Paths;
import java.nio.file.Files;




public class publisher {

	public static void main(String[] args) throws JMSException, NamingException, InterruptedException, IOException {
		
		Connection connection = null;
		final String SAMPLE_CSV_FILE_PATH = "/Users/jasvi/eclipse-workspace/jmsproject/input/user_list.csv";

		
		try {
			Context context = ContextUtil.getInitialContext();
			ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("ConnectionFactory");
			connection = connectionFactory.createConnection();
			Session session = connection.createSession(false,QueueSession.AUTO_ACKNOWLEDGE);
			Queue queue = (Queue) context.lookup("/queue/CouponQueue");
			connection.start();
			MessageProducer producer = session.createProducer(queue);
			
			// Read a record from csv file
			// open csv file
			
			Reader reader = Files.newBufferedReader(Paths.get(SAMPLE_CSV_FILE_PATH));
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            
            String age = null;
            String coupon = null;
            for (CSVRecord csvRecord : csvParser) {
                // Accessing Values by Column Index
                age = csvRecord.get(2);
                coupon = csvRecord.get(5);
                Message couponRecord = session.createTextMessage(age + "," + coupon);
    			producer.send(couponRecord);
    			Thread.sleep(500);

}
				
		} 
		finally {
			if (connection != null) {
				System.out.println("close the connection");
				connection.close();
			}
			
		}
		// TODO Auto-generated method stub

	}

}
