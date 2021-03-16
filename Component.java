package com.example.demo;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.annotation.Async;

@org.springframework.stereotype.Component
public class Component {

   @Autowired
   RetryTemplate retrytemplate;
   
   int count=0;
   
   LocalDateTime dateTime=LocalDateTime.now();
   
   private KafkaStreams streams;
   
 

	/*
	 * @Autowired StreamInitilizer inilizer;
	 */
	
	
	  @EventListener(ApplicationStartedEvent.class) public void startApp() {
	  startProcessing(null); 
	  }
	 
	 	
	
	    
	    private void printMe() {
	    		String x=null;
	    		x.charAt(0);			
		}

	    
	    public  Properties getProp() 
	    {
		   final Properties props = new Properties();
	       props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	       props.put(StreamsConfig.APPLICATION_ID_CONFIG, "x19");
//	       props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//	       props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
	       
	       props.put(ConsumerConfig.GROUP_ID_CONFIG,"y2");
	       //props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,1000);
	       
	       //commit.interval.ms
	       //props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, 
	    	//	   StreamCustomeExceptionHandler.class);
	       props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, 
	    		   StreamCustomeExceptionHandler.class);
	       props.put("max.poll.interval.ms",200);

		   return props;	    
	    }

	 @Async
	  public void   startProcessing(StreamInitilizer initilizer) 
	  {	  
		   
				final StreamsBuilder builder = new StreamsBuilder();
				   KStream<String, String> data = builder.stream("test",Consumed.with(Serdes.String(),Serdes.String()));
				   
				   data.foreach((x,y)->{	    	   
					   
							String s=null;
							retrytemplate.execute(context->{
								
								System.out.println(ChronoUnit.SECONDS.between(dateTime, LocalDateTime.now()));
							   
								System.out.println("\n\n"+Thread.currentThread().getName()+"\n\n");
								System.out.println("key=="+x+" value:="+y);
								System.out.println("\n  count=="+count+"\n");
								count++;							
								
								s.charAt(0);								
								return true;
							});			
							
							
				   });		       
					
				streams = new KafkaStreams(builder.build(), getProp());

//				streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
//					@Override
//					public void uncaughtException(Thread arg0, Throwable arg1) {
//						initilizer.rerun();						
//					}		
//			    });
//				
				streams.start();						    
	  }
	
	public void retryAndProcess(){
		String s=null;
		retrytemplate.execute(context->{
			s.charAt(0);								
			return true;
		});			
	}
	 
}
