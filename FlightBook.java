package com.flight.flightbook;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;			
import com.datastax.driver.core.Metadata;			
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;			
import com.datastax.driver.core.Row;			
import com.datastax.driver.core.Session;	
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

// Defining the class
public class FlightBook  {			
   private Cluster cluster;			
   private Session session;			

// Connecting to Cassandra Cluster
   public void connect(String node) {			
      cluster = Cluster.builder()			
            .addContactPoint(node).build();			
      Metadata metadata = cluster.getMetadata();			
      System.out.printf("Connected to cluster: %s\n", 			
            metadata.getClusterName());			
      for ( Host host : metadata.getAllHosts() ) {			
         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",			
               host.getDatacenter(), host.getAddress(), host.getRack());			
      }			
      session = cluster.connect();			
   }			
			
 			
// The method to perform required operation
  public void queryData() {
	  System.out.println("Please enter the date as DD-Mon-YY (Eg : 12-Dec-13) : "); // Accepting user input for date
	  Scanner dateinp = new Scanner(System.in);
	  String input = dateinp.next();
// Querying the data based on date and displaying particular flight IDs for a provided date
	  PreparedStatement ps = session.prepare("SELECT * FROM flight.booking_1 " +
			  "WHERE booking_date = ? ");
	  BoundStatement bs = ps.bind().setString(0, input);
	 
	  ResultSet results = session.execute(bs);
	  String date = "";
	  StringBuffer temp1 = new StringBuffer();
	  
	  int size = results.getColumnDefinitions().size(); // no of columns returned/fetched.
	  for (Row row : results) {
		  for(int i=1;i<size;i++) {
			  temp1.append("'"+row.getString(i)+"',");
			 
		  }
		  int lastComma = temp1.lastIndexOf(",");
		  temp1.deleteCharAt(lastComma);		
		  date = row.getString(0);
		  System.out.println("\nResult2 for: "+date+", "+temp1);
		  }
/* 	Querying the data taking input as the various flight IDs from the above query and displaying details of 
	  all flight IDs for user's selection */ 
	  PreparedStatement ps1 = session.prepare("SELECT * FROM flight.flight_details " +
			  "WHERE flight_id in ( "+temp1+" ) ");
	  BoundStatement bs1 = ps1.bind();
	  ResultSet results1 = session.execute(bs1);
	  int size1 = results.getColumnDefinitions().size();
	  StringBuffer temp2 = null;
	  for (Row row : results1) {
		  temp2 = new StringBuffer();
		  String flightId = row.getString(0);
		  for(int i=1;i<size1;i++) {			 
			  if("decimal".equals(row.getColumnDefinitions().getType(i).toString())) {
				  temp2.append(row.getDecimal(i)+" # ");
			  }
			  else {
			  temp2.append(row.getString(i)+" # ");
			  }
		  }
		  int lastComma = temp2.lastIndexOf(" # ");
		  temp2.deleteCharAt(lastComma);
		  System.out.println("\nResult Flight for: "+flightId+"\n "+temp2);		  
	  }
	  
// Asking user to select a flight ID from the above output
	  System.out.println("Please select a flightId : ");
	  Scanner flightinp = new Scanner(System.in);
	  String Selflight = flightinp.next();
	  
// Asking user for the number of seats to be booked
	  System.out.println("Please mention seats : ");
	  Scanner seatinp = new Scanner(System.in);
	  int no = seatinp.nextInt();
	  BigDecimal seats = null;
	  results1 = session.execute(bs1);
	  for (Row row : results1) {
		  String flightid1=row.getString(0);

		  if(flightid1.equals(Selflight)){
			seats=row.getDecimal(6); 

		  }	  
	  }
	  
	  int seats1 = seats.intValue();
	  int updseat=seats1-no;
	  
	  System.out.println("\n Available Seats for : "+Selflight+"="+updseat);
// Updating the available seats after booking into the flight details table	  
	  Statement update = QueryBuilder.update("flight","flight_details") 
			  .with(QueryBuilder.set("seats", updseat))
			  .where((QueryBuilder.eq("flight_id",Selflight)));
			session.execute(update);
// Inserting the transaction record into transaction table
			 PreparedStatement ps4 = session.prepare("SELECT count(*) FROM flight.transaction");
			  BoundStatement bs4 = ps4.bind();
			  ResultSet results4 = session.execute(bs4);
			  long trn = 0;
			  for (Row row : results4) {
				  trn=row.getLong(0);

			  }

			  DateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy");
			  Date date1 = new Date();
			  String mydate = dateFormat.format(date1);
			
			  trn=trn++;
			Statement insert = QueryBuilder.insertInto("flight", "transaction")
					.value("transaction_id",trn )
					.value("booked_seats", no)
					.value("booking_date", mydate)
					.value("flight_id",Selflight);
			session.execute(insert);

			System.out.println("\n Thank you for your booking !");
						
   }			

 // Closing the Cassandra cluster
 
  public void close() {			
	     cluster.close();			
	   }

  // The Main method
   public static void main(String[] args) {			
	   FlightBook client = new FlightBook();			
      client.connect("127.0.0.1");			
      client.queryData();			
      client.close();			
   }			
}			
