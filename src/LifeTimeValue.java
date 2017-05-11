import java.sql.* ;  
import java.io.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class LifeTimeValue {
	static int lines_in_file;
	
	
	//function to calculate total number of lines in given input JSON file
	public static int get_lines(String file_path, String file_name){
		try{
			
			BufferedReader reader = new BufferedReader(new FileReader(file_path+ "\\" + file_name));
			int lines = 0;
			while (reader.readLine() != null) lines++;
			reader.close();
			lines_in_file = lines;
			//System.out.println(lines);
		}catch(IOException ie){
            //Handle errors for IO
        	 System.out.println("IOException raised..");
            ie.printStackTrace();
		}
		
		return lines_in_file;
	}
	
	
	
	//ingest(e,D) method to store event from input JSON to mysql datastore called 'shutterfly'
	public static void ingest(JSONObject jo,Connection mysql)
	{
		String type = (String) jo.get("type");
		switch(type)
		{
			case "CUSTOMER":
				ingest_customer(jo,mysql);    //store customer event to customer table in shutterfly database
				break;
			case "SITE_VISIT":
				ingest_site_visit(jo,mysql);   //store site_visit event to site_visit table in shutterfly database
				break;
			case "IMAGE":
				ingest_image(jo,mysql);         //store image event to image table in shutterfly database
				break;
			case "ORDER":
				ingest_order(jo,mysql);          //store order event to order table in shutterfly database  
				break;				
		}
	}
	
	//convert Timestamp value from input JSON ("yyyy-mm-ddThh:MM:ss.mmmZ") to ("yyyy-mm-dd hh:MM:ss.mmm")
	public static Timestamp getTime(String timeStr) {
	    if (timeStr.toUpperCase().contains("T") && timeStr.toUpperCase().contains("Z")) 
	    {
	            timeStr = timeStr.toUpperCase().replace("T", " ");
	            timeStr = timeStr.toUpperCase().replace("Z", "");
        }   
	    
        Timestamp timeCreated = Timestamp.valueOf(timeStr);
        timeCreated.toString(); 
	    return timeCreated;
	}
	
	
	//store customer event to customer table in shutterfly database
	public static void ingest_customer(JSONObject jo,Connection mysql)
	{
		//parse event jo
		String verb = (String) jo.get("verb");
		
		String cust_key = (String)jo.get("key");
		String event_time = (String)jo.get("event_time");
		String last_name = (String)jo.get("last_name");
		String adr_city = (String)jo.get("adr_city");
		String adr_state = (String)jo.get("adr_state");
		
		Timestamp event_t = getTime(event_time);
		
		if (verb.equals("NEW"))  //insert new customer record to table
		{
			
			try{
				
				// the mysql insert statement
	            String query = " insert into customer (cust_key,event_time,last_name,addr_city,addr_state)"
	              + " values (?, ?, ?, ?, ?)";
	
	            // create the mysql insert preparedstatement
	            PreparedStatement preparedStmt = mysql.prepareStatement(query);
	            
	            preparedStmt.setString (1, cust_key);
	            preparedStmt.setTimestamp (2, event_t);
	            preparedStmt.setString   (3, last_name);
	            preparedStmt.setString (4, adr_city);
	            preparedStmt.setString    (5,adr_state);
	
	            // execute the preparedstatement
	            preparedStmt.execute();
            
			}catch(SQLException se){
	            //Handle errors for SQL
	        	 System.out.println("SQLException Raised..");
	        	 se.printStackTrace();
	         }
			
			
		}
		
		else if (verb.equals("UPDATE"))  //update existing customer record in the table
		{
			try{
				String query = "update customer set event_time = ?, last_name=?,addr_city=?,addr_state=? where cust_key = ?";
				
			      PreparedStatement preparedStmt = mysql.prepareStatement(query);
			      preparedStmt.setTimestamp  (1, event_t);
			      preparedStmt.setString(2, last_name);
			      preparedStmt.setString(3, adr_city);
			      preparedStmt.setString(4,adr_state);
			      preparedStmt.setString(5, cust_key);
	
			      // execute the java preparedstatement
			      preparedStmt.executeUpdate();
			}catch(SQLException se){
	            //Handle errors for SQL
	        	 System.out.println("SQLException Raised..");
	            se.printStackTrace();
	         }
		}
	}
	
	
	//store site_visit event to site_visit table in shutterfly database
	public static void ingest_site_visit(JSONObject jo,Connection mysql)
	{
		String verb = (String) jo.get("verb");
		String tagvalues = "";
		String visit_key = (String)jo.get("key");
		String event_time = (String)jo.get("event_time");
		String cust_id = (String)jo.get("customer_id");
		//to get tags array from input JSON event 
		JSONArray tags = (JSONArray)jo.get("tags");
		for (Object tagsobj:tags.toArray())
		{
			JSONObject jtags = (JSONObject)tagsobj;
			String some_value = (String)jtags.get("some key");
			if (tagvalues.equals("") )
			{
				tagvalues = some_value;
				
			}
			else
			{
				tagvalues = "," + some_value;
			}
			//System.out.println(some_value);
		}
		
		
		Timestamp event_t = getTime(event_time);
		
		if (verb.equals("NEW")) //insert new record
		{
			
			try{
				
				// the mysql insert statement
	            String query = " insert into site_visit (visit_key,event_time,customer_id,tags)"
	              + " values (?, ?, ?, ?)";
	
	            // create the mysql insert preparedstatement
	            PreparedStatement preparedStmt = mysql.prepareStatement(query);
	            
	            preparedStmt.setString (1,visit_key);
	            preparedStmt.setTimestamp (2, event_t);
	            preparedStmt.setString   (3, cust_id);
	            preparedStmt.setString (4, tagvalues);
	            
	
	            // execute the preparedstatement
	            preparedStmt.execute();
            
			}catch(SQLException se){
	            //Handle errors for SQL
	        	 System.out.println("SQLException raised..");
	            se.printStackTrace();
	         }
			
			
		}
		
		else if (verb.equals("UPDATE"))   //update existing record
		{
			try{
				String query = "update site_visit set event_time = ?, customer_id=?,tags=? where visit_key = ?";
				
			      PreparedStatement preparedStmt = mysql.prepareStatement(query);
			      preparedStmt.setTimestamp  (1, event_t);
			      preparedStmt.setString(2,cust_id);
			      preparedStmt.setString(3, tagvalues);
			      preparedStmt.setString(4,visit_key);
			      	
			      // execute the java preparedstatement
			      preparedStmt.executeUpdate();
			      
			}catch(SQLException se){
	            //Handle errors for SQL
	        	 System.out.println("SQLException raised..");
	            se.printStackTrace();
	         }
		}
	}
	
	
	
	
	//store image event to image table in shutterfly database
	public static void ingest_image(JSONObject jo,Connection mysql)
	{
		String verb = (String) jo.get("verb");
		
		String img_key = (String)jo.get("key");
		String event_time = (String)jo.get("event_time");
		String cust_id = (String)jo.get("customer_id");
		String cam_make = (String)jo.get("camera_make");
		String cam_model = (String)jo.get("camera_model");
		
		Timestamp event_t = getTime(event_time);
		
		//insert record to image database
			try{
				
				// the mysql insert statement
	            String query = " insert into image (img_key,event_time,customer_id,camera_make,camera_model)"
	              + " values (?, ?, ?, ?, ?)";
	
	            // create the mysql insert preparedstatement
	            PreparedStatement preparedStmt = mysql.prepareStatement(query);
	            
	            preparedStmt.setString (1, img_key);
	            preparedStmt.setTimestamp (2, event_t);
	            preparedStmt.setString   (3, cust_id);
	            preparedStmt.setString (4, cam_make);
	            preparedStmt.setString    (5,cam_model);
	
	            // execute the preparedstatement
	            preparedStmt.execute();
            
			}catch(SQLException se){
	            //Handle errors for SQL
	        	 System.out.println("SQLException raised..");
	            se.printStackTrace();
	         }			
		}
	
	
	
	//store order event to order table in shutterfly database
	public static void ingest_order(JSONObject jo,Connection mysql)
	{
		String verb = (String) jo.get("verb");
		
		String order_key = (String)jo.get("key");
		String event_time = (String)jo.get("event_time");
		String cust_id = (String)jo.get("customer_id");
		String amnt = (String)jo.get("total_amount"); 
		
		//convert amount from format "00.00 USD"  to 00.00
		double total_amnt = Float.valueOf(amnt.split(" ")[0]);
		
		total_amnt = Math.round(total_amnt * 100.0) / 100.0;  //round to 2 decimal places after decimal point
		
		Timestamp event_t = getTime(event_time);
		
		//System.out.println(order_key + ',' + event_t + ',' + cust_id + ','+total_amnt);
		
		if (verb.equals("NEW")) //insert new record
		{
			
			try{
				
				// the mysql insert statement
	            String query = " insert into shutterfly.order (order_key,event_time,customer_id,total_amount)"
	              + " values (?, ?, ?, ?)";
	
	            // create the mysql insert preparedstatement
	            PreparedStatement preparedStmt = mysql.prepareStatement(query);
	            
	            preparedStmt.setString (1,order_key);
	            preparedStmt.setTimestamp (2, event_t);
	            preparedStmt.setString   (3, cust_id);
	            preparedStmt.setDouble (4, total_amnt);
	            
	
	            // execute the preparedstatement
	            preparedStmt.execute();
            
			}catch(SQLException se){
	            //Handle errors for SQL
	        	 System.out.println("SQLException raised..");
	            se.printStackTrace();
	         }
			
			
		}
		
		else if (verb.equals("UPDATE"))  //update existing record
		{
			try{
				String query = "update shutterfly.order set event_time = ?, customer_id=?,total_amount=? where order_key = ?";
				
			      PreparedStatement preparedStmt = mysql.prepareStatement(query);
			      preparedStmt.setTimestamp  (1, event_t);
			      preparedStmt.setString(2,cust_id);
			      preparedStmt.setDouble(3, total_amnt);
			      preparedStmt.setString(4,order_key);
			      	
			      // execute the java preparedstatement
			      preparedStmt.executeUpdate();
			      
			}catch(SQLException se){
	            //Handle errors for SQL
	        	 System.out.println("SQLException raised..");
	            se.printStackTrace();
	         }
		}
	}
	
	
	
	//calculate top x customers with high LTV
	public static void TopXSimpleLTVCustomers(int x, Connection mysql)
	{
		
		try
		{
			Statement stmt = mysql.createStatement();
			Statement stmt_visit = mysql.createStatement();
			Statement stmt_order = mysql.createStatement();
	        String sql;
	        String visit_sql;
	        String order_sql;
	        ResultSet rs_visit;
	        ResultSet rs_order;
	        ResultSet rs;
			
	        
	        long start_time = 0;
	        long end_time = 0;
	        
	        double amnt = 0;
	        int num_visits = 0;   
	        
        
    // for each customer in customer table    
	        sql = "SELECT cust_key FROM customer";
	        rs = stmt.executeQuery(sql);
	       
	        
	        while(rs.next())
	        {
	           
		           String cust_key  = rs.getString("cust_key");
		           
		           //get all timestamps from site_visit table
		           
		           visit_sql = "SELECT unix_timestamp(event_time) as event_time_u FROM site_visit where customer_id like '" + cust_key +"'";
		           rs_visit = stmt_visit.executeQuery(visit_sql);
		           
		           while(rs_visit.next())
		           {
		        	   num_visits = num_visits +1;
		        	   long event_t = rs_visit.getLong("event_time_u");
		        	   
		        	   //from all timestamps for a customer calculate start and end timestamp for site visit
		        	   if (start_time == 0)
		        	   {
		        		   start_time = event_t;
		        		   end_time = event_t;    
		        	   }
		        	   else if (event_t < start_time)
		        	   {
		        		   start_time = event_t;
		        	   }
		        	   else if (event_t > end_time)
		        	   {
		        		   end_time = event_t;
		        	   }
		        	 
		        	   
		           }
		           
		           rs_visit.close();
		           
		           
		           //for each customer calculate total expenditure 
		           order_sql = "SELECT total_amount from shutterfly.order where customer_id like '" + cust_key + "'";
		           rs_order = stmt_order.executeQuery(order_sql);
		           
		           while(rs_order.next())
		           {
		        	   amnt = amnt + rs_order.getDouble("total_amount");
		           }
		           amnt = Math.round(amnt * 100.0) / 100.0;
		           //System.out.println(cust_key + "," + start_time + "," + end_time + "," + amnt + "," + num_visits);
		           
		           rs_order.close();
	               
		           int visits_per_week = 0;
		           
		           //calculate total time span for site visit
		           long sub = end_time - start_time;
		           //System.out.println(sub);
		           
		           //calculate num_weeks for total time span
		           int num_weeks = (int) Math.ceil((double)sub/(24*7*3600));
		           //System.out.println(num_weeks);
		           
		           //calculate visits_per_week
		           visits_per_week = num_visits/num_weeks;
		           
		           //calculate expenditure per visit
		           double exp_per_visit = amnt/num_visits;
		           exp_per_visit = Math.round(exp_per_visit * 100.0) / 100.0;
		           //System.out.println(visits_per_week + "," + exp_per_visit);
		           
		           //calculate a = the average customer value per week (customer expenditures per visit (USD) x number of site visits per week)
		           double a = exp_per_visit * visits_per_week;
		           a = Math.round(a *100.0)/100.0;
		           //System.out.println(a);
		           
		           //t is the average customer lifespan. The average lifespan for Shutterfly is 10 years.
		           double t= 10;
		           
		           //calculating ltv
		           double ltv = (52*a)/t;
		           ltv = Math.round(ltv *100.0)/100.0;
		           //System.out.println(ltv);
		           
		           
		           //store ltv for each customer in another table called 'ltv' so as to have ltv data for all customers avaialble at all times
		           store_to_db(cust_key,ltv,mysql);
		           
	        }
	        
            stmt_order.close();
            
            stmt_visit.close();
	        rs.close();
            
            stmt.close();
            
            //get data for top x customers with higher LTV from 'ltv' table and write to file
            write_topx_cust_file(x,mysql);
            
            
	   }catch(SQLException se)
		{
	        //Handle errors for JDBC
	    	 System.out.println("Sql Exception");
	        se.printStackTrace();
		}
		
     }
	
	
	//store ltv for each customer
	public static void store_to_db(String cust_key,double ltv,Connection mysql)
	{
		
		
		try{
			
			Statement stmt_s = mysql.createStatement();
			
	        String sql;
	        
	        ResultSet rs_s;
	        
	        sql = "SELECT ltval from ltv where cust_id like '" + cust_key + "'";
	        rs_s = stmt_s.executeQuery(sql);
	        
	        if(rs_s.next())  //if ltv for a customer already exists in the table, update ltv
	        {
	        	String u_query = " update ltv set ltval = ? where cust_id = ?";

	                  // create the mysql insert preparedstatement
	                  PreparedStatement preparedStmt1 = mysql.prepareStatement(u_query);
	                  
	                  preparedStmt1.setDouble (1,ltv);
	                  preparedStmt1.setString (2, cust_key);
	                  
	                  // execute the preparedstatement
	                  preparedStmt1.execute();
	        }
	        
	        else{  //insert ltv for new customer
			
				// the mysql insert statement
	            String query = " insert into ltv (cust_id,ltval)"
	              + " values (?, ?)";
	
	            // create the mysql insert preparedstatement
	            PreparedStatement preparedStmt = mysql.prepareStatement(query);
	            
	            preparedStmt.setString (1,cust_key);
	            preparedStmt.setDouble (2, ltv);
	            
	            // execute the preparedstatement
	            preparedStmt.execute();
	        }
	        
	        rs_s.close();
	        stmt_s.close();
        
		}catch(SQLException se){
            //Handle errors for JDBC
        	 System.out.println("in se");
            se.printStackTrace();
         }
	}
	
	
	//write data for top x customers with higher LTV
	public static void write_topx_cust_file(int x,Connection mysql)
	{
		
		String file_path = "C:\\Users\\purva\\workspace\\Test\\src";
		String file_name = "output.csv";
		//System.out.format("%s%s%s%s%s\n","Customer Name","  ","Customer Address","  ","Lifetime Value");
		try
		{
		
			Statement stmt = mysql.createStatement();			
	        String sql;	        
	        ResultSet rs;
	        
	        Statement stmt_cust = mysql.createStatement();			
	        String sql_cust;	        
	        ResultSet rs_cust;
	        
	        PrintWriter writer = new PrintWriter(file_path + "\\" +file_name,"UTF-8");
			writer.println("Customer Name,Customer Address,Lifetime Value");
			
			
	        sql = "SELECT  DISTINCT *  FROM ltv ORDER BY ltval desc LIMIT " + x;
	        rs = stmt.executeQuery(sql);
	        
	        while(rs.next())
	        {
	        	String cust_key = rs.getString("cust_id");
	        	Double ltv = rs.getDouble("ltval");
	        	
	        	sql_cust = "select last_name,addr_city,addr_state from customer where cust_key like '" + cust_key + "'";
	        	rs_cust = stmt_cust.executeQuery(sql_cust);
	        	rs_cust.next();
	        	String last_name = rs_cust.getString("last_name");
	        	String adr_city = rs_cust.getString("addr_city");
	        	String adr_state = rs_cust.getString("addr_state");
	        	String cust_addr = adr_city + "-" + adr_state;
	        	//System.out.format("%10s%s%17s%s%10.2f\n",last_name,"  ",cust_addr,"   ",ltv);
	        	writer.println(last_name + "," + cust_addr + "," +ltv);
	        	rs_cust.close();
	        	
	        }
	        stmt_cust.close();
	        rs.close();
	        stmt.close();
	        writer.close();
	        
	        System.out.println("Data for top " + x + " customers with higher LTV is available at : "+file_path + "\\" +file_name);
	        
		}catch(SQLException se)
		
		{
			System.out.println("SQLException raised..");
            se.printStackTrace();
		}catch (IOException e) {
			System.out.println("IOException raised..");
		}
		
	}
	
	
	
	public static void main(String[] args) 
	{
		final Connection con = mysql_con.get_connection();
		String file_name = "sample_input.json";
		String file_path = "C:\\Users\\purva\\workspace\\Test\\src";
		int lines = merge.get_lines(file_path,file_name);
		//System.out.println(lines);
		int i = 0;
		JSONObject jo;
		
		int x = 3;
		
		for (i=0;i<lines;i++)
		{
			jo = event.get_event(file_name,file_path,i);
			
			ingest(jo,con);
			
		}
		
		TopXSimpleLTVCustomers(x, con);
		
		try
		{
			con.close();
			
			System.out.println("Job complete!");
		}catch(SQLException se){
            //Handle errors for JDBC
        	 System.out.println("SQLException raised..");
            se.printStackTrace();
         }
		
	}

}


