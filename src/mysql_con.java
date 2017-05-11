import java.sql.* ;  // for standard JDBC programs


public class mysql_con {

	
	private static String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	private static String DB_URL = "jdbc:mysql://localhost:3306/shutterfly"; //shutterfly is the database created to save events data

	   //  Database credentials
	private static String USER = "root";
	private static String PASS = "password";
	
	private static Connection conn = null;
	
	//establish connection for mysql database 
	public static Connection  get_connection() 
	{        
        try{
            
            Class.forName(JDBC_DRIVER);

            //connect to db
            //System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

         }catch(SQLException se){
            //Handle errors for JDBC
        	 System.out.println("SQLException raised..");
            se.printStackTrace();
            
         }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
            System.out.println("Exception for Class.forName raised..");
         }
        
        return conn;
        
	}   
	   
}
