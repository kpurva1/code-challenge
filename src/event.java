import java.io.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class event {
	static JSONParser jp1 = new JSONParser();
	static Object obj;
	static JSONArray jarray;
	static JSONObject jo;
	
	
	//to extract event from input JSON file
	public static JSONObject get_event(String file_name,String file_path,int num)
	{
		try{			
			obj = jp1.parse(new FileReader(file_path + '\\' + file_name));				
			jarray = (JSONArray)obj;	
			jo = (JSONObject)jarray.get(num);			
			
		}catch(IOException ie){
            //Handle errors for io
        	 System.out.println("IOException raised");
            ie.printStackTrace();
		}catch(ParseException pe)
		{
			System.out.println("ParseException raised...");
            pe.printStackTrace();
		}
	return jo;
	}

}
