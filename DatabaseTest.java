package databasetest;

import java.sql.*;
import java.util.ArrayList;
import com.opencsv.*;
import org.json.simple.*;

public class DatabaseTest {

    public static void main(String[] args) {
        System.out.println("DATABASE TO JSON PARSER");
        System.out.println(getJSONData().toString());
        
    }
    public static JSONArray getJSONData(){
        
        Connection conn = null;
        PreparedStatement pstSelect = null, pstUpdate = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        
        String query, value;
        ArrayList <String> key = new ArrayList<>();
        JSONArray data = new JSONArray();
        
        boolean hasresults;
        int resultCount, columnCount, updateCount = 0;
        
        try {
            
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "CS488";
            System.out.println("Connecting to " + server + "...");
            
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            

            conn = DriverManager.getConnection(server, username, password);

                       
            if (conn.isValid(0)) {
                
                
                System.out.println("Connected Successfully!");

                query = "SELECT * FROM people";
                pstSelect = conn.prepareStatement(query);
                
                hasresults = pstSelect.execute();
   
                
                if (updateCount > 0) {
            
                    resultset = pstUpdate.getGeneratedKeys();

                    if (resultset.next()) {

                        System.out.print("Update Successful!  New Key: ");
                        System.out.println(resultset.getInt(1));

                    }

                }
                
                
  
                
                query = "SELECT * FROM people";
                pstSelect = conn.prepareStatement(query);
                
  
                
                System.out.println("Submitting Query ...");
                
                hasresults = pstSelect.execute();                
                
  
                
                System.out.println("Getting Results ...");
                
                while ( hasresults || pstSelect.getUpdateCount() != -1 ) {

                    if ( hasresults ) {
                        
       
                        
                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();
                        
             
                        
                        for (int i = 2; i <= columnCount; i++) {

                            key.add(metadata.getColumnLabel(i));

                            System.out.format("%20s", key);

                        }
                        
                
                        
                        while(resultset.next()) {
                            
                       

                            JSONObject jObj= new JSONObject();
                            
                       

                            for (int i = 2; i <= columnCount; i++) {

                                JSONObject jsonObj = new JSONObject();
                                value = resultset.getString(i);

                                if (resultset.wasNull()) {
                                    jsonObj.put(key.get(i - 2), "NULL");
                                    jsonObj.toJSONString();
                                }

                                else {
                                    jsonObj.put(key.get(i - 2), value);
                                    jsonObj.toString();
                                }
                            jObj.putAll(jsonObj);

                            }
                        data.add(jObj);

                        }
                        
                    }

                    else {

                        resultCount = pstSelect.getUpdateCount();  

                        if ( resultCount == -1 ) {
                            break;
                        }

                    }
                    
          

                    hasresults = pstSelect.getMoreResults();

                }
                
            }
            
            System.out.println();
            
            
            conn.close();
            
        }
        
        catch (Exception e) {
            System.err.println(e.toString());
        }
        
 
        
        finally {
            
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstSelect != null) { try { pstSelect.close(); pstSelect = null; } catch (Exception e) {} }
            
            if (pstUpdate != null) { try { pstUpdate.close(); pstUpdate = null; } catch (Exception e) {} }
            
        }
        return data;
    }
}
    
