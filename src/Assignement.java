import java.io.*;
import java.sql.*;
import java.util.ArrayList;

import javax.swing.*;

public class Assignement {
	public static void main(String[] args) {
		JFrame f = new JFrame();
		ArrayList<Object []> datas = new ArrayList<Object[]>(); //= new Object[2][10];
		String column[]={"First Name", "Last Name", "dob", "Age", "Address Line 1", "Address Line 2", "City", "State", "Country", "Postal Code"};         
		
        String jdbcURL = "jdbc:mysql://localhost:3306/assignment?allowMultiQueries=true";
        
        String username = "root";
        String password = "";
 
        String csvFilePath = "Test2.csv";
 
        int batchSize = 100;
 
        Connection connection = null;
 
        try {
        	Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);
 
            String sql = "INSERT INTO assignment (id, f_name, l_name, dob, age, postal_code) VALUES (? ,?, ?, ?, ?, ?); INSERT INTO address (id, address1, address2, city, state, country) VALUES (?,?,?,?,?,?);";
           
            PreparedStatement statement = connection.prepareStatement(sql);
 
            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;
 
            int count = 0, id = 1;
            
            lineReader.readLine(); // skip header line
 
            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
//                System.out.println(lineText);
                String firstName = data[0];
                String lastName = data[1];
                String dateOfBirth = data[2];
                String age = data[3];
                String  addressLine1 = data[4] ;
                String  addressLine2 = data[5] ;
                String city = data[6];
                String state = data[7];
                String country = data[8];
                String postalCode = data[9];
 
                if(firstName.length() < 20 && lastName.length() < 20) {
	                statement.setString(2, firstName);
	                
	                statement.setString(3, lastName);
	                String[] dateSplit = dateOfBirth.split("/");
	                String dateSql = dateSplit[2]+"-"+dateSplit[1]+"-"+dateSplit[0];
	                try{
	                	Date sqlDate = Date.valueOf(dateSql);
	                	statement.setDate(4, sqlDate);
	                	Integer ageInt = Integer.parseInt(age);
	                	if(ageInt > 23 && ageInt < 71) {
		 	                statement.setInt(5, ageInt);
		 	                try {
		 	                	int p_code = Integer.parseInt(postalCode);
		 	                	if(p_code > 99999 && p_code < 1000000) {
				 	                statement.setInt(6, Integer.parseInt(postalCode));
				 	                statement.setString(8, addressLine1);
				 	                statement.setString(9, addressLine2);
				 	                statement.setString(10, city);
				 	                statement.setString(11, state);
				 	                statement.setString(12, country);
				 	                statement.setInt(1, id);
				 	                statement.setInt(7, id++);
				 	                statement.addBatch();
				 	 
				 	                if (count % batchSize == 0) {
				 	                    statement.executeBatch();
				 	                }
		 	                	}
		 	                	else {
		 	                		System.out.printf("Row %d omitted, Postal code should be six digits!\n",id++);
		 	                	}
		 	                } catch(NumberFormatException ex){
		 	                	System.out.printf("Postal code on Row %d is not Numeric\n", id++);
		 	                }
		 	                
	                	}
	                	else {
	                		System.out.printf("Row %d omitted, Invalid Age!\n",id++);
	                	}
	                } catch (IllegalArgumentException e) {
	                	System.out.printf("Invalid Date Format on Row %d!\n",id++);
	                } 
                }
                else {
                	System.out.printf("Row %d omitted, First Name or Last Name has Invalid number of characters!\n",id++);
                }
            }
 
            lineReader.close();
 
            // execute the remaining queries
            statement.executeBatch();
 
            connection.commit();
            Statement statement1 = connection.createStatement();
            
            ResultSet results = statement1.executeQuery("SELECT f_name, l_name, dob, age, address1, address2, city, state, country, postal_code FROM assignment, address WHERE assignment.id = address.id");
             
             
            // For each row of the result set ...
            int i =0; 
            
            while (results.next()) {
             datas.add(new Object[10]);
             datas.get(i)[0] = results.getString("f_name");
             datas.get(i)[1] = results.getString("l_name");
             String sqlDate[] = results.getString("dob").split("-");
             String tableDate = sqlDate[1]+"/"+sqlDate[2]+"/"+sqlDate[0];
             datas.get(i)[2]= tableDate;
             datas.get(i)[3] = results.getInt("age");
             datas.get(i)[4] = results.getString("address1");
             datas.get(i)[5] = results.getString("address2");
             datas.get(i)[6] = results.getString("city");
             datas.get(i)[7] = results.getString("state");
             datas.get(i)[8] = results.getString("country");
             datas.get(i++)[9] = results.getString("postal_code");
            
//              System.out.println("Fetching data by column name for row " + results.getRow() + " : " + data);
            }
            Object finalData[][] = new Object[datas.size()][];
            for(int k = 0; k< datas.size(); k++) {
            	
            	finalData[k] = datas.get(k); 
            }
            connection.close();
            JTable jt=new JTable(finalData,column);    
    		jt.setBounds(30,40,200,300);          
    		JScrollPane sp=new JScrollPane(jt);    
    		f.add(sp);                
    		f.setSize(300,400);    
    		f.setVisible(true);
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();
 
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch(ClassNotFoundException ex){
        	ex.printStackTrace();
        }
 
    }

}
