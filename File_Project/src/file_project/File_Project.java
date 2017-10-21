/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package file_project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author ayush
 */
public class File_Project{
     
    public static Connection conn = null;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        int count_processed_files, count_entered_rows, count_failed_rows, count_failed_ ;
        String filePath = null;
        File[] allFiles = null;
        String[] requiredColumns = {"first_name", "last_name", "address", "telephone"};
        // instead of checking file check if directory exist
        // then if there are files in it
        // if there are files then read all files name with path into a list
        // then loop using for(file in file_list)
        // surround this with try catch and if exception catch and proceed without exiting
        // get list of file in directory
        if (args!= null && args.length>0) {
            filePath = args[0];
            File f = new File(filePath);
            if(f.exists() && f.isDirectory())
            {
                allFiles = f.listFiles();
                System.out.print("Got list of file in directory: " + filePath + "...\n");
            }else{
                System.out.print("Directory given: " + filePath + "does not exist or is not a directory\n");
                System.exit(0);
            }
        } else {
            System.out.print("Pls provide file path\n\n");
            System.exit(0);
        }
        
        // now check DB connection
        conn = ConnectDB();
        if(conn == null){
            System.out.print("Database connection failed..exiting...");
            System.exit(-1);
        }
        
        
        BufferedReader br = null;
         
        for(File f : allFiles)
        {
            // if f is a file then process
            // make path/file_name then FileReader()
            Logger.getLogger(File_Project.class.getName()).log(Level.INFO, "Now Processing file:" + f);
            try {   
                br = new BufferedReader(new FileReader(f));
            } catch (FileNotFoundException ex) {
                Logger.getLogger(File_Project.class.getName()).log(Level.SEVERE, null, ex);
            }

            String splitBy = "\t"; 
            int num_of_columns;

           String line = null;
           int count = 0;
            try {
                line = br.readLine();
            } catch (IOException ex) {
                Logger.getLogger(File_Project.class.getName()).log(Level.SEVERE, null, ex);
            }

           while(line!=null)
           {    
                if(!line.isEmpty()) 
                {
                    count += 1;
                    String[] columnsArray = line.split(splitBy);
                    num_of_columns = columnsArray.length;
                    
                    // if count is 1 then it is a header
                    if(count == 1)
                    {
                        
                        for(String eachCol : columnsArray)
                        {
                            if(!Arrays.asList(requiredColumns).contains(eachCol)){
                                
                            }else{
                                // here you will get the index and column name
                            }

                        }
                    }
                    
//                    File_Data a = new File_Data();
//                    a.first_name = b[0];
//                    a.second_name = b[1];
//                    list.add(a);
                    
                    if(insert(columnsArray, num_of_columns)){
                        System.out.print("entered a value\n");
                    }else{
                        System.out.print("insert qurey failed\n");
                        System.exit(-1);
                    }

                        //System.out.print(b[j]+" ");
                        // TBD: change this part to write column data to data base

                    //System.out.print("\n");
                }    
                try {
                     line = br.readLine();
                     } catch (IOException ex) {
                     Logger.getLogger(File_Project.class.getName()).log(Level.SEVERE, null, ex);
                 }
            }
            Logger.getLogger(File_Project.class.getName()).log(Level.INFO, "Processed " + count + " lines");
        }
    }
    
    public static Connection ConnectDB(){

        String url = "jdbc:mysql://localhost:3306/customer_DB";
        String username = "root";
        String password = "root";

        System.out.println("Connecting database...");

        try {
            Connection connection = DriverManager.getConnection(url, username, password);
            return connection;
      //connection.close();
        } catch (SQLException ex) {
            Logger.getLogger(File_Project.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }      
    }
    
    public static boolean insert( String[] columns, int num_columns)
    {

        int i = 0;
        if(i == 1)
        {    
        
            
        }
            boolean success = false;
            String fName  = null;
            String lName = null; 
            String telNum = null;
            String cellNum = null;
            String address = null;

            switch(num_columns)
            {
                case 1: 
                    fName = columns[0];
                    break;
                case 2: 
                    fName = columns[0];
                    lName = columns[1]; 
                    break;    
                case 3:
                    fName = columns[0];
                    lName = columns[1]; 
                    telNum = columns[2];
                    break;
                case 4:
                    fName = columns[0];
                    lName = columns[1]; 
                    telNum = columns[2];
                    address = columns[3]; 
                    break; 
            }

            try
            {
    //            String query = "INSERT INTO `customer_DB`.`customer` (`first_name`, "
    //                    + "`last_name`, `telephone`, `address`) VALUES ( '" + fname + 
    //                    "', '" + lname + "', " + tel + ", " + address + ")";

                String query = "INSERT INTO `customer_DB`.`customer` (`first_name`, "
                        + "`last_name`, `telephone`, `address`) VALUES (?, ?, ?, ?)";


                // create the mysql insert preparedstatement
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.setString(1, fName);
                preparedStmt.setString(2, lName);
                preparedStmt.setString(3, telNum);
                preparedStmt.setString(4, address);
                preparedStmt.addBatch();
                int[] commits = preparedStmt.executeBatch();
                if(commits.length > 0)
                    success = true;
                // execute the preparedstatement
    //            success = preparedStmt.execute();

            }catch(SQLException ex){
                Logger.getLogger(File_Project.class.getName()).log(Level.SEVERE, null, ex);
            }catch(Exception ex){
                Logger.getLogger(File_Project.class.getName()).log(Level.SEVERE, null, ex);
            }
            return success;
            
    }    
}
