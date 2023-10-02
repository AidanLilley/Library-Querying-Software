package com.example.assignment4;/*
 * BooksDatabaseService.java
 *
 * The service threads for the books database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: <2446292>
 *
 */

import java.io.*;
//import java.io.OutputStreamWriter;

import java.net.Socket;

import java.sql.*;
import javax.sql.rowset.*;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
//these clasess are not exported by the module. Instead, one needs to impor
//javax.sql.rowset.* as above.


public class BooksDatabaseService extends Thread {

    private Socket serviceSocket = null;
    private String[] requestStr = new String[2]; //One slot for author's name and one for library's name.
    private ResultSet outcome = null;

    //JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL = Credentials.URL;


    //Class constructor
    public BooksDatabaseService(Socket aSocket) {

        //TO BE COMPLETED
        this.serviceSocket = aSocket;
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest() {
        this.requestStr[0] = ""; //For author
        this.requestStr[1] = ""; //For library

        String tmp = "";
        try {

            //TO BE COMPLETED
            ObjectInputStream request = new ObjectInputStream(serviceSocket.getInputStream());
            String requestString = (String) request.readObject();
            requestString = requestString.replace(Character.toString('#'), "");
            requestStr = requestString.split(";");

        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
            System.exit(1);
        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest() {
        boolean flagRequestAttended = true;

        this.outcome = null;

        String sql = ""; //TO BE COMPLETED- Update this line as needed.


        try {
            //Connet to the database
            //TO BE COMPLETED
            Connection con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            //Make the query
            //TO BE COMPLETED
            sql = "SELECT  book.title AS book_title,  book.publisher AS publisher,  genre.name AS genre_name,  book.rrp AS price,  COUNT(bookcopy.copyid) AS copies_available FROM  book  JOIN author ON book.authorid = author.authorid  JOIN bookcopy ON book.bookid = bookcopy.bookid  JOIN genre ON book.genre = genre.name  JOIN library ON bookcopy.libraryid = library.libraryid WHERE  author.familyname = ? AND  library.city = ? GROUP BY  book.title,  book.publisher,  genre.name,  book.rrp HAVING  COUNT(bookcopy.copyid) > 0;";
            PreparedStatement pstmt = con.prepareStatement(sql);
            pstmt.clearParameters();
            pstmt.setString(1, this.requestStr[0]);
            pstmt.setString(2, this.requestStr[1]);
            ResultSet rs = pstmt.executeQuery();
            //Process query
            //TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.
            RowSetFactory aFactory = RowSetProvider.newFactory();
            CachedRowSet crs = aFactory.createCachedRowSet();
            crs.populate(rs);
            this.outcome = crs;
            //Clean up
            //TO BE COMPLETED
            rs.close();
            pstmt.close();
            con.close();
        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }

        return flagRequestAttended;
    }


    //Wrap and return service outcome
    public void returnServiceOutcome() {
        try {
            //Return outcome
            //TO BE COMPLETED
            ObjectOutputStream oos = new ObjectOutputStream(serviceSocket.getOutputStream());
            oos.writeObject(this.outcome);
            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);

            //Terminating connection of the service socket
            //TO BE COMPLETED
            serviceSocket.close();

        } catch (IOException e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
            System.exit(1);
        }
    }


    //The service thread run() method
    public void run() {
        try {
            System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
                    + "author->" + this.requestStr[0] + "; library->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        } catch (Exception e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
            System.exit(1);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
