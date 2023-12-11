package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import saaf.Inspector;



public class ProcessCSV implements RequestHandler<Request, HashMap<String, Object>> {
    
 
        Connection connection;
        String bucketname;
        String filename;
        List<ArrayList<String>> csvData;

    public HashMap<String, Object> handleRequest(Request request, Context context) {
    Inspector inspector = new Inspector();
    inspector.inspectAll();
    
    try
        {
        Thread.sleep(10000);
        }
        catch (InterruptedException ie)
        {
        System.out.println("Interruption occurred while sleeping...");
        }
    bucketname = request.getBucketname();
    filename = request.getFilename();
    
    LambdaLogger logger = context.getLogger();
    logger.log("ProcessCSV bucketname:" + bucketname + " filename:" + filename);
    
    AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
    InputStream objectData = s3Object.getObjectContent();
    
    csvData = new ArrayList<>();
    
    Scanner scanner = new Scanner(objectData);

    while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        String[] row = line.split(",");
        ArrayList<String> list = new ArrayList<>(Arrays.asList(row));
        csvData.add(list);
    }

    transformData(csvData);
    writeCsvToS3(s3Client, csvData);
    loadIntoSQLite(csvData, s3Client);
    
    Map<String, Object> service3Response = processService3Request(request);
    

    scanner.close();

    logger.log("ProcessCSV bucketname:" + bucketname + " filename:" + filename);

    for (String key : service3Response.keySet()) {
        inspector.addAttribute(key, service3Response.get(key));
    }


    Response response = new Response();
    response.setValue("Bucket: " + bucketname + " filename:" + filename + " processed.");
    
//    inspector.consumeResponse(response);

            try {
                connection.close(); // Close the connection
            } catch (SQLException ex) {
                Logger.getLogger(ProcessCSV.class.getName()).log(Level.SEVERE, null, ex);
            }
    return inspector.finish();
}

    private void transformData(List<ArrayList<String>> csvData) {
        // Service #1 Transformations

        // 1. Add column [Order Processing Time]
        csvData.get(0).add("Order Processing Time");
        for (int i = 1; i < csvData.size(); i++) {
            String orderDate = csvData.get(i).get(5);
            String shipDate = csvData.get(i).get(7); 
            // Calculate and add Order Processing Time         
            String orderProcessingTime = calculateOrderProcessingTime(orderDate, shipDate);
            csvData.get(i).add(orderProcessingTime);
        }

        // 2. Transform [Order Priority] column
        int orderPriorityIndex = getColumnIndex(csvData.get(0), "Order Priority");
        for (int i = 1; i < csvData.size(); i++) {
            String orderPriority = csvData.get(i).get(orderPriorityIndex);
            // Transform order priority as needed
            String transformedOrderPriority = transformOrderPriority(orderPriority);
            csvData.get(i).set(orderPriorityIndex, transformedOrderPriority);
        }

        // 3. Add a [Gross Margin] column
        csvData.get(0).add("Gross Margin");
        int totalProfitIndex = getColumnIndex(csvData.get(0), "Total Profit");
        int totalRevenueIndex = getColumnIndex(csvData.get(0), "Total Revenue");
        for (int i = 1; i < csvData.size(); i++) {
            // Calculate and add Gross Margin
       
            String grossMargin = calculateGrossMargin(csvData.get(i).get(totalProfitIndex), csvData.get(i).get(totalRevenueIndex));
            csvData.get(i).add(grossMargin);
        }

        // 4. Remove duplicate data identified by [Order ID]
        int orderIDIndex = getColumnIndex(csvData.get(0), "Order ID");
        List<String> processedOrderIDs = new ArrayList<>();
        List<ArrayList<String>> filteredData = new ArrayList<>();
        for (int i = 0; i < csvData.size(); i++) {
            String orderID = csvData.get(i).get(orderIDIndex);
            if (!processedOrderIDs.contains(orderID)) {
                processedOrderIDs.add(orderID);
                filteredData.add(csvData.get(i));
            }
        }
        // Update csvData with filteredData
        csvData.clear();
        csvData.addAll(filteredData);
    }

    private String calculateOrderProcessingTime(String orderDate, String shipDate) {
    try {
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

        Date orderDateObj = dateFormat.parse(orderDate);
        Date shipDateObj = dateFormat.parse(shipDate);

        long timeDifference = shipDateObj.getTime() - orderDateObj.getTime();
        long daysDifference = TimeUnit.MILLISECONDS.toDays(timeDifference);

        return String.valueOf(daysDifference);
    } catch (ParseException e) {
        e.printStackTrace();
        return "";
    }
}


    private String transformOrderPriority(String orderPriority) {
        switch (orderPriority) {
            case "L":
                return "Low";
            case "M":
                return "Medium";
            case "H":
                return "High";
            case "C":
                return "Critical";
            default:
                return orderPriority;
        }
    }

    private String calculateGrossMargin(String totalProfit, String totalRevenue) {
        try {
            double profit = Double.parseDouble(totalProfit);
            double revenue = Double.parseDouble(totalRevenue);

            if (revenue != 0) {
                double margin = (profit / revenue) * 100; // Calculate as a percentage
                return String.format("%.2f", margin); // Format to two decimal places
            } else {
                return "0.0"; // Handle division by zero
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return "";
        }
    }

    private int getColumnIndex(ArrayList<String> header, String columnName) {
        // Helper method to get the index of a column in the header
        for (int i = 0; i < header.size(); i++) {
            if (header.get(i).equals(columnName)) {
                return i;
            }
        }
        return -1; // Return -1 if the column is not found
    }
    
    
     private void writeCsvToS3(AmazonS3 s3Client, List<ArrayList<String>> csvData) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (CSVPrinter csvPrinter = new CSVPrinter(new PrintWriter(outputStream), CSVFormat.DEFAULT)) {
                for (ArrayList<String> row : csvData) {
                    csvPrinter.printRecord(row);
                }
            }

            byte[] contentBytes = outputStream.toByteArray();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentBytes.length);

            PutObjectRequest putObjectRequest = new PutObjectRequest(
                    bucketname,
                    "output.csv",
                    new ByteArrayInputStream(contentBytes),
                    metadata
            );

            PutObjectResult putObjectResult = s3Client.putObject(putObjectRequest);
            System.out.println("Data written to S3. ETag: " + putObjectResult.getETag());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
     
        

private void loadIntoSQLite(List<ArrayList<String>> csvData, AmazonS3 s3Client) {
    try {
        File databaseFile = new File("/tmp/sales.db");

        Class.forName("org.sqlite.JDBC");
        String dbUrl = "jdbc:sqlite:" + databaseFile.getAbsolutePath();

        // Establish the database connection
        connection = DriverManager.getConnection(dbUrl);
        connection.setAutoCommit(false);

        createOrdersTable(connection);

        try (PreparedStatement preparedStatement = connection.prepareStatement(
                "INSERT INTO Orders (Region, Country, ItemType, SalesChannel, OrderPriority, OrderDate, OrderID, ShipDate, UnitsSold, UnitPrice, UnitCost, TotalRevenue, TotalCost, TotalProfit, OrderProcessingTime, GrossMargin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )) {
            for (int i = 1; i < csvData.size(); i++) {
                ArrayList<String> row = csvData.get(i);
                preparedStatement.setString(1, row.get(0)); // Region
                preparedStatement.setString(2, row.get(1)); // Country
                preparedStatement.setString(3, row.get(2)); // ItemType
                preparedStatement.setString(4, row.get(3)); // SalesChannel
                preparedStatement.setString(5, row.get(4)); // OrderPriority
                preparedStatement.setString(6, row.get(5)); // OrderDate
                preparedStatement.setString(7, row.get(6)); // OrderID
                preparedStatement.setString(8, row.get(7)); // ShipDate
                preparedStatement.setString(9, row.get(8)); // UnitsSold
                preparedStatement.setString(10, row.get(9)); // UnitPrice
                preparedStatement.setString(11, row.get(10)); // UnitCost
                preparedStatement.setString(12, row.get(11)); // TotalRevenue
                preparedStatement.setString(13, row.get(12)); // TotalCost
                preparedStatement.setString(14, row.get(13)); // TotalProfit
                preparedStatement.setString(15, row.get(14)); // OrderProcessingTime
                preparedStatement.setString(16, row.get(15)); // GrossMargin

                preparedStatement.addBatch();
                if (i % 1000 == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }
            }
            preparedStatement.executeBatch();
            connection.commit();
        }
        
        

        uploadSQLiteToS3(s3Client, databaseFile);

    } catch (ClassNotFoundException | SQLException e) {
        e.printStackTrace();
    }
}
private void createOrdersTable(Connection connection) throws SQLException {
    try (PreparedStatement preparedStatement = connection.prepareStatement(
            "CREATE TABLE IF NOT EXISTS Orders (" +
                    "Region TEXT," +
                    "Country TEXT," +
                    "ItemType TEXT," +
                    "SalesChannel TEXT," +
                    "OrderPriority TEXT," +
                    "OrderDate TEXT," +
                    "OrderID TEXT PRIMARY KEY," +
                    "ShipDate TEXT," +
                    "UnitsSold TEXT," +
                    "UnitPrice TEXT," +
                    "UnitCost TEXT," +
                    "TotalRevenue TEXT," +
                    "TotalCost TEXT," +
                    "TotalProfit TEXT," +
                    "OrderProcessingTime TEXT," +
                    "GrossMargin TEXT)"
    )) {
        preparedStatement.executeUpdate();
    }
}


private void uploadSQLiteToS3(AmazonS3 s3Client, File databaseFile) {
    try {
        // Upload the SQLite database file to S3
        byte[] contentBytes = Files.readAllBytes(databaseFile.toPath());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);

        PutObjectRequest putObjectRequest = new PutObjectRequest(
                bucketname, // Replace with your actual S3 bucket name
                "sales.db",
                new ByteArrayInputStream(contentBytes),
                metadata
        );

        PutObjectResult putObjectResult = s3Client.putObject(putObjectRequest);
        System.out.println("SQLite database written to S3. ETag: " + putObjectResult.getETag());

    } catch (IOException e) {
        e.printStackTrace();
    }
}

private Map<String, Object> processService3Request(Request request) {
    Map<String, Object> response = new HashMap<>();
    // Extract filters and aggregations from the JSON request
    Map<String, String> filters = request.getFilters();
    List<String> aggregations = request.getAggregations();

    // Build SQL query dynamically based on filters and aggregations
    String sql = buildSQLQuery(filters, aggregations);

    // Execute the SQL query
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {

        // Process the query results and create a response
        while (resultSet.next()) {
            // Process each row and create a JSON object for the response
    

            // Process each aggregation and add it to the response map
            for (String aggregation : aggregations) {
                // Get the value from the result set using the aggregation name
                double value = resultSet.getDouble(aggregation);
                // Add the value to the response map
                response.put(aggregation, value);
            }

        }
    } catch (SQLException e) {
        e.printStackTrace();
    }

    return response;
}

    private String buildSQLQuery(Map<String, String> filters, List<String> aggregations) {
        // Build the SQL query dynamically based on filters and aggregations
        StringBuilder sqlBuilder = new StringBuilder("SELECT ");
        for (String aggregation : aggregations) {
            sqlBuilder.append(aggregation).append(", ");
        }
        // Remove the trailing comma
        sqlBuilder.delete(sqlBuilder.length() - 2, sqlBuilder.length());
        sqlBuilder.append(" FROM Orders WHERE ");
        for (Map.Entry<String, String> filter : filters.entrySet()) {
            sqlBuilder.append(filter.getKey()).append("='").append(filter.getValue()).append("' AND ");
        }
        // Remove the trailing "AND"
        sqlBuilder.delete(sqlBuilder.length() - 5, sqlBuilder.length());
        sqlBuilder.append(";");

        return sqlBuilder.toString();
    }  
}




