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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import saaf.Inspector;

public class ProcessCSV implements RequestHandler<Request, HashMap<String, Object>> {
    
        String bucketname;
        String filename;
        List<ArrayList<String>> csvData;

    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        bucketname = request.getBucketname();
        filename = request.getFilename();
        
        LambdaLogger logger = context.getLogger();
        logger.log("ProcessCSV bucketname:" + bucketname + " filename:" + filename);
        
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        //scanning data line by line
        String text = "";
        
        Scanner scanner = new Scanner(objectData);
     
        
        csvData = new ArrayList<>();
        
        
        
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] row = line.split(",");
            ArrayList<String> list = new ArrayList<>(Arrays.asList(row));
            csvData.add(list);
        }
        
        // Service #1 Transformations
        transformData(csvData);
        
        writeCsvToS3(s3Client, csvData);
        
        loadIntoSQLite(csvData, s3Client);
        
        

        // Further processing or storage logic can be added here

        scanner.close();

        logger.log("ProcessCSV bucketname:" + bucketname + " filename:" + filename);

        inspector.addAttribute("message", "Hello " + request.getBucketname() 
                + "! This is an attribute added to the Inspector!");
     
        

        Response response = new Response();
        response.setValue("Bucket: " + bucketname + " filename:" + filename + " processed.");

        inspector.consumeResponse(response);

        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private void transformData(List<ArrayList<String>> csvData) {
        // Service #1 Transformations

        // 1. Add column [Order Processing Time]
        csvData.get(0).add("Order Processing Time");
        for (int i = 1; i < csvData.size(); i++) {
            String orderDate = csvData.get(i).get(5); // Assuming Order Date is at index 5
            String shipDate = csvData.get(i).get(7); // Assuming Ship Date is at index 7
            // Calculate and add Order Processing Time
            // You need to implement the logic to calculate the order processing time based on your date format
            String orderProcessingTime = calculateOrderProcessingTime(orderDate, shipDate);
            csvData.get(i).add(orderProcessingTime);
        }

        // 2. Transform [Order Priority] column
        int orderPriorityIndex = getColumnIndex(csvData.get(0), "Order Priority");
        for (int i = 1; i < csvData.size(); i++) {
            String orderPriority = csvData.get(i).get(orderPriorityIndex);
            // Transform order priority as needed
            // You need to implement the logic to transform order priority
            String transformedOrderPriority = transformOrderPriority(orderPriority);
            csvData.get(i).set(orderPriorityIndex, transformedOrderPriority);
        }

        // 3. Add a [Gross Margin] column
        csvData.get(0).add("Gross Margin");
        int totalProfitIndex = getColumnIndex(csvData.get(0), "Total Profit");
        int totalRevenueIndex = getColumnIndex(csvData.get(0), "Total Revenue");
        for (int i = 1; i < csvData.size(); i++) {
            // Calculate and add Gross Margin
            // You need to implement the logic to calculate the gross margin based on your data format
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
        // Assuming date format is "MM/dd/yyyy"
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
    
//       private void writeCsvToFile(List<ArrayList<String>> csvData, String outputFileName) {
//        try (FileWriter writer = new FileWriter(outputFileName)) {
//            for (ArrayList<String> row : csvData) {
//                StringBuilder csvLine = new StringBuilder();
//                for (String value : row) {
//                    csvLine.append(value).append(",");
//                }
//                // Remove the trailing comma and write the line to the file
//                writer.write(csvLine.substring(0, csvLine.length() - 1) + "\n");
//            }
//            System.out.println("Data written to " + outputFileName);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
    
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
        // Create SQLite database file in the /tmp directory
        File databaseFile = new File("/tmp/sales.db");

        // Connect to SQLite database
        Class.forName("org.sqlite.JDBC");
        String dbUrl = "jdbc:sqlite:" + databaseFile.getAbsolutePath();

        try (Connection connection = DriverManager.getConnection(dbUrl)) {
            // Disable auto-commit to perform a batch insert
            connection.setAutoCommit(false);

            // Create the Orders table if it does not exist
            createOrdersTable(connection);

            try (PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT INTO Orders (Region, Country, ItemType, SalesChannel, OrderPriority, OrderDate, OrderID, ShipDate, UnitsSold, UnitPrice, UnitCost, TotalRevenue, TotalCost, TotalProfit, OrderProcessingTime, GrossMargin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )) {
                // Iterate over CSV data and add batch inserts
                for (int i = 1; i < csvData.size(); i++) {
                    ArrayList<String> row = csvData.get(i);
                    // Set values based on the structure of your CSV data
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
}


                // Execute batch insert
                preparedStatement.executeBatch();

                // Commit the transaction
                connection.commit();
            }
        }

        // Upload SQLite database file to S3
        uploadSQLiteToS3(s3Client, databaseFile);
        System.out.println("uploadsqlite");

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


// ... (Remaining code remains unchanged)

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

}




