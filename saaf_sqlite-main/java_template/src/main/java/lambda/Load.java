/**
 * AWS Lambda function for loading data from a CSV file stored in an S3 bucket into an SQLite database,
 * and subsequently uploading the database file back to the S3 bucket.
 *
 * This class implements the RequestHandler interface for handling Lambda function requests.
 *
 * @author Ingeun Hwang, Karandeep Sangha, Kevin Truong, Khin Win
 */
package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import saaf.Inspector;

/**
 * AWS Lambda function implementation.
 */
public class Load implements RequestHandler<Request, HashMap<String, Object>> {

    // Instance variables
    String bucketname;
    Connection connection;
    List<ArrayList<String>> csvData;
    AmazonS3 s3Client;

    /**
     * Handles Lambda function requests.
     *
     * @param request The request object containing necessary parameters.
     * @param context The Lambda execution environment context.
     * @return A HashMap containing information for AWS Lambda Inspector.
     */
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        bucketname = request.getBucketname();
        csvData = new ArrayList<>();
        s3Client = AmazonS3ClientBuilder.standard().build();

        downloadCSVFileFromS3();
        loadIntoSQLite(csvData, s3Client);

        return inspector.finish();
    }

    /**
     * Downloads a CSV file from the specified S3 bucket and populates the 'csvData' list.
     */
    private void downloadCSVFileFromS3() {
        String key = "output.csv";

        try {
            // Download the object
            S3Object s3Object = s3Client.getObject(new GetObjectRequest("records-462", key));
            InputStream objectData = s3Object.getObjectContent();

            Scanner scanner = new Scanner(objectData);

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] row = line.split(",");
                ArrayList<String> list = new ArrayList<>(Arrays.asList(row));
                csvData.add(list);
            }

            // Close the input stream
            objectData.close();
        } catch (Exception e) {
            System.out.println("Failed to download the csv");
            e.printStackTrace();
            // Handle exceptions appropriately
        }
    }

    /**
     * Loads data from the 'csvData' list into an SQLite database.
     *
     * @param csvData   The list containing CSV data.
     * @param s3Client  The Amazon S3 client.
     */
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

    /**
     * Creates the 'Orders' table in the SQLite database if it does not exist.
     *
     * @param connection The database connection.
     * @throws SQLException If an SQL exception occurs.
     */
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

    /**
     * Uploads the SQLite database file to the specified S3 bucket.
     *
     * @param s3Client       The Amazon S3 client.
     * @param databaseFile   The SQLite database file.
     */
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
            System.out.println("Failed to upload to S3");
            e.printStackTrace();
        }
    }
}
