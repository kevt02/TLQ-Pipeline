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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
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

/**
 * The Transform class implements an AWS Lambda function for processing CSV files stored in S3.
 * It performs various transformations on the CSV data and then uploads the transformed data back to S3.
 */
public class Transform implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

    // Creates an S3 client using the default configuration.
    private AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    /**
     * The entry point for the Lambda function.
     *
     * @param input   The input data for the Lambda function, containing the bucket name and file name.
     * @param context The execution context provided by AWS Lambda.
     * @return A HashMap representing the result of the processing.
     */
    @Override
    public HashMap<String, Object> handleRequest(HashMap<String, Object> input, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        // Extract bucket name and file name from the input.
        String bucketname = (String) input.get("bucketname");
        String filename = (String) input.get("filename");

        // Download CSV file from S3 and store its data.
        List<ArrayList<String>> csvData = downloadCSVFileFromS3(bucketname, filename);

        // Perform data transformation.
        transformData(csvData);

        // Write the transformed data back to S3.
        writeCsvToS3(bucketname, csvData);

        // Finish the inspection and return the result.
        return inspector.finish();
    }

    /**
     * Downloads a CSV file from S3 and reads its content into a list of lists.
     *
     * @param bucketname The name of the S3 bucket.
     * @param filename   The name of the file to download.
     * @return A List of ArrayLists, where each ArrayList represents a row of the CSV file.
     */
    private List<ArrayList<String>> downloadCSVFileFromS3(String bucketname, String filename) {
        List<ArrayList<String>> csvData = new ArrayList<>();
        try {
            // Retrieve the object from S3.
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
            InputStream objectData = s3Object.getObjectContent();

            // Read the CSV data line by line.
            Scanner scanner = new Scanner(objectData);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] row = line.split(",");
                csvData.add(new ArrayList<>(Arrays.asList(row)));
            }
            objectData.close();
        } catch (Exception e) {
            System.out.println("Failed to download the csv");
            e.printStackTrace();
        }
        return csvData;
    }

    /**
     * Transforms the CSV data by adding additional columns and filtering data.
     *
     * @param csvData The original CSV data.
     */
    private void transformData(List<ArrayList<String>> csvData) {
        // Add a new column for Order Processing Time.
        csvData.get(0).add("Order Processing Time");

        // Calculate and add processing time for each order.
        for (int i = 1; i < csvData.size(); i++) {
            String orderDate = csvData.get(i).get(5);
            String shipDate = csvData.get(i).get(7);
            csvData.get(i).add(calculateOrderProcessingTime(orderDate, shipDate));
        }

        // Transform 'Order Priority' column.
        int orderPriorityIndex = getColumnIndex(csvData.get(0), "Order Priority");
        for (int i = 1; i < csvData.size(); i++) {
            String orderPriority = csvData.get(i).get(orderPriorityIndex);
            csvData.get(i).set(orderPriorityIndex, transformOrderPriority(orderPriority));
        }

        // Add a new column for Gross Margin.
        csvData.get(0).add("Gross Margin");

        // Calculate and add gross margin for each order.
        int totalProfitIndex = getColumnIndex(csvData.get(0), "Total Profit");
        int totalRevenueIndex = getColumnIndex(csvData.get(0), "Total Revenue");
        for (int i = 1; i < csvData.size(); i++) {
            csvData.get(i).add(calculateGrossMargin(csvData.get(i).get(totalProfitIndex), csvData.get(i).get(totalRevenueIndex)));
        }

        // Filter out duplicate order IDs.
        int orderIDIndex = getColumnIndex(csvData.get(0), "Order ID");
        List<String> processedOrderIDs = new ArrayList<>();
        List<ArrayList<String>> filteredData = new ArrayList<>();
        for (ArrayList<String> row : csvData) {
            String orderID = row.get(orderIDIndex);
            if (!processedOrderIDs.contains(orderID)) {
                processedOrderIDs.add(orderID);
                filteredData.add(row);
            }
        }
        csvData.clear();
        csvData.addAll(filteredData);
    }

    /**
     * Calculates the number of days between the order date and the ship date.
     *
     * @param orderDate The order date in MM/dd/yyyy format.
     * @param shipDate  The ship date in MM/dd/yyyy format.
     * @return A string representing the number of days between the two dates.
     */
    private String calculateOrderProcessingTime(String orderDate, String shipDate) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
            Date orderDateObj = dateFormat.parse(orderDate);
            Date shipDateObj = dateFormat.parse(shipDate);
            long timeDifference = shipDateObj.getTime() - orderDateObj.getTime();
            return String.valueOf(TimeUnit.MILLISECONDS.toDays(timeDifference));
        } catch (ParseException e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * Transforms the order priority from a single letter to a full word.
     *
     * @param orderPriority The order priority as a single letter.
     * @return The full word representation of the order priority.
     */
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

    /**
     * Calculates the gross margin as a percentage.
     *
     * @param totalProfit   The total profit as a string.
     * @param totalRevenue  The total revenue as a string.
     * @return A string representing the gross margin percentage.
     */
    private String calculateGrossMargin(String totalProfit, String totalRevenue) {
        try {
            double profit = Double.parseDouble(totalProfit);
            double revenue = Double.parseDouble(totalRevenue);
            return revenue != 0 ? String.format("%.2f", (profit / revenue) * 100) : "0.0";
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * Finds the column index for a given column name in the CSV data.
     *
     * @param header     The header row of the CSV data.
     * @param columnName The name of the column to find.
     * @return The index of the column, or -1 if not found.
     */
    private int getColumnIndex(ArrayList<String> header, String columnName) {
        for (int i = 0; i < header.size(); i++) {
            if (header.get(i).equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Writes the transformed CSV data back to an S3 bucket.
     *
     * @param bucketname The name of the S3 bucket.
     * @param csvData    The CSV data to write.
     */
    private void writeCsvToS3(String bucketname, List<ArrayList<String>> csvData) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            CSVPrinter csvPrinter = new CSVPrinter(new PrintWriter(outputStream), CSVFormat.DEFAULT);

            // Print each row of the CSV data to the output stream.
            for (ArrayList<String> row : csvData) {
                csvPrinter.printRecord(row);
            }
            csvPrinter.close();

            // Prepare the content to be uploaded.
            byte[] contentBytes = outputStream.toByteArray();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentBytes.length);

            // Create and execute a PutObjectRequest to upload the data.
            PutObjectRequest putObjectRequest = new PutObjectRequest(
                    bucketname, "output.csv", new ByteArrayInputStream(contentBytes), metadata);

            s3Client.putObject(putObjectRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}