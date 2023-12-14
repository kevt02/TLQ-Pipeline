/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
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
 *
 * @author kevint
 */
public class Transform implements RequestHandler<Request, HashMap<String, Object>> {
   // test
    String bucketname;
    String filename;
    List<ArrayList<String>> csvData;
    AmazonS3 s3Client;
    
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
          Inspector inspector = new Inspector();
          inspector.inspectAll();

          bucketname = request.getBucketname();
          filename = request.getFilename();
          csvData = new ArrayList<>();
          s3Client = AmazonS3ClientBuilder.standard().build();

          downloadCSVFileFromS3();
          transformData(csvData);
          writeCsvToS3(s3Client, csvData);

          return inspector.finish();
      }
    
    private void downloadCSVFileFromS3() {
          try {
              // Download the object

              S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
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
    
}
