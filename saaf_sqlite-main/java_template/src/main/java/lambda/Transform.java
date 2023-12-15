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

public class Transform implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

    private AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    @Override
    public HashMap<String, Object> handleRequest(HashMap<String, Object> input, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        String bucketname = (String) input.get("bucketname");
        String filename = (String) input.get("filename");
        List<ArrayList<String>> csvData = downloadCSVFileFromS3(bucketname, filename);

        transformData(csvData);
        writeCsvToS3(bucketname, csvData);

        return inspector.finish();
    }

    private List<ArrayList<String>> downloadCSVFileFromS3(String bucketname, String filename) {
        List<ArrayList<String>> csvData = new ArrayList<>();
        try {
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
            InputStream objectData = s3Object.getObjectContent();
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

    private void transformData(List<ArrayList<String>> csvData) {
        csvData.get(0).add("Order Processing Time");
        for (int i = 1; i < csvData.size(); i++) {
            String orderDate = csvData.get(i).get(5);
            String shipDate = csvData.get(i).get(7);
            csvData.get(i).add(calculateOrderProcessingTime(orderDate, shipDate));
        }

        int orderPriorityIndex = getColumnIndex(csvData.get(0), "Order Priority");
        for (int i = 1; i < csvData.size(); i++) {
            String orderPriority = csvData.get(i).get(orderPriorityIndex);
            csvData.get(i).set(orderPriorityIndex, transformOrderPriority(orderPriority));
        }

        csvData.get(0).add("Gross Margin");
        int totalProfitIndex = getColumnIndex(csvData.get(0), "Total Profit");
        int totalRevenueIndex = getColumnIndex(csvData.get(0), "Total Revenue");
        for (int i = 1; i < csvData.size(); i++) {
            csvData.get(i).add(calculateGrossMargin(csvData.get(i).get(totalProfitIndex), csvData.get(i).get(totalRevenueIndex)));
        }

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
            return revenue != 0 ? String.format("%.2f", (profit / revenue) * 100) : "0.0";
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return "";
        }
    }

    private int getColumnIndex(ArrayList<String> header, String columnName) {
        for (int i = 0; i < header.size(); i++) {
            if (header.get(i).equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    private void writeCsvToS3(String bucketname, List<ArrayList<String>> csvData) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            CSVPrinter csvPrinter = new CSVPrinter(new PrintWriter(outputStream), CSVFormat.DEFAULT);
            for (ArrayList<String> row : csvData) {
                csvPrinter.printRecord(row);
            }
            csvPrinter.close();

            byte[] contentBytes = outputStream.toByteArray();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentBytes.length);

            PutObjectRequest putObjectRequest = new PutObjectRequest(
                    bucketname, "output.csv", new ByteArrayInputStream(contentBytes), metadata);

            s3Client.putObject(putObjectRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
