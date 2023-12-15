/**
 * AWS Lambda function for querying an SQLite database based on specified filters and aggregations,
 * and processing the results to generate a response.
 *
 * This class implements the RequestHandler interface for handling Lambda function requests.
 *
* @author Ingeun Hwang, Karandeep Sangha, Kevin Truong, Khin Win
 */
package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import saaf.Inspector;

/**
 * AWS Lambda function implementation.
 */
public class Query implements RequestHandler<Request, HashMap<String, Object>> {

    // Instance variables
    Connection connection;
    String bucketname;

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

        downloadDbFileFromS3();

        Map<String, Object> service3Response = processService3Request(request);

        LambdaLogger logger = context.getLogger();

        for (String key : service3Response.keySet()) {
            inspector.addAttribute(key, service3Response.get(key));
            System.out.println(key + ": " + service3Response.get(key));
        }

        return inspector.finish();
    }

    /**
     * Downloads the SQLite database file from the specified S3 bucket and saves it to /tmp directory.
     */
    private void downloadDbFileFromS3() {
        String key = "sales.db";

        try {
            // Download the object
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, key));
            InputStream objectData = s3Object.getObjectContent();

            // Specify an absolute path for the target file in /tmp
            java.nio.file.Path targetPath = java.nio.file.Paths.get("/tmp/sales.db");

            // Copy the input stream to the target path
            java.nio.file.Files.copy(objectData, targetPath);

            // Close the input stream
            objectData.close();
        } catch (Exception e) {
            System.out.println("Failed to download the database");
            e.printStackTrace();
            // Handle exceptions appropriately
        }
    }

    /**
     * Processes the Service 3 request by executing the SQL query and generating a response.
     *
     * @param request The request object containing filters and aggregations.
     * @return A map containing the response with aggregated values.
     */
    private Map<String, Object> processService3Request(Request request) {
        Map<String, Object> response = new HashMap<>();
        // Extract filters and aggregations from the JSON request
        Map<String, String> filters = request.getFilters();
        List<String> aggregations = request.getAggregations();
        // Build SQL query dynamically based on filters and aggregations
        String sql = buildSQLQuery(filters, aggregations);

        // Execute the SQL query

        try {
            File databaseFile = new File("/tmp/sales.db");

            Class.forName("org.sqlite.JDBC");
            String dbUrl = "jdbc:sqlite:" + databaseFile.getAbsolutePath();

            // Establish the database connection
            connection = DriverManager.getConnection(dbUrl);
            connection.setAutoCommit(false);
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
            }
            connection.commit();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        return response;
    }

    /**
     * Builds the SQL query dynamically based on filters and aggregations.
     *
     * @param filters      The map containing filters.
     * @param aggregations The list containing aggregations.
     * @return The dynamically generated SQL query.
     */
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
