package lambda;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String name;
    
    private String bucketname;
    
    private String filename;
    
    private Map<String, String> filters;
    
    private List<String> aggregations;

    public String getName() {
        return name;
    }
    
    public String getNameALLCAPS() {
        return name.toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Request(String name) {
        this.name = name;
    }

    public Request() {

    }

//    int getRow() {
//        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
//    }
//
//    int getCol() {
//        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
//    }

    /**
     * @return the bucketname
     */
    public String getBucketname() {
        return bucketname;
    }

    /**
     * @param bucketname the bucketname to set
     */
    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename the filename to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

    /**
     * @return the filters
     */
    public Map<String, String> getFilters() {
        return filters;
    }

    /**
     * @param filters the filters to set
     */
    public void setFilters(Map<String, String> filters) {
        this.filters = filters;
    }

    /**
     * @return the aggregations
     */
    public List<String> getAggregations() {
        return aggregations;
    }

    /**
     * @param aggregations the aggregations to set
     */
    public void setAggregations(List<String> aggregations) {
        this.aggregations = aggregations;
    }

    
}
