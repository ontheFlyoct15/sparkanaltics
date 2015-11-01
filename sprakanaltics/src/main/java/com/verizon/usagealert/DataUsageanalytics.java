package com.verizon.usagealert;

import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.Serializable;

public class DataUsageanalytics implements Serializable {
    private transient SparkConf conf;
  
    private DataUsageanalytics(SparkConf conf) {
        this.conf = conf;
    }
  
    private void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        generateData(sc);
        compute(sc);
        showResults(sc);
        sc.stop();
    }
  
    private void generateData(JavaSparkContext sc) {
    	CassandraConnector connector = CassandraConnector.apply(sc.getConf());
    	 
    /*	try (Session session = connector.openSession()) {
    	    session.execute("DROP KEYSPACE IF EXISTS java_api");
    	    session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    	    session.execute("CREATE TABLE java_api.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
    	    session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
    	    session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
    	}*/
    }
  
    private void compute(JavaSparkContext sc) {
    }
  
    private void showResults(JavaSparkContext sc) {
    }
  
    public static void main(String[] args) {
//        if (args.length != 2) {
//            System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
//            System.exit(1);
//        }
    	
    	
    	/*The problem is that you specify 2 schemas in the URL you pass to SparkConf.setMaster().

    	The spark is the schema, so you don't need to add http after spark. See the javadoc of SparkConf.setMaster() for more examples.

    	So the master URL you should be using is "spark://localhost:18080". Change this line:
*/
//    	SparkConf conf = new SparkConf().setAppName("Data_usage")
//    	    .setMaster("spark://113.128.163.213:65443").set("spark.ui.port","65443");
//        SparkConf conf = new SparkConf();
//        conf.setAppName("Java API demo");
//        conf.setMaster("113.128.163.213:4041");
    	System.out.println("start ");;
    	SparkConf conf = new SparkConf().setMaster("spark://113.128.163.213:4041");
    	conf.setAppName("Java API demo");
    	System.out.println("end");
        conf.set("spark.cassandra.connection.host", "113.128.163.213");
        DataUsageanalytics app = new DataUsageanalytics(conf);
        app.run();
    } 
}
