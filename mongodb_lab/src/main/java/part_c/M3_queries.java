package part_c;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import java.util.Arrays;
import java.util.List;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

public class M3_queries {
    static long Q1_time = 0l;
    static long Q2_time = 0l;
    static long Q3_time = 0l;
    static long Q4_time = 0l;

    public static void Q1() {
        System.out.println("Q1 result:");
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("M3");
        MongoCollection<Document> companyCollection = database.getCollection("Company_Person");

        long startTime = System.currentTimeMillis();
        companyCollection.find().forEach(company -> {
            List<Document> people = company.getList("employees", Document.class);
            for (Document person : people) {
                System.out.println("Person " + person.get("personName") + "works in company " + company.get("name"));
            }
        });
        Q1_time = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println();

        client.close();
    }


    public static void Q2() {
        System.out.println("Q2 result:");
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("M3");
        MongoCollection<Document> companyCollection = database.getCollection("Company_Person");

        long startTime = System.currentTimeMillis();
        companyCollection.find().forEach(company -> {
            List<Document> employees = company.getList("employees", Document.class);
            System.out.println("Company's name: " + company.get("name") + "\t-\t" + "Number of employees: " + employees.size());
        });
        Q2_time = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println();

        client.close();
    }


    public static void Q3() {
        System.out.println("Q3 result:");
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("M3");
        MongoCollection<Document> companyCollection = database.getCollection("Company_Person");

        long startTime = System.currentTimeMillis();
        companyCollection.updateMany(
                eq("employees.yearOfBirth", new Document("$lt", 1988)),
                set("employees.$[person].age", 30),
                new UpdateOptions().arrayFilters(Arrays.asList(eq("person.yearOfBirth", new Document("$lt", 1988))))
        );
        Q3_time = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println();

        client.close();
    }


    public static void Q4() {
        System.out.println("Q4 result:");
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("M3");
        MongoCollection<Document> companyCollection = database.getCollection("Company_Person");

        long startTime = System.currentTimeMillis();
        MongoCursor<Document> Q4_result = companyCollection.find().iterator();

        while(Q4_result.hasNext()) {
            Document company = Q4_result.next();
            String curr_name = company.getString("name");
            String new_name = "Company " + curr_name;
            companyCollection.updateOne(eq("name", curr_name), set("name", new_name));
        }
        System.out.println("All companies updated.");
        Q4_time = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println();

        client.close();
    }


    public static void printTimes() {
        System.out.println("Q1 query time (ms): " + Q1_time);
        System.out.println("Q2 query time (ms): " + Q2_time);
        System.out.println("Q3 query time (ms): " + Q3_time);
        System.out.println("Q4 query time (ms): " + Q4_time);
    }
}
