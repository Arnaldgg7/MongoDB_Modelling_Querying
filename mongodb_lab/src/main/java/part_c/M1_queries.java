package part_c;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import java.util.List;
import static com.mongodb.client.model.Updates.*;
import static com.mongodb.client.model.Filters.eq;

public class M1_queries {
    static long Q1_time = 0l;
    static long Q2_time = 0l;
    static long Q3_time = 0l;
    static long Q4_time = 0l;

    public static void Q1() {
        System.out.println("Q1 result:");
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("M1");
        MongoCollection<Document> personCollection = database.getCollection("Person");
        MongoCollection<Document> companyCollection = database.getCollection("Company");

        long startTime = System.currentTimeMillis();
        personCollection.find().forEach(person -> {
            Document company = companyCollection.find(eq("_id", person.get("companyId"))).first();
            System.out.println("Person " + person.get("fullName") + "works in company " + company.get("name"));
        });
        Q1_time = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println();

        client.close();
    }


    public static void Q2() {
        System.out.println("Q2 result:");
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("M1");
        MongoCollection<Document> companyCollection = database.getCollection("Company");

        long startTime = System.currentTimeMillis();
        companyCollection.find().forEach(company -> {
            List<String> employees = company.getList("employees", String.class);
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
        MongoDatabase database = client.getDatabase("M1");
        MongoCollection<Document> personCollection = database.getCollection("Person");

        Document filter = new Document();
        filter.put("yearOfBirth", new Document("$lt", 1988));

        long startTime = System.currentTimeMillis();
        UpdateResult Q3_result = personCollection.updateMany(filter, set("age", 30));
        System.out.println("Updated: " + Q3_result);

        Q3_time = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println();

        client.close();
    }


    public static void Q4() {
        System.out.println("Q4 result:");
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("M1");
        MongoCollection<Document> companyCollection = database.getCollection("Company");

        long startTime = System.currentTimeMillis();
        companyCollection.find().forEach(company -> {
            String curr_name = company.getString("name");
            String new_name = "Company " + curr_name;
            companyCollection.updateOne(eq("name", curr_name), set("name", new_name));
        });
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
