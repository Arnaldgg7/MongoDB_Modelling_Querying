package part_c;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import java.util.List;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

public class M2_queries {
    static long Q1_time = 0l;
    static long Q2_time = 0l;
    static long Q3_time = 0l;
    static long Q4_time = 0l;

    public static void Q1() {
        System.out.println("Q1 result:");
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("M2");
        MongoCollection<Document> personCollection = database.getCollection("Person_Company");

        long startTime = System.currentTimeMillis();
        personCollection.find().forEach(person -> {
            System.out.println("Person " + person.get("fullName") + "works in company " + person.get("companyName"));
        });
        Q1_time = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println();

        client.close();
    }


    public static void Q2() {
        System.out.println("Q2 result:");
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("M2");
        MongoCollection<Document> personCollection = database.getCollection("Person_Company");

        long startTime = System.currentTimeMillis();
        personCollection.distinct("companyName", String.class).forEach(company -> {
            MongoCursor<Document> employees = personCollection.find(eq("companyName", company)).iterator();

            int size = 0;
            while(employees.hasNext()) {
                employees.next();
                size++;
            }
            System.out.println("Company's name: " + company + "\t-\t" + "Number of employees: " + size);
        });
        Q2_time = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println();

        client.close();
    }


    public static void Q3() {
        System.out.println("Q3 result:");
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("M2");
        MongoCollection<Document> personCollection = database.getCollection("Person_Company");

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
        MongoDatabase database = client.getDatabase("M2");
        MongoCollection<Document> personCollection = database.getCollection("Person_Company");

        // We create a list to add the companies we have already updated their name, so that we get more
        // efficiency by only launching 'updates' to the database if there is actually a company that
        // we have not seen before.
        List<String> companies = Lists.newArrayList();

        long startTime = System.currentTimeMillis();
        personCollection.find().forEach(person -> {
            String curr_name = person.getString("companyName");
            String new_name = "Company " + curr_name;

            // Here, we use the 'updateMany()' in order to update all same companies from all existing
            // 'person' documents in the database at once, every time we retrieve a document with a new
            // company name. Otherwise, if we check that the company is already updated (it is in the
            // above stated list) we do nothing.
            if (!companies.contains(curr_name) && !companies.contains(new_name)) {
                personCollection.updateMany(eq("companyName", curr_name), set("companyName", new_name));
                companies.add(curr_name);
                companies.add(new_name);
            }
        });
        System.out.println("All companies updated.");
        Q4_time = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println();

        // This approach in Q4 query in the M2 model should be more efficient than retrieving all documents,
        // using a 'distinct()' to get all different values in the key 'companyName' and then retrieving
        // again all documents per each 'companyName' to update them, which would require looping twice
        // over the collection.

        client.close();
    }


    public static void printTimes() {
        System.out.println("Q1 query time (ms): " + Q1_time);
        System.out.println("Q2 query time (ms): " + Q2_time);
        System.out.println("Q3 query time (ms): " + Q3_time);
        System.out.println("Q4 query time (ms): " + Q4_time);
    }
}
