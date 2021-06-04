package part_c;

import com.mongodb.MongoClient;
import org.bson.Document;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;

public class Part_C_example {
	
	public static void dataGenerator(int N) {
		Fairy fairy = Fairy.create();
		
		MongoClient client = new MongoClient();
		MongoDatabase database = client.getDatabase("test2");
		MongoCollection<Document> collection = database.getCollection("partc_example");
		
		String oneName = "";
		for (int i = 0; i < N; ++i) {
			Person person = fairy.person();
			
			if (i == 0) {
				oneName = person.getFirstName();
			}
			
			Document random = new Document();
			random.put("firstName", person.getFirstName());
			random.put("passportNumber", person.getPassportNumber());
			// Do the same for the other fields of class Person
			// ...
			collection.insertOne(random);
		}
		
		Document query = new Document();
		query.put("firstName", oneName);

		long startTime = System.currentTimeMillis(); // Get time at the start of the query
		String queryResult = collection.find(query).first().toJson();
		long queryTime = System.currentTimeMillis() - startTime; // Measure query execution time
		
		System.out.println("First inserted person [ " + queryTime + " ms]: " + queryResult);
		
		client.close();	
	}
}
