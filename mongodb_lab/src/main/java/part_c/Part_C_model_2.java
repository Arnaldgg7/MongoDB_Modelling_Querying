package part_c;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.company.Company;
import io.codearte.jfairy.producer.person.Person;
import org.bson.Document;
import static part_c.M2_queries.*;

public class Part_C_model_2 {

	public static void dataGenerator(int N) {
		Fairy fairy = Fairy.create();

		MongoClient client = new MongoClient();
		MongoDatabase database = client.getDatabase("M2");
		MongoCollection<Document> personCollection = database.getCollection("Person_Company");

		String personName = "";
		for (int i = 0; i < N; ++i) {
			Person person = fairy.person();

			// Creating and inserting the person:
			Document randomPerson_Comp = new Document();
			randomPerson_Comp.put("_id", person.getPassportNumber());
			randomPerson_Comp.put("fullName", person.getFullName());
			randomPerson_Comp.put("passportNumber", person.getPassportNumber());
			randomPerson_Comp.put("yearOfBirth", person.getDateOfBirth().year().get());
			randomPerson_Comp.put("age", person.getAge());

			// Inserting only the required, non-redundant Company attributes for the queries directly
			// to the 'root' of the Document itself, along with Person attributes, which is simply the
			// company name. That is, the 'vatId' becomes now redundant, since the '_id' of the document
			// is now the 'passportNumber' of the person, and we don't need anymore the unique identifier
			// for the company, as it is not required for the queries. The company name itself is enough.
			Company company = person.getCompany();
			randomPerson_Comp.put("companyName", company.getName());
			personCollection.insertOne(randomPerson_Comp);

			if (i == 0) {
				personName = person.getFullName();
			}
		}

		Document example = new Document();
		example.put("fullName", personName);

		long startTime = System.currentTimeMillis(); // Get time at the start of the query
		String exampleResult = personCollection.find(example).first().toJson();
		long queryTime = System.currentTimeMillis() - startTime; // Measure query execution time

		System.out.println("First inserted person [" + queryTime + " ms]: " + exampleResult);

		client.close();
		
	}

	public static void perform_queries() {
		Q1();
		Q2();
		Q3();
		Q4();
		printTimes();
	}
}
