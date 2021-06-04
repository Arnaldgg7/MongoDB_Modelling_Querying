package part_c;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.company.Company;
import io.codearte.jfairy.producer.person.Person;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;
import static part_c.M3_queries.*;

public class Part_C_model_3 {

	public static void dataGenerator(int N) {
		Fairy fairy = Fairy.create();

		MongoClient client = new MongoClient();
		MongoDatabase database = client.getDatabase("M3");
		MongoCollection<Document> companyCollection = database.getCollection("Company_Person");

		String companyName = "";
		for (int i = 0; i < N; ++i) {
			Person person = fairy.person();

			// Creating and the person document without the '_id' field, since now it is embedded and the '_id'
			// for the main document is going to be the 'vatId' of the company, as well as its 'companyId'
			// attribute, which is now part of the 'root' or main document:
			Document randomPerson = new Document();
			randomPerson.put("personName", person.getFullName());
			randomPerson.put("passportNumber", person.getPassportNumber());
			randomPerson.put("yearOfBirth", person.getDateOfBirth().year().get());
			randomPerson.put("age", person.getAge());

			// Getting the company by the existing company the person works for, in order to make sense of
			// the whole schema and avoiding to create companies that nobody works for:
			Company company = person.getCompany();

			//Now, we have to check if this company already exists:
			Document exists = companyCollection.find(eq("name", company.getName())).first();

			if (exists != null) {
				// If so, simply add the entire person document to its 'employees' array:
				String companyId = exists.getString("_id");
				companyCollection.updateOne(eq("_id", companyId), push("employees", randomPerson));
			}
			else {
				// If that's not the case, just insert this new company in the collection, with the entire document
				// of the 'Person' as the first list element in 'employees':
				Document randomCompany = new Document();
				randomCompany.put("_id", company.getVatIdentificationNumber());
				randomCompany.put("vatId", company.getVatIdentificationNumber());
				randomCompany.put("name", company.getName());
				randomCompany.put("employees", Lists.newArrayList(randomPerson));
				companyCollection.insertOne(randomCompany);
			}

			if (i == 0) {
				companyName = company.getName();
			}
		}

		Document example = new Document();
		example.put("name", companyName);

		long startTime = System.currentTimeMillis(); // Get time at the start of the query
		String exampleResult = companyCollection.find(example).first().toJson();
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
