package part_c;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.company.Company;
import io.codearte.jfairy.producer.person.Person;
import org.bson.Document;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static part_c.M1_queries.*;

public class Part_C_model_1 {

	public static void dataGenerator(int N) {

		Fairy fairy = Fairy.create();

		MongoClient client = new MongoClient();
		MongoDatabase database = client.getDatabase("M1");
		MongoCollection<Document> personCollection = database.getCollection("Person");
		MongoCollection<Document> companyCollection = database.getCollection("Company");

		String personName = "";
		String companyName = "";
		for (int i = 0; i < N; ++i) {
			Person person = fairy.person();

			// Creating but not inserting the person, since we need to match its company to the
			// existing ones afterwards:
			Document randomPerson = new Document();
			randomPerson.put("_id", person.getPassportNumber());
			randomPerson.put("fullName", person.getFullName());
			randomPerson.put("passportNumber", person.getPassportNumber());
			randomPerson.put("yearOfBirth", person.getDateOfBirth().year().get());
			randomPerson.put("age", person.getAge());

			// Checking if there is another company with the same name, but perhaps different '_id' (vatId)
			// in order to match its '_id' and be able to get more employees in its array:
			Company company = person.getCompany();
			Document exists = companyCollection.find(eq("name", company.getName())).first();

			if (exists != null) {
				// If so, simply add the '_id' of the person we've just inserted to the existing company's
				// 'employees' array, add the 'companyId' attribute with the '_id' of this company and
				// insert the person with the right 'companyId' value in the 'Person' collection as well:
				String companyId = exists.getString("_id");
				companyCollection.updateOne(eq("_id", companyId), push("employees", person.getPassportNumber()));
				randomPerson.put("companyId", companyId);
				personCollection.insertOne(randomPerson);
			}
			else {
				// If we are placed in this 'else' clause means that there is no existing company with the
				// company name of the person, so we simply add its 'id' as it has been randomly generated
				// in the 'Person' class and we finally insert the person in the 'Person' collection:
				randomPerson.put("companyId", company.getVatIdentificationNumber());
				personCollection.insertOne(randomPerson);

				// Then, we just insert this new company as it is in the collection:
				Document randomCompany = new Document();
				randomCompany.put("_id", company.getVatIdentificationNumber());
				randomCompany.put("vatId", company.getVatIdentificationNumber());
				randomCompany.put("name", company.getName());
				randomCompany.put("employees", Lists.newArrayList(person.getPassportNumber()));
				companyCollection.insertOne(randomCompany);
			}

			if (i == 0) {
				personName = person.getFullName();
				companyName = company.getName();
			}
		}

		Document example1 = new Document();
		example1.put("fullName", personName);
		Document example2 = new Document();
		example2.put("name", companyName);

		long startTime = System.currentTimeMillis(); // Get time at the start of the query
		String exampleResult1 = personCollection.find(example1).first().toJson();
		String exampleResult2 = companyCollection.find(example2).first().toJson();
		long queryTime = System.currentTimeMillis() - startTime; // Measure query execution time

		System.out.println("First inserted person [" + queryTime + " ms]: " + exampleResult1);
		System.out.println("First inserted company [" + queryTime + " ms]: " + exampleResult2);

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
