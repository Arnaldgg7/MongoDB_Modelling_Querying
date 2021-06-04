import part_c.Part_C_example;
import part_c.Part_C_model_1;
import part_c.Part_C_model_2;
import part_c.Part_C_model_3;
import util.Utils;

public class MongoDB {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 1) {
			throw new Exception("Wrong number of parameters, usage: (partc_example,partc_model1,partc_model2,partc_model3); extra parameters specifics for the exercise");
		}
		if (args[0].equals("partc_example")) {
			if (args.length != 2 || !Utils.isNumber(args[1])) {
				throw new Exception("[Error ex2_example] params: N (number of documents to create)");
			}
			Part_C_example.dataGenerator(Integer.parseInt(args[1]));
		}
		if (args[0].equals("partc_model1")) {
			if (args[1].equals("queries")) {
				Part_C_model_1.perform_queries();
			}
			else if (args.length != 2 || !Utils.isNumber(args[1])) {
				throw new Exception("[Error partc_model1] params: N (number of documents to create)");
			}
			else {
				Part_C_model_1.dataGenerator(Integer.parseInt(args[1]));
			}
		}
		if (args[0].equals("partc_model2")) {
			if (args[1].equals("queries")) {
				Part_C_model_2.perform_queries();
			}
			else if (args.length != 2 || !Utils.isNumber(args[1])) {
				throw new Exception("[Error partc_model2] params: N (number of documents to create)");
			}
			else {
				Part_C_model_2.dataGenerator(Integer.parseInt(args[1]));
			}

		}
		if (args[0].equals("partc_model3")) {
			if (args[1].equals("queries")) {
				Part_C_model_3.perform_queries();
			}
			else if (args.length != 2 || !Utils.isNumber(args[1])) {
				throw new Exception("[Error partc_model3] params: N (number of documents to create)");
			}
			else {
				Part_C_model_3.dataGenerator(Integer.parseInt(args[1]));
			}
		}
		
	}
	
}
