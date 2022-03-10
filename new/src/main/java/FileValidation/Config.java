package FileValidation;

public class Config {
	//Configuration parameters for file validation
	static String bucketPath = "gs://apdh-test-bucket/data/conf/*";
	static String headers = "id_property,Ministry,Health_System,Property_Name,Street_Address,Address_2,City,State,Zip_Code,Owned_Leased,SquareFootage_External,SquareFootage_Internal,SquareFootage_Rentable,SquareFootage_Usable,isDeleted,Date";

	//static String fileName = "gs://apdh-test-bucket/Medxcel_sample_data.csv";
	static String fileName = "gs://apdh-test-bucket/anotherfile.csv";
	static String statePath = "gs://apdh-test-bucket/data/conf/statefile.csv";
	
}