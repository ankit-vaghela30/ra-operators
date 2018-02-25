import java.sql.*;

public class JDBCExample {
	
	private static final String QUERY_END = ");";
	private static final String QUERY_STUDENT = "Insert into student values (";
	private static final String QUERY_DEPARTMENT = "Insert into department values (";
	private static final String QUERY_PROFESSOR = "Insert into professor values (";
	private static final String QUERY_COURSE = "Insert into course values (";
	private static final String QUERY_TEACHING = "Insert into teaching values (";
	private static final String QUERY_COURSES_STUDENT = "Insert into courses_student values (";

	public static void main(String[] argv) {

		
		try {

		    Class.forName("org.postgresql.Driver");

			Connection connection = null;

		
			connection = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/Project4_dummy", "postgres",
					"ankit@123");
			Statement st = connection.createStatement();
			
			TupleGenerator test = new TupleGeneratorImpl ();

			test.addRelSchema ("Student",
                           "id name address status",
                           "Integer String String String",
                           "id",
                           null);
						   
			test.addRelSchema ("Department",
							   "id name",
							   "String String",
							   "id",
							   null);
        
			test.addRelSchema ("Professor",
							   "id name deptId",
							   "Integer String String",
							   "id",
							    new String [][] {{"deptId", "Department", "id" }});
			
			test.addRelSchema ("Course",
							   "crsCode deptId crsName",
							   "String String String",
							   "crsCode",
							   new String [][] {{"deptId", "Department", "id" }});
			
			test.addRelSchema ("Teaching",
							   "crsCode semester profId",
							   "String String Integer",
							   "crcCode semester",
							   new String [][] {{ "profId", "Professor", "id" },
												{ "crsCode", "Course", "crsCode" }});
												
			test.addRelSchema ("Courses_Student",
							   "id crscode",
							   "Integer String",
							   "id crscode",
							   new String [][] {{ "id", "Student", "id" },
												{ "crscode", "Course", "crsCode" }});
							   
			Table student = new Table ("Student",
                           "id name address status",
                           "Integer String String String",
                           "id");       
			
			Table department = new Table("Department",
							   "id name",
							   "String String",
							   "id");
			
			Table professor = new Table("Professor",
							   "id name deptId",
							   "Integer String String",
							   "id");
			
			Table course = new Table("Course",
							   "crsCode deptId crsName",
							   "String String String",
							   "crsCode");

			Table teaching = new Table ("Teaching",
							   "crsCode semester profId",
							   "String String Integer",
							   "crcCode semester");
			
			Table courses_student = new Table("Courses_Student",
							   "id crscode",
							   "Integer String",
							   "id crscode");
			 

			   int tups [] = new int [] { 4, 4, 4, 4, 4, 4 };

			   String [] tables = { "Student", "Department", "Professor", "Course", "Teaching", "Courses_Student" };

			   Table [] tablesObj = { student, department, professor, course, teaching, courses_student };


			   Comparable [][][] resultTest = test.generate (tups);

			   for (int i = 0; i < resultTest.length; i++) {
				  // out.println (tables [i]);
				   for (int j = 0; j < resultTest [i].length; j++) {
					 //Added to create a tuple
					 //tuple = new Comparable[resultTest [i][j].length];
					 String query = "";
					   for (int k = 0; k < resultTest [i][j].length; k++) {
						   //out.print (resultTest [i][j][k] + ",");
						   //tuple[k] = resultTest [i][j][k];
						   
						   if (i == 0){
							   if(k == 0){
								 query = query + resultTest [i][j][k] + ",";
							   }else if(k == (resultTest [i][j].length -1)){
							     query = query + "'" + resultTest [i][j][k] + "'";
							   }else{
								 query = query + "'" + resultTest [i][j][k] + "',";
							   }
						   }
						   if (i == 1){
							   if(k == 0){
								 query = query + "'" + resultTest [i][j][k] + "'" + ",";
							   }else {
							     query = query + "'" + resultTest [i][j][k] + "'";
							   }
						   }
						   else if(i == 2){
								if(k == 0){
								 query = query + resultTest [i][j][k] + ",";
							   }else if(k == (resultTest [i][j].length -1)){
							     query = query + "'" + resultTest [i][j][k] + "'";
							   }else{
								 query = query + "'" + resultTest [i][j][k] + "',";
							   }
						   }
						   else if(i == 3){
								if(k == (resultTest [i][j].length -1)){
								 query = query + "'" + resultTest [i][j][k] + "'";
							   }else{
								 query = query + "'" + resultTest [i][j][k] + "',";
							   }
						   }
						   else if(i == 4){
								if(k == (resultTest [i][j].length -1)){
								 query = query + resultTest [i][j][k];
							   }else{
								 query = query + "'" + resultTest [i][j][k] + "',";
							   }
						   }
						   else if(i == 5){
								if(k == (resultTest [i][j].length -1)){
								 query = query + "'" + resultTest [i][j][k] + "'";
							   }else{
								 query = query + resultTest [i][j][k] + ",";
							   }
						   }
					   } // for
					   //out.println ();
					   if (i == 0){
						   System.out.println("Query is--"+QUERY_STUDENT+query+QUERY_END);
						   st.executeUpdate(QUERY_STUDENT+query+QUERY_END);
						   
					   }else if(i == 1){
						   System.out.println("Query is--"+QUERY_DEPARTMENT+query+QUERY_END);
						   st.executeUpdate(QUERY_DEPARTMENT+query+QUERY_END);
						   
					   }else if (i == 2){
						   System.out.println("Query is--"+QUERY_PROFESSOR+query+QUERY_END);
						   st.executeUpdate(QUERY_PROFESSOR+query+QUERY_END);
						   
					   }
					   else if(i == 3){
						   System.out.println("Query is--"+QUERY_COURSE+query+QUERY_END);
						   st.executeUpdate(QUERY_COURSE+query+QUERY_END);
						   
					   }else if (i == 4){
						   System.out.println("Query is--"+QUERY_TEACHING+query+QUERY_END);
						   st.executeUpdate(QUERY_TEACHING+query+QUERY_END);
						   
					   }else{
						   System.out.println("Query is--"+QUERY_COURSES_STUDENT+query+QUERY_END);
						   st.executeUpdate(QUERY_COURSES_STUDENT+query+QUERY_END);
						   
					   }
					   
					   //tablesObj[i].insert(tuple);
				   } // for
				   //out.println ();
				  
			   } // for
			
			
			/*while (rs.next())
			{
				System.out.print("Column 1 returned ");
				System.out.println(rs.getString(1)+ ","+rs.getString(2).trim()+","+rs.getString(3));
			}*/
			
			
			connection.close();

		} catch (Exception e) {

			System.out.println("Something went wrong here");
			e.printStackTrace();
			return;

		}
	}

}
