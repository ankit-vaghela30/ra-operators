/*****************************************************************************************
* @file  TestTupleGenerator.java
*
* @author   Sadiq Charaniya, John Miller
*/

import static java.lang.System.out;

/*****************************************************************************************
* This class tests the TupleGenerator on the Student Registration Database defined in the
* Kifer, Bernstein and Lewis 2006 database textbook (see figure 3.6).  The primary keys
* (see figure 3.6) and foreign keys (see example 3.2.2) are as given in the textbook.
*/
public class TestTupleGenerator
{
   /*************************************************************************************
    * The main method is the driver for TestGenerator.
    * @param args  the command-line arguments
    */
   public static void main (String [] args)
   {
       TupleGenerator test = new TupleGeneratorImpl ();

       test.addRelSchema ("Student",
                          "id name",
                          "Integer String",
                          "id",
                          null);
       test.addRelSchema ("Transcript",
                          "transid studId grade",
                          "Integer Integer String",
                          "transid",
                          new String [][] {{ "studId", "Student", "id"}});

      

       Table student = new Table ("Student",
                          "id name",
                          "Integer String",
                          "id");       

         Table transcript = new Table ("Transcript",
                          "transid studId grade",
                          "Integer Integer String",
                          "transid");
     

      int tups [] = new int [] { 10, 900000 };

       String [] tables = { "Student", "Transcript" };

       Table [] tablesObj = { student, transcript };


       Comparable [][][] resultTest = test.generate (tups);

       Comparable [] tuple = null;
	   Comparable [] tempTuple = null;
       for (int i = 0; i < resultTest.length; i++) {
          // out.println (tables [i]);
           for (int j = 0; j < resultTest [i].length; j++) {
             //Added to create a tuple
             tuple = new Comparable[resultTest [i][j].length];
               for (int k = 0; k < resultTest [i][j].length; k++) {
                   //out.print (resultTest [i][j][k] + ",");
                   tuple[k] = resultTest [i][j][k];
               } // for
               //out.println ();
               tablesObj[i].insert(tuple);
           } // for
           //out.println ();
		   if(i == 0){
			   tempTuple = tuple;
		   }
       } // for
       
       //Ankit Code Begins------------------------------------------------------------------------
       out.println ("This is for select with LinHashMap Ankit:");
       long totalTime_linhMap_select =0;
       long startTime_linhMap_select;
       long endTime_linhMap_select;
       Table t_iselect_linhMap_select = null;
	   Table t_join_linhMap_select = null;
       startTime_linhMap_select = System.currentTimeMillis();
       
	   for (int i = 0; i<1000; i++)
       {
    	  t_join_linhMap_select = student.select (new KeyType (tempTuple[0]));		//selects the last entry which is inserted from the last table transcript
		 
       }
	   
       endTime_linhMap_select = System.currentTimeMillis();
       totalTime_linhMap_select = endTime_linhMap_select-startTime_linhMap_select;
       //t_join_linhMap_select.print();
       out.println("total time is: "+totalTime_linhMap_select);
       
       out.println("total time for each loop select is: "+(double)((double)totalTime_linhMap_select/(double)(1000)));
	   
	   out.println ("This is for join with LinHashMap Ankit:");
       long totalTime_linhMap_join =0;
       long startTime_linhMap_join;
       long endTime_linhMap_join;
       Table t_iselect_linhMap_join = null;
	   Table t_join_linhMap_join = null;
       startTime_linhMap_join = System.currentTimeMillis();
       
	   for (int i = 0; i<1000; i++)
       {
    	  t_join_linhMap_join = transcript.i_join ("studId" , "id", student);
		 
       }
	   
       endTime_linhMap_join = System.currentTimeMillis();
       totalTime_linhMap_join = endTime_linhMap_join - startTime_linhMap_join;
       //t_join_linhMap_join.print();
       out.println("total time is: "+totalTime_linhMap_join);
       
       out.println("total time for each loop join is: "+(double)((double)totalTime_linhMap_join/(double)(1000)));
	   
	   //Ankit Code ends------------------------------------------------------------------------
      /* 
       out.println ("This is indexSelect output:");
       long totalTime =0;
       long startTime;
       long endTime;
       Table t_iselect = null;
       startTime = System.currentTimeMillis();
       for (int i = 0; i<1000; i++)
       {
    	  t_iselect = studentAlumni.select (new KeyType (tuple[0]));		//selects the last entry which is inserted from the last table transcript  
       } 
       endTime = System.currentTimeMillis();
       totalTime = endTime-startTime;
       t_iselect.print();
       out.println("total time is: "+totalTime);
       
       out.println("total time for each loop is: "+(double)((double)totalTime/(double)(1000)));
       
       
       out.println ("This is for join with LinHashMap Ankit:");
       long totalTime_linhMap_join =0;
       long startTime_linhMap_join;
       long endTime_linhMap_join;
       Table t_iselect_linhMap_join = null;
	   Table t_join_linhMap_join = null;
       startTime_linhMap_join = System.currentTimeMillis();
       
	   for (int i = 0; i<1000; i++)
       {
    	  t_join_linhMap_join = studentAlumni.i_join ("id" , "id", student);
		 
       }
	   
       endTime_linhMap_join = System.currentTimeMillis();
       totalTime_linhMap_join = endTime_linhMap_join - startTime_linhMap_join;
       //t_join_linhMap_join.print();
       out.println("total time is: "+totalTime_linhMap_join);
       
       out.println("total time for each loop join is: "+(double)((double)totalTime_linhMap_join/(double)(1000)));
       
//     **************** For HASH JOIN ****************

       out.println ("For HASH JOIN:");
       long hTotalTime  = 0;
       long hStartTime;
       long hEndTime;
       Table testing_h_join = null;

       hStartTime = System.currentTimeMillis();
       for (int i = 0; i < 1000; i++) {
           testing_h_join = studentAlumni.h_join("id" , "id", student);
       }
       hEndTime = System.currentTimeMillis();

       hTotalTime = hEndTime - hStartTime;
       testing_h_join.print();

       out.println("total time for HJ is: " + hTotalTime);
       
       out.println("total time for each loop for HJ is: "+(double)((double)hTotalTime/(double)(1000)));              

       
 //  	************* INDEX JOIN ********************
 // 	  	- Johnathan Kulovitz
       
       out.println("For INDEX JOIN:");
       long iTotalTime = 0;
       long iStartTime;
       long iEndTime;
       Table testing_i_join = null;
       
       iStartTime = System.currentTimeMillis();
       for (int i = 0; i < 1000; i ++) {
    	   		testing_i_join = transcript.i_join("studId", "id", student);
       }
       iEndTime = System.currentTimeMillis();
       
       iTotalTime = iEndTime - iStartTime;
       //testing_i_join.print();
       
       out.println("Total time for Index Join is : " + iTotalTime);
       out.println("Average Time for each iteration of Index Join is: " +(double)((double)iTotalTime/(double)(1000)));
       
       
       //  	************* NESTED LOOP JOIN (NLJ) ********************
       // 	  	- Johnathan Kulovitz
             
             out.println("For Nested Loop Join:");
             long nljTotalTime = 0;
             long nljStartTime;
             long nljEndTime;
             Table testing_nlj_join = null;
             
             nljStartTime = System.currentTimeMillis();
             for (int i = 0; i < 1000; i ++) {
          	   		testing_nlj_join = transcript.join("studId", "id", student);
             }
             nljEndTime = System.currentTimeMillis();
             
             nljTotalTime = nljEndTime - nljStartTime;
             //testing_nlj_join.print();
             
             out.println("Total time for Nested Loop Join is : " + nljTotalTime);
             out.println("Average Time for each iteration of Nested Loop Join is: " +(double)((double)nljTotalTime/(double)(1000)));
			 */
       
   } // main



} // TestTupleGenerator