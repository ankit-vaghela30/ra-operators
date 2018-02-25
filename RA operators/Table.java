/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Boolean.*;
import static java.lang.System.out;

/****************************************************************************************
 * This class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join.  The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

    /** Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /** Counter for naming temporary tables.
     */
    private static int count = 0;

    /** Flag to check if the attribute is present in the table.
     */
    private static boolean isPresent;

    /** Table name.
     */
    private final String name;

    /** Array of attribute names.
     */
    private final String [] attribute;

    /** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;

    /** Collection of tuples (data storage).
     */
    private final List <Comparable []> tuples;

    /** Primary key.
     */
    private final String [] key;

    /** Index into tuples (maps key to tuple number).
     */
    private final Map <KeyType, Comparable []> index;

    /** The supported map types.
     */
    private enum MapType { NO_MAP, TREE_MAP, LINHASH_MAP, BPTREE_MAP }

    /** The map type to be used for indices.  Change as needed.
     */
    private static final MapType mType = MapType.LINHASH_MAP;

    /************************************************************************************
     * Make a map (index) given the MapType.
     */
    private static Map <KeyType, Comparable []> makeMap ()
    {
        switch (mType) {
        case TREE_MAP:    return new TreeMap <> ();
         case LINHASH_MAP: return new LinHashMap <> (KeyType.class, Comparable [].class);
         case BPTREE_MAP:  return new BpTreeMap <> (KeyType.class, Comparable [].class);
        default:          return null;
        } // switch
    } // makeMap

    //-----------------------------------------------------------------------------------
    // Constructors
    //-----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = new ArrayList <> ();
        index     = makeMap ();
    } // primary constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuples     the list of tuples containing the data
     */
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = makeMap ();
    } // constructor

    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param _name       the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     * @param _key        the primary key
     */
    public Table (String _name, String attributes, String domains, String _key)
    {
        this (_name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        String [] attrs     = attributes.split (" ");
        Class []  colDomain = extractDom (match (attrs), domain);
        String [] newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs;

        List <Comparable []> rows = new ArrayList <> ();

        /** @author Niraj Kadam
        // If the attribute is present in the table only then traverse through all the tuples
        // and get the values for the mentioned attributes.
        */

        if (isPresent) {
            this.tuples.stream().forEach(item -> rows.add(extract(item, attrs)));
        }

        return new Table (name + count++, attrs, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");

        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.
     *
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal)
    {
       //out.println("AKSHAY CODE HERE");
        out.println ("RA> " + name + ".select (" + keyVal + ")");
        List <Comparable []> rows = new ArrayList <> ();
          Comparable[] a =null;
       //@author akshay.
         if(mType != MapType.NO_MAP)
        {//get value from key. get method of respective Maps are called.
           a = index.get(keyVal);
        }
        else
        {
          out.println("Please select a Map. MapType is NO_MAP currently");
        }
        if(a != null) //check if value returned is null of not
        rows.add(a);

        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (keyval1 <= value < keyval2).
     * Use an B+ Tree index (SortedMap) to retrieve the tuples with keys in the given range.
     *
     * @param keyVal1  the given lower bound for the range (inclusive)
     * @param keyVal2  the given upper bound for the range (exclusive)
     * @return  a table with the tuples satisfying the key predicate
     */
    public Table select (KeyType keyVal1, KeyType keyVal2)
    {
         out.println ("RA> " + name + ".select between (" + keyVal1 + ") and " + keyVal2);
        List <Comparable []> rows = new ArrayList <> ();

         if(mType == MapType.BPTREE_MAP || mType == MapType.TREE_MAP)
    {

       //@author Akshay Mendki
      @SuppressWarnings("rawtypes")

      SortedMap<KeyType,Comparable[]> sortedMap =  ((SortedMap<KeyType, Comparable[]>) index).subMap(keyVal1, keyVal2);


     //Get all Values from sortedMap retrieved.
      Collection<Comparable[]> values = sortedMap.values();

      Iterator i= values.iterator();

      //Loop through the values
      while(i.hasNext())
      {
    	//add values into rows
    	 rows.add((Comparable[]) i.next());
      }
    }
    else
    {
            if((mType == MapType.NO_MAP)) {
                // @author Niraj
                int priKeys[] = match(key);

                for (int i = 0; i < this.tuples.size(); i++) {
                    Comparable [] keyVal = new Comparable [priKeys.length];

                    // iterating through primary key tuple(s)
                    for (int pk = 0; pk < priKeys.length ; pk++)
                        keyVal[pk] = this.tuples.get(i)[priKeys[pk]];

                    if (new KeyType(keyVal).compareTo(keyVal2) < 0 ) {
                       if (new KeyType(keyVal).compareTo(keyVal1) >= 0) {
                            rows.add(this.tuples.get(i));
                        }
                    }
                }
            }
      else
      {
        	out.println("MapType is not B+ or TreeMap or NoMap. Range select will not work on LinearHashMap");
      }
    }
      return new Table (name + count++, attribute, domain, key, rows);
    } // range_select

    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");

        List <Comparable []> rows = new ArrayList <> ();

        // returning empty table in case tables are not compatible. Code changed to avoid NullPointerException.
        if (! compatible (table2)) return new Table (name + count++, attribute, domain, key, rows);

    /** The first for loop cycles through the first union table
     *  and adds all of these rows to the return table.
     */
    for (int i = 0; i < this.tuples.size(); i++) {
        rows.add(this.tuples.get(i));
    }

    /** The second for loop goes through the second table and
     *  adds anything that isn't a duplicate to the results table
     */
    for (int i = 0; i < table2.tuples.size(); i++) {
        /** The match boolean is used to determine if a duplicate
         *  was found in the scan of the table.
         */
        boolean match = true;
        for (int j = 0; j < this.tuples.size(); j++) {
        /** The below expression determines if the current row in
         *  table 2 is the same as any of the rows in table 1.
         *  if there is a match, it moves on without adding to the results
         */
        if (table2.tuples.get(i).equals(this.tuples.get(j))) {
            match = false;
            break;
        }
        }
        /** If there is no match, the tuple is added
         */
        if (match) {
        rows.add(table2.tuples.get(i));
        }
    }
        return new Table (name + count++, attribute, domain, key, rows);
    } // union

    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");

        // returning empty table in case tables are not compatible. Code changed to avoid NullPointerException.

        List <Comparable []> rows = new ArrayList <> ();
        if (! compatible (table2)) return new Table (name + count++, attribute, domain, key, rows);

        /*  Using streams, filter out the tuples from table1 which are present in table2
         *  and using foreach add the tuples of table1 which are not present in table2 to rows.
         */
        this.tuples.stream().filter(item -> !(table2.tuples.contains(item)))
                            .forEach(item -> rows.add(item));

        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by append "2" to the end of any duplicate attribute name.  Implement using
     * a Nested Loop Join algorithm.
     *
     * #usage movie.join ("studioNo", "name", studio)
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
     public Table join (String attributes1, String attributes2, Table table2)
    {
        //out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
          //       + table2.name + ")");

        String [] t_attrs = attributes1.split (" ");
        String [] u_attrs = attributes2.split (" ");

        List <Comparable []> rows = new ArrayList <> ();

        // Implemented by: Ankit Vaghela

    // Comment added by Ankit: Method match is used to get the column positions of attributes t_attrs and u_attrs
    int [] colPosAttr1 = this.match(t_attrs);
    int [] colPosAttr2 = table2.match(u_attrs);

        // Comment added by Ankit: Method extractDom is used to get the domains for columns retrieved in last step
    Class [] domainsTable1 = this.extractDom(colPosAttr1, this.domain);
        Class [] domainsTable2 = this.extractDom(colPosAttr2, table2.domain);

        String [] attributesTable2 = new String [table2.attribute.length];

        // Comment added by Ankit: If the domains are different then this operation should be stopped
        // (This saves time by not executing below step of nested for loops)

        if(Arrays.equals(domainsTable1, domainsTable2)) {

        /*  Comment added by Ankit: Below for loops do Cartesian product of tuples of this table and
         *  table 2 by matching tuple values of relevant attribute and puts resultant tuples in row array.
         *  boolean matchesWhole = true;
         */

            for (Comparable [] tup1 : this.tuples) {
                for (Comparable [] tup2 : table2.tuples) {
                    int matchesWhole = 0;

                    for (int i = 0; i < colPosAttr1.length; i++ ) {
                        if (tup1 [ (int)colPosAttr1[i]].equals(tup2 [(int)colPosAttr2[i]])) {
                            matchesWhole++;
                        }
                        if (i == colPosAttr1.length -1 && matchesWhole == colPosAttr1.length) {
                            rows.add(ArrayUtil.concat(tup1, tup2));
                        }
                    }
                }
            }

        //  Comment added by Ankit: Now appending "2" after duplicate columns to disambiguate as
        //  suggested in the comments of this method

            for (int i = 0; i < table2.attribute.length; i ++) {
                attributesTable2[i] = table2.attribute[i];
            }

            for (int attribute2 = 0; attribute2 < attributesTable2.length; attribute2++) {
                for (int attribute1 = 0; attribute1 < this.attribute.length; attribute1++) {
                    if (this.attribute[attribute1].equalsIgnoreCase(attributesTable2[attribute2])) {
                        attributesTable2[attribute2] = attributesTable2[attribute2] + "2";
                    }
                }
            }
        }

        return new Table (name + count++, ArrayUtil.concat (attribute, attributesTable2),
                            ArrayUtil.concat (domain, table2.domain), this.findPrimaryKeyForJoin(attributes1, attributes2, table2), rows);
    } // join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using an Index Join algorithm.
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table i_join (String attributes1, String attributes2, Table table2)
    {
        //Johnathan Kulovitz and Ankit worked on this.

      //System.out.println("Key length: "+key.length+" attribute length : "+temp.length);
      List <Comparable []> rows = new ArrayList <> ();

      if(mType != MapType.TREE_MAP)
     {
      //If the key is beyond length one, this will turn it into an array.
      Comparable [] attributesSplit = attributes1.split(" ");
      Comparable [] attributesSplit2 = attributes2.split(" ");

        String [] attributesTable2 = new String [table2.attribute.length];

      if (attributesSplit2.length == table2.key.length && 0 == new KeyType(table2.key).compareTo(new KeyType(attributesSplit2)))
     {

  int [] cols = match(attributes1.split(" "));



  for (int i = 0; i < this.tuples.size(); i++)
     {
  //This tuple is used to get only the key data frome the tuples
  Comparable [] t = new Comparable[cols.length];
  for ( int j = 0; j < t.length; j++)
     {
  t[j] = this.tuples.get(i)[cols[j]];

     }//for
  //Concats the rest of the row and adds it to the result array list.
  Comparable [] u = table2.index.get(new KeyType(t));
  if (u != null)
     {
  rows.add(ArrayUtil.concat(this.tuples.get(i), u));
     }//if
     }//for
     }//if
      else

     {
  out.println("Tables cannot be joined");
  return null;
     }

      // appends a 2 to any duplicate column names
            for (int i = 0; i < table2.attribute.length; i ++)
                attributesTable2[i] = table2.attribute[i];

            for (int attribute2 = 0; attribute2 < attributesTable2.length; attribute2++)
                for (int attribute1 = 0; attribute1 < this.attribute.length; attribute1++)
                    if (this.attribute[attribute1].equalsIgnoreCase(attributesTable2[attribute2]))
                        attributesTable2[attribute2] = attributesTable2[attribute2] + "2";

      Table result = new Table (name + count++, ArrayUtil.concat(attribute, attributesTable2),
 ArrayUtil.concat (domain, table2.domain), this.key, rows);
   return result;
}
else
{
  out.println("Please select an index map. Map is currently NO_MAP");
  Table result = new Table (name + count++, attribute,
domain, this.key, rows);
  return result;
}


    } // i_join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using a Hash Join algorithm.
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table h_join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".hashJoin (" + attributes1 + ", " + attributes2 + ", "
                 + table2.name + ")");

        String [] t_attrs = attributes1.split (" ");
        String [] u_attrs = attributes2.split (" ");

        List <Comparable []> rows = new ArrayList <> ();

        String [] attributesTable2 = new String [table2.attribute.length];

        // Code reused from equi-join. credits: @Ankit

        // Fetching the attribute positions of the keys from tables
        int [] colPosAttr1 = this.match(t_attrs);
        int [] colPosAttr2 = table2.match(u_attrs);

        // Fetching the domain types of the keys from tables
        Class [] domainsTable1 = this.extractDom(colPosAttr1, this.domain);
        Class [] domainsTable2 = this.extractDom(colPosAttr2, table2.domain);

        // Compute only if the domains are matching
        if (Arrays.equals(domainsTable1, domainsTable2)) {

            // Creating an instance of HashMap to generate
            HashMap <KeyType, List<Comparable[]>> h_map = new HashMap <>();

            // iterating through the table instance "primary-key-table"
            for (int i = 0; i < this.tuples.size(); i++) {

                Comparable [] keyVal = new Comparable [colPosAttr1.length];

                // iterating through primary key tuple(s)
                for (int pk = 0; pk < colPosAttr1.length; pk++)
                    keyVal[pk] = this.tuples.get(i)[colPosAttr1[pk]];

                // inserting into HashMap
                List <Comparable[]> valueToInsert = h_map.getOrDefault(new KeyType(keyVal), new ArrayList<>());
                valueToInsert.add(this.tuples.get(i));
                h_map.put(new KeyType(keyVal), valueToInsert);
            }

            // iterating throught the table instance "foreign-key-table"
            for (int i = 0; i < table2.tuples.size(); i++) {

                Comparable [] keyVal = new Comparable [colPosAttr2.length];

                // iterating through foreign key tuple(s)
                for (int fk = 0; fk < keyVal.length; fk++)
                    keyVal[fk] = table2.tuples.get(i)[colPosAttr2[fk]];

                List <Comparable[]> fetchedValue = h_map.get(new KeyType(keyVal));

                // if a collision occurs add tuples to joins
                if (fetchedValue != null)
                    for (Comparable[] fetchedData : fetchedValue)
                        rows.add(ArrayUtil.concat(fetchedData,table2.tuples.get(i)));
            }
        }

      // appends a 2 to any duplicate column names
            for (int i = 0; i < table2.attribute.length; i ++)
                attributesTable2[i] = table2.attribute[i];

            for (int attribute2 = 0; attribute2 < attributesTable2.length; attribute2++)
                for (int attribute1 = 0; attribute1 < this.attribute.length; attribute1++)
                    if (this.attribute[attribute1].equalsIgnoreCase(attributesTable2[attribute2]))
                        attributesTable2[attribute2] = attributesTable2[attribute2] + "2";

        return new Table (name + count++, ArrayUtil.concat (attribute, attributesTable2),
                                          ArrayUtil.concat (domain, table2.domain), this.findPrimaryKeyForJoin(attributes1, attributes2, table2), rows);
    } // h_join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
     public Table join (Table table2)
    {
       out.println ("RA> " + name + ".join (" + table2.name + ")");

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D
        // Implemented by Ankit Vaghela

        // Comment added by Ankit: Below two are the placeholders for matching attribute names
        Comparable[] attrTable1 = new Comparable [this.attribute.length];
        Comparable[] attrTable2_ = new Comparable [table2.attribute.length];

        // Comment added by Ankit: Below two are the placeholders for matching attribute positions
        Comparable[] attrPosTable1_ = new Comparable [this.attribute.length];
        Comparable[] attrPosTable2_ = new Comparable [table2.attribute.length];

        // Comment added by Ankit: Below code populates matching attributes columns arrays and matching attribute names arrays
        int count = 0;

        for(int i = 0 ; i < this.attribute.length ; i++) {
            for(int j = 0 ; j < table2.attribute.length ; j++) {
                if(this.attribute[i].equalsIgnoreCase(table2.attribute[j])) {

                    attrPosTable1_[count] = i;

                    attrPosTable2_[count] = j;

                    attrTable1[count] = this.attribute[i];
                    attrTable2_[count] = table2.attribute[j];

                    count++;
                    break;
                }
            }
        }

        Comparable[] attrPosTable1 = new Comparable [count];
        Comparable[] attrPosTable2 = new Comparable [count];
        Comparable[] attrTable2 = new Comparable [count];

        for(int i = 0 ; i < count ; i++ ) {
            attrPosTable1[i] = attrPosTable1_[i];
            attrPosTable2[i] = attrPosTable2_[i];
            attrTable2[i] = attrTable2_[i];
        }

        boolean isEmpty = false;
        int countForEmpty = 0;

        for (int i = 0; i < attrPosTable1.length ; i++) {
            if (attrPosTable1[i] == null) {
                countForEmpty++;
            }
        }

        if (countForEmpty == attrPosTable1.length) {
            isEmpty = true;
        }

        //  Comment added by Ankit: Below code compares tuples of two tables based on their matching attributes
        //  and populates rows array with joined tuples if match is found

        if (!isEmpty) {
        boolean matchesWhole = true;

        for (Comparable [] tup1 : this.tuples) {
            for (Comparable [] tup2 : table2.tuples) {
                int countForMatchJoin = 0;

                for (int i = 0; i < attrPosTable1.length; i++) {
                    if (tup1 [(int) attrPosTable1[i]].equals(tup2 [(int) attrPosTable2[i]])) {
                        countForMatchJoin++;
                    }
                }

                if (countForMatchJoin == attrPosTable1.length) {
                    rows.add(ArrayUtil.concat(tup1, tup2));
                }
            }
        }

       // Comment added by Ankit: Below code resizes the attributes and
       // domains of table 2 as in natural join duplicate columns are avoided in resultant table

       List<String> newTableAttributesList2 = new ArrayList<>();
       List<Class> newTableDomainsList2 = new ArrayList<>();

        boolean contains = true;
        for (int i = 0; i < table2.attribute.length; i++) {
            for (int j = 0; j < attrTable2.length; j++) {
                if (table2.attribute[i].equalsIgnoreCase((String) attrTable2[j])) {
                    contains = false;
                }
            }

            if (contains == true) {
                newTableAttributesList2.add(table2.attribute[i]);
                newTableDomainsList2.add(table2.domain[i]);
            }
            contains = true;
        }

        String[] newTableAttributes2 = newTableAttributesList2.toArray(new String[newTableAttributesList2.size()]);
        Class[] newTableDomains2 = newTableDomainsList2.toArray(new Class[newTableDomainsList2.size()]);

       // Comment added by Ankit: Below code modifies rows list as the tuples of rows
       // has to match with resized attributes and domains of table 2

       Comparable [] correctPositions = new Comparable [this.attribute.length+(table2.attribute.length-attrPosTable2.length)];
       int countCorrectPositions = 0;

        for (int i = 0; i < (this.attribute.length+table2.attribute.length) ; i ++) {
            if (i < this.attribute.length) {
                correctPositions[countCorrectPositions] = i;
                countCorrectPositions++;
            } else {
                int countForNotMatch = 0;
                for (int j = 0; j <  attrPosTable2.length; j++) {
                    if (!(i == (int) attrPosTable2[j] + this.attribute.length)) {
                        countForNotMatch++;
                    }
                }
                if(countForNotMatch == attrPosTable2.length) {
                    correctPositions[countCorrectPositions] = i;
                    countCorrectPositions++;
                }
            }
        }

        for (int t = 0; t < rows.size(); t++) {
            Comparable [] replacementTouple = new Comparable[correctPositions.length];
            for (int i = 0; i < correctPositions.length; i++) {
                replacementTouple[i] = rows.get(t)[(int)correctPositions[i]];
            }
            rows.remove(rows.get(t));

            rows.add(t, replacementTouple);
        }

       // Comment added by Ankit: Commenting below return statement to modify attributes and
       // domains of new table which will be returned as a result of natural join

       return new Table (name + count++, ArrayUtil.concat (attribute, newTableAttributes2),
               ArrayUtil.concat (domain, newTableDomains2), key, rows);
        } else {
            return new Table (name + count++, ArrayUtil.concat (attribute, table2.attribute),
                    ArrayUtil.concat (domain, table2.domain), key, rows);
        }
    } // join

    /************************************************************************************
     * Return the column position for the given attribute name.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (int i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;  // not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("'Star_Wars'", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            Comparable [] keyVal = new Comparable [key.length];
            int []        cols   = match (key);
            for (int j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
            if (mType != MapType.NO_MAP) index.put (new KeyType (keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        out.print ("| ");
        for (String a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        for (Comparable [] tup : tuples) {
            out.print ("| ");
            for (Comparable attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        if (mType != MapType.NO_MAP) {
            for (Map.Entry <KeyType, Comparable []> e : index.entrySet ()) {
                out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
            } // for
        } // if
        out.println ("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory.
     *
     * @param name  the name of the table to load
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
            ObjectOutputStream oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if
        for (int j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (int j = 0; j < column.length; j++) {
            boolean matched = false;
            for (int k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                    isPresent = true;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
                isPresent = false;
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        Comparable [] tup = new Comparable [column.length];
        int [] colPos = match (column);
        for (int j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in list) as well as the type of
     * each value to ensure it is from the right domain.
     *
     * @param t  the tuple as a list of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     */
    public boolean typeCheck (Comparable [] t)
    {
        // implemented by Johnathan
        if(this.attribute.length != t.length) return false;

        for(int i = 0; i < this.domain.length; i++) {
            if(this.domain[i] != t[i].getClass()) return false;
        }

        return true;
    } // typeCheck

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        Class [] classArray = new Class [className.length];

        for (int i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos the column positions to extract.
     * @param group  where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        Class [] obj = new Class [colPos.length];

        for (int j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom

    /**************************************************************************************
     * @author Ankit Vaghela
     * Method created by Ankit to help test minus and union to compare two instances of Table
     * Return true if this Table and t2 Table have same tuples
     * @param t2 the table you want to compare this Table with
     * @return true or false based on comparison
     *
     */
    public boolean areEqualTables(Table t2)
    {
        boolean equal = true;

        if ((this.attribute.length != t2.attribute.length) || (this.domain.length != t2.domain.length) ||
            (this.tuples.size()!=t2.tuples.size())) {
                equal = false;
        }
        if (equal == true) {
            for(int i = 0; i < tuples.size(); i++) {
            for(int j = 0; j < t2.tuples.size(); j ++) {
                    if(i == j && !Arrays.equals(tuples.get(i), t2.tuples.get(j))) {
                        equal = false;
                break;
                    }
                    if(equal == false) {
                        break;
                    }
            }
            if(equal == false) {
                    break;
            }
            }
        }
        return equal;
    } //areEqualTables

    /**
     * @author Ankit Vaghela
     * This method returns primary key for any type of join and is used to set primary key for new table created after join
     * @param attributes1: attributes for this table
     * @param attributes2: attributes for table2
     * @param table2
     * @return
     */
    public String [] findPrimaryKeyForJoin (String attributes1, String attributes2, Table table2) {

    	String [] attributes1Array = attributes1.split(" ");
    	List<String> attributes1List = new ArrayList<String>(Arrays.asList(attributes1Array));
    	List<String> table1KeyList = new ArrayList<>(Arrays.asList(this.key));

    	String [] attributes2Array = attributes2.split(" ");
    	List<String> attributes2List = new ArrayList<String>(Arrays.asList(attributes2Array));
    	List<String> table2KeyList = new ArrayList<>(Arrays.asList(table2.key));
    //	System.out.println("Primary key is :: ");
    	if(attributes1List.containsAll(table1KeyList) && !attributes2List.containsAll(table2KeyList)) {
    		for(int i = 0 ; i < table2.key.length ; i ++ ) {
    		//	System.out.println(table2.key[i]);
    		}
    		return table2.key;

    	}else if(attributes2List.containsAll(table2KeyList) && !attributes1List.containsAll(table1KeyList)) {
    		for(int i = 0 ; i < this.key.length ; i ++ ) {
    		//	System.out.println(this.key[i]);
    		}
    		return this.key;
    	}else {
    		table1KeyList.addAll(table2KeyList);

    		for(int i = 0 ; i < (table1KeyList).toArray().length ; i ++ ) {
    		//	System.out.println((table1KeyList).toArray()[i]);
    		}
    		return (table1KeyList).toArray(new String [table1KeyList.size()]);

    	}
    }
} // Table class