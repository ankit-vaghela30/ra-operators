/************************************************************************************
 * @file LinHashMap.java
 *
 * @author  John Miller
 */

import java.io.*;
import java.lang.reflect.Array;
import static java.lang.System.out;
import java.util.*;

/************************************************************************************
 * This class provides hash maps that use the Linear Hashing algorithm.
 * A hash table is created that is an array of buckets.
 */
public class LinHashMap <K, V>
       extends AbstractMap <K, V>
       implements Serializable, Cloneable, Map <K, V>
{
    /** The number of slots (for key-value pairs) per bucket.
     */
    private static final int SLOTS = 10;
    
    /** The threshold for Load factor
     */
    private static final double LOAD_FACTOR_THRESHOLD = 1.0;
    
    /** The counter for number of tuples inserted
     */
    private int noTuples = 0;

    /** The class for type K.
     */
    private final Class <K> classK;

    /** The class for type V.
     */
    private final Class <V> classV;

    /********************************************************************************
     * This inner class defines buckets that are stored in the hash table.
     */
    private class Bucket
    {
        int    nKeys;
        K []   key;
        V []   value;
        Bucket next;

        @SuppressWarnings("unchecked")
        Bucket (Bucket n)
        {
            nKeys = 0;
            key   = (K []) Array.newInstance (classK, SLOTS);
            value = (V []) Array.newInstance (classV, SLOTS);
            next  = n;
        } // constructor
    } // Bucket inner class

    /** The list of buckets making up the hash table.
     */
    private final List <Bucket> hTable;

    /** The modulus for low resolution hashing
     */
    private int mod1;

    /** The modulus for high resolution hashing
     */
    private int mod2;

    /** Counter for the number buckets accessed (for performance testing).
     */
    private int count = 0;

    /** The index of the next bucket to split.
     */
    private int split = 0;

    /********************************************************************************
     * Construct a hash table that uses Linear Hashing.
     * @param classK    the class for keys (K)
     * @param classV    the class for keys (V)
     * @param initSize  the initial number of home buckets (a power of 2, e.g., 4)
     */
    public LinHashMap (Class <K> _classK, Class <V> _classV)    // , int initSize)
    {
        classK = _classK;
        classV = _classV;
        hTable = new ArrayList <> ();
        mod1   = 8;                        // initSize;
        mod2   = 2 * mod1;
        
        //Added by Ankit beause we have to create initial 4 buckets of hTable
        for(int i = 0 ; i < mod1 ; i ++) {
        	Bucket b = new Bucket(null);
        	hTable.add(b);
        }
    } // constructor

    /********************************************************************************
     * Return a set containing all the entries as pairs of keys and values.
     * @return  the set view of the map
     */
    public Set <Map.Entry <K, V>> entrySet ()
    {
        Set <Map.Entry <K, V>> enSet = new HashSet <> ();

        //  T O   B E   I M P L E M E N T E D
        // Implemented by Ankit Vaghela
        
        for(int i = 0 ; i < hTable.size() ; i ++) {
        	//out.println("Bucket: "+i);
        	for(Bucket bucket = hTable.get(i) ; bucket != null;  bucket = bucket.next) {
        		
        		for(int j = 0 ; j < SLOTS ; j ++) {
        			if(bucket.key[j] != null) {
        			enSet.add(new AbstractMap.SimpleEntry <K, V> (bucket.key[j] , bucket.value[j]));
        			}
        		}
        	}
        }
        
        
        return enSet;
    } // entrySet

    /********************************************************************************
     * Given the key, look up the value in the hash table.
     * @param key  the key used for look up
     * @return  the value associated with the key
     */
    // modified Object to KeyType
    public V get (Object key)
    {
		
        int i = h (key);

        //  T O   B E   I M P L E M E N T E D
        if(i < split) i = h2(key);
        
        for(Bucket bucket = hTable.get(i) ; bucket != null;  bucket = bucket.next) {
        	
        	count++;
        	
        	for(int j = 0 ; j < SLOTS ; j++) {
        		//out.println("Outside if of get");
        		  //if(bucket.key[j] != null && new KeyType ((Comparable) key).equals(new KeyType ((Comparable) bucket.key[j]))) {
					if(bucket.key[j] != null && ((KeyType) key).equals((KeyType) bucket.key[j])) {
        			//out.println("Inside if of get");
        			return bucket.value[j];
        		}
        	}
        }
 
        return null;
    } // get

    /********************************************************************************
     * Put the key-value pair in the hash table.
     * @param key    the key to insert
     * @param value  the value to insert
     * @return  null (not the previous value)
     */
    public V put (K key, V value)
    {
    	
        int i = h (key);
        //out.println ("LinearHashMap.put: key = " + key + ", h() = " + i + ", value = " + value);
        
        
        //  T O   B E   I M P L E M E N T E D
        // Implemented by Ankit Vaghela
        
        noTuples++;
        
        if(i<split) i = h2(key);
        
        //Check for loading factor and split of it is greater than LOAD_FACTOR_THRESHOLD
        
        if((double)(noTuples + 1)/size() >= LOAD_FACTOR_THRESHOLD) {
        	split (key, value);
        }else {
        	//We insert the key if the event is not eligible for split
        	insertKey (key, value);
        }
		
        return null;
    } // put
    /**
     * @author ankit Vaghela
     * This method will split the home bucket and and also adjust lower and higher resolution functions
     * @param key
     * @param value
     */
    public void split (K key, V value)
    {
    	
    	int originalSplit = split;
    	
    	if(split+1 == mod1) {
    		
    		mod1 = mod1 * 2;
    		mod2 = 2 * mod1;
    		split = 0;
    		
    	}
    	else {
    	split++;
    	}
    	
    	Bucket b = new Bucket(null);
    	hTable.add(b);
    	//Getting all the values of split bucket and the one we want to add in a map
    	Map<K,V> keysSplitBucket = new HashMap<K, V>();
    	keysSplitBucket.put(key, value);
    	for(Bucket bucket = hTable.get(originalSplit) ; bucket != null;  bucket = bucket.next) {
    		for(int i = 0 ; i < SLOTS ; i++) {
    			if(bucket.key[i] != null) {
    			keysSplitBucket.put(bucket.key[i], bucket.value[i]);
    			}
    		}
    	}
    	
    	//empty the bucket
    	hTable.remove(originalSplit);
    	hTable.add(originalSplit, new Bucket(null));
    	
    	for(K  k : keysSplitBucket.keySet()) {
    		insertKey(k, keysSplitBucket.get(k));
    	}
    }
    /**
     * @author ankit Vaghela
     * This method inserts the key in the hTable and overloading is also handeled here
     * @param key
     * @param value
     */
    public void insertKey (K key, V value)
    {
    	
    	int i = h(key);
    	if (i < split) {
    		i = h2(key);
    	}
    	
    	for(Bucket bucket = hTable.get(i) ; bucket != null;  bucket = bucket.next) {
    		
    		int count =0;
    		
    		if(bucket.nKeys < SLOTS){
    			
    			bucket.key[bucket.nKeys] = key;
    			bucket.value[bucket.nKeys] = value;
    			bucket.nKeys++;
    			break;
    			
    		}
    		//Below is the overloading event
    		else if (bucket.nKeys == SLOTS && bucket.next == null) {
    			
    			bucket.next = new Bucket(null);
    			bucket.next.key[bucket.next.nKeys] = key;
    			bucket.next.value[bucket.next.nKeys] = value;
    			bucket.next.nKeys++;
    			
    			break;
    		}
    		
    		
    	}
    }

    /********************************************************************************
     * Return the size (SLOTS * number of home buckets) of the hash table. 
     * @return  the size of the hash table
     */
    public int size ()
    {
        return SLOTS * (mod1 + split);
    } // size

    /********************************************************************************
     * Print the hash table.
     */
    public void print ()
    {
        out.println ("Hash Table (Linear Hashing)");
        out.println ("-------------------------------------------");

        //  T O   B E   I M P L E M E N T E D
        // Implemented by Ankit Vaghela
        for(int i = 0 ; i < hTable.size() ; i ++) {
        	out.println("Bucket: "+i);
        	for(Bucket bucket = hTable.get(i) ; bucket != null;  bucket = bucket.next)  {
        		out.println();
        		
        		for(int j = 0 ; j < SLOTS ; j ++) {
        			if(bucket.key[j] != null) {
        			out.println("[key -->"+ bucket.key[j]+ "] [Value --> " + bucket.value[j] +"]");
        		}
        		}
        	}
        	out.println();
        }
        
        out.println ("-------------------------------------------");
    } // print

    /********************************************************************************
     * Hash the key using the low resolution hash function.
     * @param key  the key to hash
     * @return  the location of the bucket chain containing the key-value pair
     */
    private int h (Object key)
    {
    	//Absolute function added by Ankit to handle non positive hashcodes
    	if(key.hashCode() < 0) {
    		return Math.abs(key.hashCode()) % mod1;
			//return (key.hashCode() * -1) % mod1;
    	}
        return key.hashCode () % mod1;
    } // h

    /********************************************************************************
     * Hash the key using the high resolution hash function.
     * @param key  the key to hash
     * @return  the location of the bucket chain containing the key-value pair
     */
    private int h2 (Object key)
    {
    	//Absolute function added by Ankit to handle non positive hashcodes
    	if(key.hashCode() < 0) {
    		return Math.abs(key.hashCode()) % mod2;
			//return (key.hashCode() * -1) % mod2;
    	}
        return key.hashCode () % mod2;
    } // h2

    /********************************************************************************
     * The main method used for testing.
     * @param  the command-line arguments (args [0] gives number of keys to insert)
     */
    public static void main (String [] args)
    {
	{

	    int totalKeys    = 3000;
	    boolean RANDOMLY = false;

	    LinHashMap <Integer, Integer> ht = new LinHashMap <> (Integer.class, Integer.class);
	    if (args.length == 1) totalKeys = Integer.valueOf (args [0]);

	    if (RANDOMLY) {
		Random rng = new Random ();
		for (int i = 1; i <= totalKeys; i += 2) ht.put (rng.nextInt (2 * totalKeys), i * i);
	    } else {
		for (int i = 1; i <= totalKeys; i += 1) ht.put (i, i * i);
	    } // if

	    ht.print ();
	    out.println("Testing 1: Adding a value with no split occuring.");
	    out.println("Adding value 0 to key 0.");
	    ht.put(0, 0);
	    ht.print();
        
	    out.println("\nTest 2: Adding a new key and value to a full bucket.");
	    out.println("Adding key 4 with value 13\n");
	    ht.put(4, 12);
	    ht.print();
	    out.print(ht.entrySet());
       
	    out.println("\n\nGrabbing value of key 3... Expecting 9...");
	    out.print("Actual Result: "+ ht.get(3));
	    
	    out.println("\n\nGrabbing value of key 10... Expecting null...");
	    out.print("Actual Result: "+ ht.get(10));
	    
	    ht.put(5, 36);
	    ht.put(6, 34);
	    ht.put(7, 14);
	    
	    ht.print();
	    out.println("We can tell that the keys were input correctly by looking at the\n"
			+ "number associated with the key. Key 0 and key 4 end in the bits 00 while key 3\n"
			+ "key 7 end in the bits 11.");
	    out.println("Adding key 11 with value 100. Expecting it to be added to bucket 3.");
	    ht.put(11, 100);
	    ht.print();
	    out.println("Adding key 15 then 19 to overflow bucket 3.");
	    ht.put(15, 50);
	    ht.put(19, 60);
	    ht.print();
	    out.println("A new bucket was chained onto bucket 3. Attempting to overflow bucket 1.");
	    ht.put(9, 14);
	    ht.put(13, 24);
	    ht.put(17, 167);
	    ht.print();
	    
	    out.println("Forcing a new bucket to be made.");
	    ht.put(19, 1);
	    ht.put(12, 9);
	    out.println("Bucket 4 was added to our map. The bit value of this is 100. We can see that\n"
			+ "as both 4 and 12 end in 100.");
	    ht.put(9, 25);
	    ht.put(2, 49);
	    ht.put(18, 81);
	    
	    ht.print();
	    out.println("This table has been filled correctly.\n\n");
	    out.println(ht.entrySet());
	    
	    out.println ("Average number of buckets accessed = " + ht.count / (double) totalKeys);
	    out.println("No of keys: "+ht.noTuples +"  Real count: "+ht.entrySet().size()); 
	    
	    out.println("\n\n^^^^^^ TESTING BEGINS AT THE START OF THE CONSOLE OUTPUT. ^^^^^^");

	}
    } // main

} // LinHashMap class