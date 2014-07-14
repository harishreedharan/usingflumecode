/*
 *  Algorithm:
 *  1) For each test case find immediate neighbors. - findFriends
 *  - argument - file path
 *  - additional data structures 'seen' and 'visited' of type Set<String> 
 *  	- seen - add words to seen set if they have been seen once
 *  	- visited - add words to visited set if all the friends of a word are visited 	
 *  - take additional List<String> data structures 'testcases' and 'dictionary' to keep track of the
 *    testcases and dictionary words (list of given words in input file after END OF INPUT terminator
 *  - additional data structure 'stack' to add the words to the stack for Depth First Search like traversal
 *  - mySocialNetwork - Map<String, Set<String>>
 * 		  - keeps track of a every testcases social network
 *  - friendsCount - Map<String, Integer>
 *  	  - count friends of each of the testcase word
 *  - FRIENDS_DISTANCE = 1 (if 2 words are friends)    
 *  - calculate edit distance between 2 words using Levenshtien algorithm  
 *  2) Then find the complete social network for each of the test case
 *  - argument - testcase, network, dictionary
 *  
 *  
 */
package usingflume.ch05;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

public class Main {

  final int FRIENDS_DISTANCE = 1;

  long lavenshTienTime = 0;

  // seen keeps track of the words that are seen once
  Set<String> seen = new HashSet<String>();

  // visited keeps track of the words whose friend words are also added to the friend list
  Set<String> visited = new HashSet<String>();

  //map to track every words social network
  Map<String, Set<String>> SocialNetwork = new HashMap<String, Set<String>>();

  public static void main (String[] args){

    Main distance = new Main();

    long startTime = System.currentTimeMillis();

    List<Integer> list = distance.findFriends(args[0]);
    long endTime = System.currentTimeMillis();


    Iterator<Integer> iter = list.iterator();

    while(iter.hasNext()){
      System.out.println(iter.next());
    }
    System.out.println("Total time: "+(endTime - startTime));

  }

  /*
   * find the friends for each of the testcases
   * @ return : List with number of friends for each testcase
   */
  public List<Integer> findFriends(String file){

    BufferedReader reader = null;

    //a map keep track of a every testcase's social network
    Map<String,Set<String>> testCaseSocialNetwork = new HashMap<String, Set<String>>();

    //a set which tracks the social network for the test case returned from getMySocialNetwork() method
    Set<String> myNetwork = new HashSet<String>();

    //a map to count number of friends for a testcase
    Map<String,Integer> countMySocialNetwork = new HashMap<String, Integer>();

    //list to return the final result
    List<Integer> returnList = new LinkedList<Integer>();


    try{
      List<String> testCases = new LinkedList<String>();
      List<String> dictionary = new LinkedList<String>();

      String currentLine;

      reader = new BufferedReader(new FileReader(file));

      while ((currentLine = reader.readLine()) != null) {

        if(currentLine.equalsIgnoreCase("END OF INPUT"))
          break;
        testCases.add(currentLine.trim());
      }


      while ((currentLine = reader.readLine()) != null) {
        dictionary.add(currentLine.trim());
      }

      for(String testCase : testCases){
        testCaseSocialNetwork.put(testCase, new HashSet<String>());
      }

			/*
			 * for each testcase:
			 * 1) find all the friends
			 * 2) calculate the number of friends
			 */
      for(Entry<String, Set<String>> entry : testCaseSocialNetwork.entrySet()){
        //				System.out.println(entry.getKey());
        getMySocialNetwork(entry.getKey(), entry.getValue(), dictionary);
        countMySocialNetwork.put(entry.getKey(),  entry.getValue().size());
      }

      for(String testCase : testCases){
        //				System.out.println(testCase);
        returnList.add(countMySocialNetwork.get(testCase));
      }

      System.out.println("Levenshtien time: "+lavenshTienTime);
    }

    catch (IOException e) {
      e.printStackTrace();
    }
    finally
    {
      try
      {
        if (reader != null)
          reader.close();
      }
      catch (IOException ex)
      {
        ex.printStackTrace();
      }
    }

    return returnList;
  }

  /*
   * This method takes the testcase - 'key' , friends - 'network' and given list of words - dictionary'
   * as input and returns the final set of friends as output.
   */
  public Set<String> getMySocialNetwork(String key, Set<String> network, List<String> dictionary){



    Stack<String> stack = new Stack<String>();

    stack.push(key);

    while(!stack.isEmpty()){

      String popElem = stack.peek();
      //			System.out.println("popelem: "+popElem);

      //pop the popElem if it has been both seen and visited
      if(visited.contains(popElem) && seen.contains(popElem)){
        //				System.out.println("seenandvisited: "+popElem);
        continue;
      }

      //if the word has not been seen add it to seen list
      if(!seen.contains(popElem)){
        seen.add(popElem);
        //				System.out.println("seenelement: "+popElem);

      }

      //else if the word has been seen but not visited then add it to visited list
      else if(seen.contains(popElem) && !visited.contains(popElem)){
        //				System.out.println("seenbutnotvisisted: "+popElem);
        visited.add(popElem);
        stack.pop();

      }

			/*
			 * for every word which has not been seen or has been seen but not visited: 
			 * 1) iterate through the dictionary
			 * 		a) calculate the edit distance between the popElem and the dictionary word
			 * 		b) if the distance is FREINDS_DISTANCE = 1
			 * 			b.1) if the element is not already on the stack
			 * 				b.1.1) add it to the stack
			 * 				b.1.2) add the dictionary element to the seen list
			 * 				b.1.3) add the word to the 'key's social network  
			 */

      for(int j = 0 ; j < dictionary.size() ; j++){
        Set<String> myFriends = new HashSet<String>();
        int len = Math.abs(popElem.length() - dictionary.get(j).length());
        if(len > 1){
          continue;
        }
        if(popElem.equalsIgnoreCase(dictionary.get(j))){
//					System.out.println(dictionary.get(j));
          continue;
        }

        if(SocialNetwork.containsKey(dictionary.get(j)) && (SocialNetwork.get(dictionary.get(j))!=null)){
          network.addAll(SocialNetwork.get(dictionary.get(j)));
          continue;
        }

        long startTimeLavenshtien = System.currentTimeMillis();
        int distance = editDistance(popElem, dictionary.get(j));
//				System.out.println(distance);
        long endTimeLavenshtien = System.currentTimeMillis();
        //System.out.println(endTimeLavenshtien - startTimeLavenshtien);
        lavenshTienTime += (endTimeLavenshtien - startTimeLavenshtien);

        if(distance == FRIENDS_DISTANCE)
        {
          if(!stack.contains(dictionary.get(j))){
            //						System.out.println("myfriends: "+dictionary.get(j));
            stack.push(dictionary.get(j));
            seen.add(dictionary.get(j));
            myFriends.add(dictionary.get(j));
            network.add(dictionary.get(j));
          }
        }
        SocialNetwork.put(popElem, myFriends);
      }
    }

    return network;
  }
  /*
   * This method takes 2 string and finds the minimum edit distance between the 2 words
   * using Levenshtien algorithm
   */
  public int editDistance(String X, String Y){

    int lenX = X.length() + 1;

    int lenY = Y.length() + 1;

    char[] x= X.toCharArray();
    char[] y = Y.toCharArray();

    final int editCost = 1;

    int cost = 0;
    int left, bottom, corner;

		/*
		 * 2D array to keep track of the minimum distance
		 */
    int[][] arr = new int[lenX][lenY];

		/*
		 * Initialize the array 
		 */
    for(int i = 0; i<lenX; i++){
      for (int j = 0 ; j<lenY ; j++){
        arr[i][j] = -1;
      }
    }

		/*
		 * Base cases where lenX or lenY is 0
		 */
    for (int i = 0; i< lenX ;i ++)
      arr[i][0] = i;

    for (int j = 0; j< lenY ;j ++)
      arr[0][j] = j;

		/*
		 * Cache the values of left, bottom and corner cells so as to avoid any repetitions. 
		 * Add editCost for each iteration to left, bottom and corner values.
		 * To the array add only Minimum value of left, bottom and right values
		 */
    for (int i = 1; i< lenX ;i ++){
      for (int j = 1; j< lenY ;j ++){

        //for left cell i.e. arr[i][j-1] - add
        left = arr[i][j-1];
        left = left + editCost;

        //for bottom cell i.e. arr[i-1][j] - delete
        bottom = arr[i-1][j];
        bottom = bottom + editCost;

        //for the corner cell i.e. arr[i-1][j-1] - replace
        corner = arr[i-1][j-1];
        corner = corner + (x[i-1] != y[j-1] ? editCost : 0);

        //add the minimum value of left, bottom or corner to the array
        arr[i][j] = Minimum(left, bottom, corner);
      }
    }

    //final minimum cost to convert String X to String Y
    cost = arr[lenX-1][lenY-1];
    return cost;

  }

  //Helper function to calculate the minimum value between a, b and c
  int Minimum(int a , int b , int c){
    return min(min(a,b), c);
  }

  int min(int a , int b){
    return (a < b ? a : b);
  }
}
