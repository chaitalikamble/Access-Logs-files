import java.io.*;
import java.util.*;
/*
 * This class provides an implementation to get top 15 occurring ip addresses by using
 * Mapper Reducer concept.
 * @Authors		Chaitali Kamble
 * 			
 * @Date		12/06/2016
 * @Version		1.0
 */
public class AccessLogs extends MapReduce {
	
	public static void main(String args[]) throws IOException {
		/*
		 * Execution starts from here.
		 */

		if(args.length == 0){
			System.out.println("Please provide file names as arguments");
		}else{
			/*
			 * Iterate through the file names and concatenate them with ";" symbol
			 */
			AccessLogs mrInstance = new AccessLogs();
			for(String filename: args){
				mrInstance.execute(filename);
			}
			mrInstance.postprocessing();
		}
		/*
		// This block of code is useful for taking inputs from console
		System.out.println("please enter file name");
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		String line;
		while ((line = stdin.readLine()) != null && line.length() != 0) {
			mrInstance.execute(line);

		}
		mrInstance.postprocessing();
		*/
	}

	public void mapper(String file) {
		/*
		 * This method is called through execute method.
		 * Mapper reads files and emits ip address with corresponding
		 * account by reading files line by line.
		 * @parameters	String	String of file name
		 * @return		None
		 */
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			String line = null;

			// itearte over each line in a file
			while ((line = br.readLine()) != null) {
				String IP = "";
				String Account = "";
				int start;

				// get ip address
				IP = line.split(" ")[0];

				// get "~" symbol index from line
				start = line.indexOf("~");

				// excutes only when "~" is found
				if (start >= 0) {

					// iterate over remaining line from the "~" sysmbol
					for (int j = start + 1; j < line.length(); j++) {

						// if space occurs after "~" symbol
						if (line.charAt(start + 1) == ' ') {
							break;
						}

						// recognize " ' " quote after "~" symbol
						int quoteChar = line.indexOf("'");

						// stop when any of below character appears after "~"
						// symbol
						if (line.charAt(j) == '/' || line.charAt(j) == ' ' || 
								line.charAt(j) == '"' || j == quoteChar) {

							// emit IP adress
							emit(IP, Account);
							break;

						}

						// concatenate characters to get account name
						Account = Account + line.charAt(j);
					}

				}

			}
		}

		catch (IOException e) {
		}
		try {
			br.close();
		} catch (IOException e) {
		}
	}

	public void reducer(String key, LinkedList<String> values) {
		/*
		 * for each IP adress, reducer gets list of accounts and emit each account
		 * with corresponding ip address.
		 * @parameters	String	key(ip adress) LinkedList values(accounts)
		 * @return		None
		 */
		for(int i=0;i< values.size();i++){
			emit(key, values.get(i));
		}
	}
	
	private void postprocessing() {
		/*
		 * This method gets the tree map produced by Mapper and reducer.
		 * For each ip address, it removes duplicate accounts and then produce only top
		 * 15 ip addresses in required format.
		 * @parameters	None
		 * @return		None
		 */
		
		// tree map to store the final results 
		TreeMap<String, LinkedList<String>> treeMap = getResults();
		
		// key set to get all accessed ips
		Set<String> keySetIp = treeMap.keySet();
		Iterator<String> i = keySetIp.iterator();
		
		// Array of objects that stores ip, accounts and length of accounts list.
		List[] data = new List[keySetIp.size()];
		int data_index = 0;
		
		// iterate over each ip address and get unique accounts 
		while(i.hasNext()){
			String ip = (String) i.next();
			LinkedList<String> accounts = treeMap.get(ip);
			Set<String> uniqueAccounts = new HashSet<String>();
			uniqueAccounts.addAll(accounts);
			List node = new List(ip, uniqueAccounts, uniqueAccounts.size());
			data[data_index] = node;
			data_index ++ ;
		}
		
		// sort the array of objects on the basis of length of accounts list
		Arrays.sort(data, new Comparator<List>() {
			public int compare(List node1, List node2) {
				return Integer.compare(node1.length, node2.length);
			};

		});

		// Print top 15 occuring IPs
		String line;
		int count = 1;
		for (int c = data.length - 1; c >= 0; c--) {
			if (count == 16 ) {
				break;
			}
			line = "#" + count + " " + "occuring address:         " + data[c].ip;
			System.out.println(line);
			line = "       ";
			int ac = 0;
			Iterator<String> iter = data[c].accounts.iterator();
			while(iter.hasNext()){
				if(ac == 0){
					line = line + iter.next();
				}
				else{
					line = line + ", " + iter.next();
				}
				ac = ac + 1;
			}

			System.out.println(line);
			count++;
		}

	}

	public class List implements Comparable<List> {
		String ip;
		HashSet<String> accounts;
		int length;

		public List(String ip, Set<String> accounts, int length) {
			this.ip = ip;
			this.accounts = (HashSet<String>) accounts;
			this.length = accounts.size();
		}

		public String toString() {
			return (ip + " " + accounts + " " + length + " ");
		}

		public int compareTo(List o) {
			return toString().compareTo(o.toString());
		}
	}

}
