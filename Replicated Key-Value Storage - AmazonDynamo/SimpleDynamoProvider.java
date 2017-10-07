package edu.buffalo.cse.cse486586.simpledynamo;

//package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.util.Comparator;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	public ConcurrentHashMap hmap;
	public String myPort;
	public String myPort_hash;

	static  String[] remote_ports = {"11108","11112","11116","11120","11124"};

	static  String pred;
	static  String suc;
	static  String pred_hash;
	static  String suc_hash;
	static  int avd_count =0;

	static  String global_min = null;
	static  String global_max = null;
	static Boolean single_avd;



	ArrayList<String> remote_ports_hash = new ArrayList<String>();
	ArrayList<String> remote_ports_list = new ArrayList<String>();
//	Map<String, HashMap<String,String>> all_resp_nodes_key_value= new HashMap<String, HashMap<String,String>>();

	ConcurrentHashMap<String, HashMap<String,String>> all_resp_nodes_key_value= new ConcurrentHashMap<String, HashMap<String, String>>();


	PriorityBlockingQueue<HashMap> pbq = new PriorityBlockingQueue<>(25, idComparator);
	public static Comparator<HashMap> idComparator = new Comparator<HashMap>(){
		@Override
		public int compare(HashMap h1, HashMap h2) {
			int i = Integer.compare(h1.size(), h2.size());
			if (i != 0) return i;
			return i;
		}
	};


	PriorityBlockingQueue<Integer> pbq2 = new PriorityBlockingQueue<>(25, idComparator2);
	public static Comparator<Integer> idComparator2 = new Comparator<Integer>(){
		@Override
		public int compare(Integer h1, Integer h2) {
			int i = (h1+h2);
			if (i != 0) return i;
			return i;
		}
	};


	public SimpleDynamoProvider() throws NoSuchAlgorithmException{}

	public class each_avd_info{
		String mine;
		String mine_hash;
		String pred;
		String pred_hash;
		String suc;
		String suc_hash;

		public each_avd_info() throws NoSuchAlgorithmException {}

		public  void set_hashes() throws NoSuchAlgorithmException {
			this.mine_hash = genHash(mine);
			this.pred_hash = genHash(pred);
			this.suc_hash = genHash(suc);
		}
		public  void display_all(){
			System.out.println(this.mine);
			System.out.println(this.mine_hash);
			System.out.println(this.pred);
			System.out.println(this.pred_hash);
			System.out.println(this.suc);
			System.out.println(this.suc_hash);
		}
	}

	HashMap<String,each_avd_info> all_avds = new HashMap<String, each_avd_info>();
	each_avd_info new_avd = new each_avd_info();


	static HashMap<String,Boolean> visited_info = new HashMap<String, Boolean>();

	public String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

//	@Override
//	public int delete(Uri uri, String selection, String[] selectionArgs) {
//		// TODO Auto-generated method stub
//
//		Log.d("Delete", "Delete ->"+selection);
//		try {
//			if (selection.endsWith("+")) {
//				Log.d("Delete", "Deletion "+selection+" ends with + ");
//
//				selection = selection.substring(0,selection.length()-1);
//				if (selection.equals("@")) {
//					Log.d("Delete", "Deletion @");
//
//					hmap.clear();
//					return 0;
//				}
//				if (selection.equals("*")) {
//					Log.d("Delete", "Deletion @");
//
//					hmap.clear();
//					return 0;
//				}else {
//					Log.d("Delete", "Deletion @");
//					hmap.remove(selection);
//					return 0;
//				}
//
//
//			}else {
//				Log.d("Delete", "Deletion "+selection+" doesnt end with +");
//
//				if (selection.equals("@")) {
//					Log.d("Delete", "Deletion @");
//
//					hmap.clear();
//					return 0;
//				}
//				if (selection.equals("*")) {
//					Log.d("Delete", "Deletion *");
//
//					hmap.clear();
//					String remotePort = Integer.toString(Integer.parseInt(suc) * 2);
//					Log.d("Deletion:", "Delete * " + suc);
//					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
//					socket.setSoTimeout(300);
//
//
//					String msgToSend = "Delete" + " " + myPort + " " + selection;
//
//					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//					out.println(msgToSend);
//					String message_from_server = null;
//					BufferedReader in_from_server = null;
//					in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//					message_from_server = in_from_server.readLine();
//					Log.d("Delete", message_from_server);
//
//					socket.close();
//
//					Log.d("Delete", "waiting for take...");
//
//					Integer i = pbq2.take();
//
//					Log.d("Delete", "take released");
//					return 0;
//
//				}
//				else {
//
//					if(hmap.containsKey(selection)){
//						Log.d("Delete", "Deletion key found locally");
//
//						hmap.remove(selection);
//						return 0;
//					}
//					else{
//						Log.d("Delete", "Deletion key need to be searched");
//
//
//						String remotePort = Integer.toString(Integer.parseInt(suc) * 2);
//						Log.d("Delete:", "Delete indidvidual query forwarded to : " + suc);
//						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
//						socket.setSoTimeout(300);
//
//
//						String msgToSend = "Delete" + " " + myPort + " " + selection;
//
//						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//						out.println(msgToSend);
//						String message_from_server = null;
//						BufferedReader in_from_server = null;
//						in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//						message_from_server = in_from_server.readLine();
//						Log.d("Delete", message_from_server);
//
//						socket.close();
//						Log.d("Delete", "waiting for take...");
//
//						Integer i = pbq2.take();
//
//						Log.d("Delete", "take released");
//
//
//						return 0;
//
//					}
//
//				}
//			}
//
//		}catch(Exception e){
//			e.printStackTrace();
//		}
//		return 0;
//	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		Log.d("Delete", "Delete ->"+selection);
		try {
			if (selection.endsWith("+")) {
				Log.d("Delete", "Deletion "+selection+" ends with + ");

				selection = selection.substring(0,selection.length()-1);
				if (selection.equals("@")) {
					Log.d("Delete", "Deletion @");
					// TODO: 5/9/17 need to clear replica as well??
					hmap.clear();
					all_resp_nodes_key_value.clear();
					return 0;
				}
				if (selection.equals("*")) {
					Log.d("Delete", "Deletion *");
					all_resp_nodes_key_value.clear();
					hmap.clear();
					return 0;
				}else {
					Log.d("Delete", "Deletion individual");
					hmap.remove(selection);
					remove_from_backup(selection);//doesnt contain +
					return 0;
				}


			}else {
				Log.d("Delete", "Deletion "+selection+" doesnt end with +");

				if (selection.equals("@")) {
					Log.d("Delete", "Deletion @");

					hmap.clear();
					return 0;
				}
				if (selection.equals("*")) {
					Log.d("Delete", "Deletion *");
					Socket socket = null;
					for (String indi_avd : remote_ports_list) {
						try {

							String remotePort = indi_avd;// TODO: 5/9/17 not supposed to send to local as well?
							Log.d("Delete:", "Deleting from  : " + remotePort);
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
							socket.setSoTimeout(300);

							String msgToSend = "Delete" + " " + myPort + " " + selection;

							PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
							out.println(msgToSend);
							String message_from_server = null;
							BufferedReader in_from_server = null;
							in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							message_from_server = in_from_server.readLine();
							Log.d("Delete", message_from_server);
							socket.close();

						}catch (SocketTimeoutException e){
							socket.close();
							e.printStackTrace();
							Log.e(TAG, "delete: Socket time out!!" );
							continue;
						}catch (Exception e){
							socket.close();
							e.printStackTrace();
							Log.e(TAG, "delete: Some other exception");
							continue;
						}
					}
					return 0;

				}
				else {
					Log.d("Delete", "Deleting individual from all");

					for (String indi_avd : remote_ports_list) {
						try {

							String remotePort = indi_avd;// TODO: 5/9/17 not supposed to send to local as well?
							Log.d("Delete:", "Deleting from  : " + remotePort);
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
							socket.setSoTimeout(300);

							String msgToSend = "Delete" + " " + myPort + " " + selection;

							PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
							out.println(msgToSend);
							String message_from_server = null;
							BufferedReader in_from_server = null;
							in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							message_from_server = in_from_server.readLine();
							Log.d("Delete", message_from_server);

							socket.close();


						}catch(Exception e){
							e.printStackTrace();
							continue;
						}
					}

				}
			}

		}catch(Exception e){
			e.printStackTrace();
		}
		return 0;
	}

	void remove_from_backup(String key){
		Set all_nodes = all_resp_nodes_key_value.keySet();
		for (Object each_node:all_nodes){
			if(all_resp_nodes_key_value.get(each_node).containsKey(key)){
				all_resp_nodes_key_value.get(each_node).remove(key);
			}
		}
		return;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	Uri forward_replicas(int index_to,String key,String value,Uri uri,int till) throws IOException {

		int sz = remote_ports_list.size();
//		Log.d("forward_replicas","from : "+ index_to +" "+remote_ports_list.get(index_to%sz)+" "+"to : "+index_to+"+"+till
//		+" "+remote_ports_list.get((index_to+till)%sz));

		String all_resp = null;
//		HashMap<String,String> temp = new HashMap<>();

		if(till==2){//which means...
//			all_resp = remote_ports_list.get((index_to-1)%sz)+" "+remote_ports_list.get((index_to)%sz)+" "+remote_ports_list.get((index_to)%sz);
			all_resp = remote_ports_list.get((index_to-1)%sz)+" "+remote_ports_list.get((index_to)%sz)+" "+remote_ports_list.get((index_to+1)%sz);
//			temp.put(key,value);
//			all_resp_nodes_key_value.put(remote_ports_list.get((index_to-1)%sz),temp);

			update_replication_info(key,value,all_resp);

		}else{
			all_resp = remote_ports_list.get(index_to%sz)+" "+remote_ports_list.get((index_to+1)%sz)+" "+remote_ports_list.get((index_to+2)%sz);
		}

		//updating reponsible hashmap on local as well


		for (int i=index_to;i<index_to+till;i++){// TODO: 5/2/17 <= or <?
			// TODO: 5/8/17 sending to self port as well??!
			try {
				int j = i; //so that mod operation ca be done.
				if (i >= remote_ports_list.size()) {
					j = i % remote_ports_list.size();
				}
//				Log.d("forward_replicas", "My port - " + myPort);

//				Log.d("forward_replicas", "forwarding to - " + "i:" + i + " - " + " j:" + j + " - " + remote_ports_list.get(j));

				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(remote_ports_list.get(j)));
				socket.setSoTimeout(300);

				String msgToSend = "Forwarded" + " " + key + "+" + " " + value + " " + all_resp;// TODO: 5/7/17  "+" ????

				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				out.println(msgToSend);
//				Log.d("forward_replicas", "replicate sent");

				String message_from_server = null;
				BufferedReader in_from_server = null;
				in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				message_from_server = in_from_server.readLine();
				Log.d("forward_replicas", "Reply to the sent replica: " + message_from_server);
			}catch(SocketTimeoutException e){
				Log.e("Forward Replicas","forwarding to a dead node - continuing");
				e.printStackTrace();
				continue;

			}
		}
		return uri;
	}

	void update_replication_info(String key_part, String value_part, String resp_avd_string){
		//************processing the responsible avds part
		//The hasmap that stores the info is made global.
		HashMap temp = new HashMap<String,String>();
		String[] resp_avds = resp_avd_string.split(" ");

		if(hmap.containsKey(key_part)){
			int ver = Integer.parseInt(hmap.get(key_part).toString().split("@")[1]);
			ver = ver+1;
			temp.put(key_part,value_part+"@"+ver);
		}else{
			temp.put(key_part,value_part+"@"+1);
		}

		for(String each_avd:resp_avds) {
			if(all_resp_nodes_key_value.containsKey(each_avd)){
//					all_resp_nodes_key_value.get(each_avd).put(key_part,value_part);
				all_resp_nodes_key_value.get(each_avd).put(key_part,temp.get(key_part).toString());
			}else{
				all_resp_nodes_key_value.put(each_avd, temp);
			}
		}
		//*************************sufficient i guess.
		return;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = values.getAsString("key");
		String value = values.getAsString("value");

		String remotePort = null;
		Socket socket = null;
		BufferedReader reply_from_suc = null;

		String min = getmin();
		String max = getmax();

		Log.d("Insert", "******insert begin - processing request received in insert: "+key + " " + value);

		if(key.endsWith("+")){
			Log.d("Insert", "this is a forwarded insert. no need to process. Just store. No forwarding as well.");
			check_and_insert_hmap(key.substring(0,key.length()-1),value);
//			Log.d("Insert", "After inserting. Hashmap state."+hmap.toString());
			return uri;
		}

//		Log.d("Insert","mine: "+myPort+" global min: "+global_min);
//		Log.d("Insert","Single avd?:"+single_avd);

		try {
			String key_hashed = genHash(key);
			String value_hashed = genHash(value);

			int count =0 ;
//			Log.d("Insert", "processing request received in insert - key hashed - "+key_hashed);

			int i = 0 ;
			while (count < remote_ports_hash.size()*2){
				String indi_port = remote_ports_hash.get(i%remote_ports_hash.size());

				//******************uncommet later oon
//				Log.d("Insert", "key hashed - "+key_hashed+" - port hashed - "+indi_port);
//				Log.d("Insert","current port - "+ remote_ports_list.get(i%remote_ports_hash.size()));

				i++;
				count++;
//				Log.d("Insert","count:"+count);



//				Log.d("Insert",key_hashed.compareTo(indi_port)>0?"key greater than mine":"key less than mine");
				if(key_hashed.compareTo(max)>0){
					if(myPort_hash.compareTo(min)==0){
						Log.d("Insert", "key greater than max. I am min. So storing. Also forwarding to next 3.");
						check_and_insert_hmap(key,value);
//						Log.d("Insert", "After inserting. Hashmap state."+hmap.toString());
						uri = forward_replicas(remote_ports_hash.indexOf(indi_port)+1,key,value,uri,2); return uri;
					}
					else{
						Log.d("Insert", "key greater than max. Forwarding to 3.");
						uri = forward_replicas(remote_ports_hash.indexOf(min),key,value,uri,3); return uri;
					}
				}
				if(key_hashed.compareTo(min)<0){
					if(myPort_hash.compareTo(min)==0){
						Log.d("Insert", "key less than min. I am min. So storing. Also forwarding to next 2.");
						check_and_insert_hmap(key,value);
//						Log.d("Insert", "After inserting. Hashmap state."+hmap.toString());
						uri = forward_replicas(remote_ports_hash.indexOf(indi_port)+1,key,value,uri,2);return uri;
					}
					else{
						Log.d("Insert", "key less than min. Forwarding to 3");
						uri = forward_replicas(remote_ports_hash.indexOf(min),key,value,uri,3);return uri;
					}
				}
				if(key_hashed.compareTo(indi_port) > 0 ){
					Log.d("Insert", "key greater than curr. continuing.");
					continue;
				}
				if(key_hashed.compareTo(indi_port) < 0 && count>remote_ports_hash.size()){
//					Log.d("Insert", "key less than mine. and count > ... so storing and forwarding to 2.");
//					hmap.put(key,value);
//					Log.d("Insert", "After inserting. Hashmap state."+hmap.toString());
//					forward_replicas(remote_ports_hash.indexOf(indi_port)+1,key,value,uri,2);
					if(indi_port.compareTo(myPort_hash)==0){
						Log.d("Insert", "key less than mine. and i am me. and count > ... so storing and forwarding to 2.");
						check_and_insert_hmap(key,value);
//						Log.d("Insert", "After inserting. Hashmap state."+hmap.toString());
						uri = forward_replicas(remote_ports_hash.indexOf(indi_port)+1,key,value,uri,2);return uri;
					}
					else{
						Log.d("Insert", "key less than mine. and count > ... But I am not it s0 forwarding to 3.");
						uri = forward_replicas(remote_ports_hash.indexOf(indi_port),key,value,uri,3);return uri;
					}
				}

			}


		} catch (Exception e) {
			e.printStackTrace();
		}

//        Log.v("insert", values.toString());
		return uri;
	}

	void check_and_insert_hmap(String key, String value){

		//if keys exists .. increment version.. both for hmap and all_resp_nodes_key_value

		if(hmap.containsKey(key)){
//			hmap.put(key,value+" "+Integer.parseInt((hmap.get(key).toString().split(" ")[1])+1));
//			hmap.put(key,value+"@"+(Integer.parseInt((hmap.get(key).toString().split("@")[1])+1)));
			hmap.put(key,value+"@"+(Integer.parseInt((hmap.get(key).toString().split("@")[1]))+1));

			return;
		}

		//else just do the below and and insert
		else {
//			hmap.put(key, value+" "+1);
			hmap.put(key, value+"@"+1);
			return;
		}
	}

	String getmin(){
		Object[] temp = remote_ports_hash.toArray();
		Arrays.sort(temp);
		return temp[0].toString();
	}
	String getmax(){
		Object[] temp = remote_ports_hash.toArray();
		Arrays.sort(temp);
		return temp[temp.length - 1].toString();
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		try {
			hmap = new ConcurrentHashMap<String, String>();
			if(hmap.size()!=0){
				hmap.clear();
			}
			if(all_resp_nodes_key_value.size()!=0){
				all_resp_nodes_key_value.clear();
			}
			//need the below to be in sequence. Neighbour info needed!!!
			remote_ports_hash.add(genHash("5562"));
			remote_ports_hash.add(genHash("5556"));
			remote_ports_hash.add(genHash("5554"));
			remote_ports_hash.add(genHash("5558"));
			remote_ports_hash.add(genHash("5560"));
			//need the below to be in sequence. Neighbour info needed!!!
			remote_ports_list.add("11124");
			remote_ports_list.add("11112");
			remote_ports_list.add("11108");
			remote_ports_list.add("11116");
			remote_ports_list.add("11120");


//			single_avd = true;
//			Log.d("Oncreate", "Single_avd?: " + single_avd);

			Context context = getContext();
			TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			myPort = String.valueOf((Integer.parseInt(portStr)));
//			global_max = myPort;
//			global_min = myPort;
//			pred = myPort;
//			suc = myPort;
			myPort_hash = genHash(myPort);
			Log.d("Oncreate", "check!!");

			Log.d("Oncreate", "myport:" + myPort);
			Log.d("Oncreate", "min:" + getmin());
			Log.d("Oncreate", "max:" + getmax());


//			Log.d("Oncreate", "hashed myport:" + genHash(myPort));


			ServerSocket serverSocket = null;
			serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

//			new JoinRequestTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);

			// TODO: 5/1/17 Recovery from failure handle? 
			//ping all avds and ask for missing info and store
			new RetrieveMissedInfoTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, myPort);


		}catch(Exception e){
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		MatrixCursor cursor = new MatrixCursor(new String[] { "key", "value" });
		HashMap key_values_taken = null;
		Log.d("Query", "******* Selection is ----"+selection);

		try {
			if (selection.endsWith("+")) {
				Log.d("Query", "Selection ends with + ");

				selection = selection.substring(0,selection.length()-1);
				if (selection.equals("@")) {
					Set key_set = hmap.keySet();

					Log.d("Query", "Selection is @");

					for (Object keys : key_set) {
//						Log.d("Query:", "selection - adding value to cursor" + selection + hmap.get(keys));
//						cursor.addRow(new Object[]{keys, hmap.get(keys).toString().split("@")[0]});
						cursor.addRow(new Object[]{keys, hmap.get(keys).toString()});//need to send entirely. because host node needs version.
					}
					return cursor;
				}
				if (selection.equals("*")) {
					//1 - So that it does not goes into infinite loop.
					Set key_set = hmap.keySet();
					Log.d("Query", "Selection is *");
					for (Object keys : key_set) {
						Log.d("Query:", "selection - adding value to cursor" + keys + hmap.get(keys));
//						cursor.addRow(new Object[]{keys, hmap.get(keys).toString().split("@")[0]});
						cursor.addRow(new Object[]{keys, hmap.get(keys).toString()});//need to send entirely. because host node needs version.
					}
					return cursor;
				}else {
//					String value = hmap.get(selection).toString().split("@")[0];
					String value = hmap.get(selection).toString();//need to send entirely. because host node needs version.
					Log.d("Query", "Selection is individual and found locally.");
//					Log.d("Query:", "selection - hmap Status:" + selection + hmap.toString());
					cursor.addRow(new Object[]{selection, value});
					return cursor;
				}

			//selection doesnt end with +
			}else{
				Log.d("Query", "Selection doesnt end with +");

				if (selection.equals("@")) {
					Set key_set = hmap.keySet();
					Log.d("Query", "Selection is @");

					for (Object keys : key_set) {
//						Log.d("Query:", "selection - adding value to cursor" + selection + hmap.get(keys));
						cursor.addRow(new Object[]{keys, hmap.get(keys).toString().split("@")[0]});//sends key,value@version
					}
					return cursor;
				}
				if (selection.equals("*")) {
					//1 - first process local
					Set key_set = null;
					key_set  = hmap.keySet();
					JSONObject jobj = new JSONObject();

					Log.d("Query", "Selection is *");

					// TODO: 5/7/17  can optimize this. no need to attach the local keys to the json being sent.
					for (Object keys : key_set) {
						String val = hmap.get(keys).toString();
						Log.d("Query:", "selection - adding to json" + selection + hmap.get(keys));
//						jobj.put(keys.toString(), val.split(" ")[0]+val.split(" ")[1]);
						jobj.put(keys.toString(), val);//key,value@version 
					}

//					Log.d("Query", "Selection is *");

					//Will store all data .. local and collected into a hmap ... then convert that hashmap into a cursor and return.
					//this new hashmap will contain version data as well.
					ConcurrentHashMap all_avds_data_hmap = null;
					all_avds_data_hmap = hmap;

					for (String indi_avd : remote_ports_list) {
						try {

							if (indi_avd.equals(Integer.toString(Integer.parseInt(myPort) * 2))) {
								continue;
							}
//						String remotePort = Integer.toString(Integer.parseInt(suc) * 2); // sending the request to an already hard coded node.
							String remotePort = indi_avd;
							Log.d("Query:", "Collecting results from : " + remotePort);
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
							socket.setSoTimeout(300);


							String msgToSend = "Query" + " " + myPort + " " + selection + " " + jobj.toString();

							Log.d("Query:", "msgToSend" + msgToSend);


							PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
							out.println(msgToSend);
							String message_from_server = null;
							BufferedReader in_from_server = null;
							in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							message_from_server = in_from_server.readLine();
							if(message_from_server==null||message_from_server=="null"||message_from_server.equals(null)||message_from_server.equals("null")){
								Log.e("Query","Querying a dead node!!!");
								continue;
							}
							Log.d("Query", message_from_server);

							socket.close();

							Log.d("Query", "waiting for take...");

							try {
								key_values_taken = pbq.take();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							Log.d("Query", "take released");

							//3 - add data received after circle to cursor.
							Set key_set_taken = key_values_taken.keySet();


							//if key exists ... compare value split .. version
							for (Object keys : key_set_taken) {
								String val_part_taken= key_values_taken.get(keys).toString();
								Log.d("Query:", "selection - adding to consolidated hasmap if version appropriate" + keys + " " + key_values_taken.get(keys));
//							cursor.addRow(new Object[]{keys, key_values_taken.get(keys)});
								if (all_avds_data_hmap.containsKey(keys)) {
									String val_part_all_avd = all_avds_data_hmap.get(keys).toString();
//									if (val_part_all_avd.substring(val_part_all_avd.length()-1).compareTo(val_part_taken.substring(val_part_taken.length()-1)) < 0) {
									if (val_part_all_avd.split("@")[1].compareTo(val_part_taken.split("@")[1]) < 0) {//expecting key,value@version
										Log.d("Query:", "This new key has newer version .. so replacing");
										all_avds_data_hmap.put(keys,val_part_taken);
									}
								} else {
									Log.d("Query:", "Old version only.");
									all_avds_data_hmap.put(keys, val_part_taken);
								}
							}


						}catch(SocketTimeoutException e){
							e.printStackTrace();
							Log.e("Query:","This port has failed: "+indi_avd+" "+"continuing" );
							continue;
						}catch (Exception e){
							e.printStackTrace();
							Log.e("Query","Not a sockettimeout exception!!!");
							continue;
						}
					}

					Log.d("Query","***********Result of star*******"+all_avds_data_hmap.toString());

					Set keys_all_avd_data = all_avds_data_hmap.keySet();
					for(Object each_key : keys_all_avd_data){
//						String val_with_ver = key_values_taken.get(each_key).toString();
						String val_with_ver = all_avds_data_hmap.get(each_key).toString();
//						cursor.addRow(new Object[]{each_key, val_with_ver.substring(0,val_with_ver.length()-1)});
						cursor.addRow(new Object[]{each_key, val_with_ver.split("@")[0]});//expecting key,value@version
					}
					return cursor;




				//selection is individual and doesnt end with +
				}else{
//					String value = (String) hmap.get(selection);
//					if(value!=null) {
//						Log.d("Query", "Selection is individual and found locally.");
//
////						Log.d("Query:", "selection - hmap Status:" + selection + hmap.toString());
////						cursor.addRow(new Object[]{selection, value.toString().split(" ")[0]});
//						cursor.addRow(new Object[]{selection, value.toString().split("@")[0]});//expecting key,value@version
//
//						return cursor;
//					}else {
						//need to query single key, find its loc, and query from it.
						Log.d("Query", "need to query single key, query all replicas and return latest version");

						HashMap temp = new HashMap();

//						String to_be_queried_from = null;
						int to_be_queried_from = 0;
						int size_map = remote_ports_list.size();

						try {
							String key = selection;
							String key_hashed = genHash(selection);
							String max = getmax();
							String min = getmin();

							int count =0;
							Log.d("Query", "processing query key(selection) received,selection hashed: "+key_hashed);

							int i = 0 ;
							while (count < remote_ports_hash.size()*2){
								String indi_port = remote_ports_hash.get(i%remote_ports_hash.size());
								Log.d("Query", "key/selection hashed - "+key_hashed+" - port hashed - "+indi_port);
								Log.d("Query","current port - "+ remote_ports_list.get(i%remote_ports_hash.size()));

								i++;
								count++;
								Log.d("Query","count:"+count);

								Log.d("Query",key_hashed.compareTo(indi_port)>0?"key greater than mine":"key less than mine");
								if(key_hashed.compareTo(max)>0){
									if(myPort_hash.compareTo(min)==0){
										//wont be needed.
										Log.d("Query", "key greater than max. I am max.");
//										to_be_queried_from = remote_ports_list.get((remote_ports_hash.indexOf(min)+2)%size_map);break;
										to_be_queried_from = remote_ports_hash.indexOf(min);break;
									}
									else{
										Log.d("Query", "key greater than max. I am max.");
										Log.d("Query", "remote_ports_list.indexOf(indi_port) - "+remote_ports_hash.indexOf(min));
										Log.d("Query", "(remote_ports_list.indexOf(indi_port)+2)%size_map - "+(remote_ports_hash.indexOf(min)+2)%size_map);
//										to_be_queried_from = remote_ports_list.get((remote_ports_hash.indexOf(min)+2)%size_map);break;
										to_be_queried_from = remote_ports_hash.indexOf(min);break;
									}
								}
								if(key_hashed.compareTo(min)<0){
									if(myPort_hash.compareTo(min)==0){
										Log.d("Query", "key less than min. I am min.");
//										to_be_queried_from = remote_ports_list.get((remote_ports_hash.indexOf(max)+2)%size_map);break;
										to_be_queried_from = remote_ports_hash.indexOf(max);break;
									}
									else{
										Log.d("Query", "key less than min. I am min.");
										Log.d("Query", "remote_ports_list.indexOf(indi_port) - "+remote_ports_hash.indexOf(max));
										Log.d("Query", "(remote_ports_list.indexOf(indi_port)+2)%size_map - "+(remote_ports_hash.indexOf(max)+2)%size_map);
//										to_be_queried_from = remote_ports_list.get((remote_ports_hash.indexOf(max)+2)%size_map);break;
										to_be_queried_from = remote_ports_hash.indexOf(max);break;

									}
								}
								if(key_hashed.compareTo(indi_port) > 0 ){
									Log.d("Query", "key greater than curr. continuing.");
									continue;
								}
								if(key_hashed.compareTo(indi_port) < 0 && count>remote_ports_hash.size()){
									if(indi_port.compareTo(myPort_hash)==0){
										Log.d("Query", "key less than mine. and count > ... and i am that");
										Log.d("Query", "remote_ports_list.indexOf(indi_port) - "+remote_ports_hash.indexOf(indi_port));
										Log.d("Query", "(remote_ports_list.indexOf(indi_port)+2)%size_map - "+(remote_ports_hash.indexOf(indi_port)+2)%size_map);
//										to_be_queried_from = remote_ports_list.get((remote_ports_hash.indexOf(indi_port)+2)%size_map);break;
										to_be_queried_from = remote_ports_hash.indexOf(indi_port);break;

									}
									else{
										Log.d("Query", "key less than mine. and count > ... and i am not that");
										Log.d("Query", "remote_ports_list.indexOf(indi_port) - "+remote_ports_hash.indexOf(indi_port));
										Log.d("Query", "(remote_ports_list.indexOf(indi_port)+2)%size_map - "+(remote_ports_hash.indexOf(indi_port)+2)%size_map);
//										to_be_queried_from = remote_ports_list.get((remote_ports_hash.indexOf(indi_port)+2)%size_map);break;
										to_be_queried_from = remote_ports_hash.indexOf(indi_port);break;
									}
								}

							}


						} catch (Exception e) {
							e.printStackTrace();
						}
//						int to_be_queried_from_loc = remote_ports_list.indexOf(to_be_queried_from);
						String[] all_thress_resp = {remote_ports_list.get(to_be_queried_from),
													remote_ports_list.get((to_be_queried_from+1)%size_map),
													remote_ports_list.get((to_be_queried_from+2)%size_map)//changed from +12 to +2
						};

//						if(Integer.toString(Integer.parseInt(myPort)*2)==to_be_queried_from){
//						if(remote_ports_list.get(to_be_queried_from).equals(Integer.parseInt(myPort)*2)){
//							Log.e("Query","Says I am resposible, but not found");
//						}else {

							//need a for loop and try catch here. ******************
					Set key_set_taken = null;
							for(int i=0;i<=2;i++) {
								try {
//									String remotePort = Integer.toString(Integer.parseInt(to_be_queried_from)-4*i);
									if(!remote_ports_list.get(to_be_queried_from).equals(Integer.parseInt(myPort)*2)) {
										String remotePort = all_thress_resp[i];
										Log.d("Query:", "Individual query --- will be queried from(to_be_queried_from)" + remotePort);
										Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
										socket.setSoTimeout(300);

										JSONObject jobj = new JSONObject();
										jobj.put(selection, hmap.get(selection));

										String msgToSend = "Query" + " " + myPort + " " + selection + " " + jobj.toString();//guess it handles null as wel.

										PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
										out.println(msgToSend);
										String message_from_server = null;
										BufferedReader in_from_server = null;
										in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
										message_from_server = in_from_server.readLine();
										if(message_from_server==null||message_from_server=="null"||message_from_server.equals(null)||message_from_server.equals("null")){
											Log.e("Query","Querying a dead node!!!");
											continue;
										}
										Log.d("Query", message_from_server);

										socket.close();
										//2 wait for circle completion via take.
										Log.d("Query", "waiting for take ... ");

										key_values_taken = pbq.take();
										Log.d("Query", " take released.");

									}
									else{
										Log.d("Query", " One of the nodes responsible for the selection is local. So adding it to results.");
										key_values_taken.put(selection,hmap.get(selection));
									}
									key_set_taken = key_values_taken.keySet();

									for (Object keys : key_set_taken) {
										Log.d("Query:", "selection - adding to consolidated hasmap if version appropriate" + keys +" "+ key_values_taken.get(keys));
										String val_part = key_values_taken.get(keys).toString();
										//temp stores key val from all responsible nodes. so compare version and return latest version.
										if(temp.containsKey(keys)){
//											if(temp.get(keys).toString().substring(val_part.length()-1).compareTo(val_part.substring(val_part.length()-1)) < 0){
											if(temp.get(keys).toString().split("@")[1].compareTo(val_part.split("@")[1]) < 0){//expects key,value@version
												Log.d("Query:","This new key has newer version .. so replacing");
//												temp.put(keys,val_part.substring(0,val_part.length()-1));
												temp.put(keys,val_part);
											}
										}
//										else{temp.put(keys,val_part.substring(0,val_part.length()-1));}
										else{temp.put(keys,val_part);}
									}
								}catch(SocketTimeoutException e){
									Log.e("Query","queried a dead node i guess");
									e.printStackTrace();
									continue;
								}catch (Exception e){
									Log.e("Query","Not time out exception!!");
									e.printStackTrace();
									continue;
								}
							}
//						}

						// TODO: 5/7/17  no need to for here. since most recent verison itself is being stored. we can just get key value and 
						// TODO: 5/7/17 just add to cursor and return.
//						Set key_set_of_temp = temp.keySet();
//						for (Object keys : key_set_of_temp) {
//							Log.d("Query:", "selection - adding value" + keys + temp.get(keys));
////							cursor.addRow(new Object[]{keys, temp.get(keys).toString()});
//							cursor.addRow(new Object[]{keys, temp.get(keys).toString().split("@")[0]});
//						}
//						return cursor;
						cursor.addRow(new Object[]{selection,temp.get(selection).toString().split("@")[0]});
					return cursor;
//					}
				}

			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return cursor;
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	// TODO: 5/8/17 retrieve all avds replica info .. not just mine i guess.

	private class ServerTask extends AsyncTask<ServerSocket, String, Void>{


		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket2 = sockets[0];
			Socket clientSocket = null;
			BufferedReader in = null;
			String message = null;
//            ContentResolver mContentResolver = getContentResolver();
			ContentResolver mContentResolver = null;
			Uri mUri;
			mUri = buildUri("content", "content://edu.buffalo.cse.cse486586.simpledynamo.provider");



			try{
				while (true){
					Log.e(TAG, "waiting for client");
					clientSocket = serverSocket2.accept();
					Log.d(TAG, "Server was able to connect to client to receive message");
					in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					message = in.readLine();
					if(message==null || message == "null "|| message.equals("null")||message.equals(null)){
						continue;
					}
					System.out.println(message);

					String[] data_received = message.split(" ");
//					if (data_received[0].equals("JoinRequest")) {
//
//						Log.d("ServerTask", "JoinRequest from : " + data_received[1]);
//						join_request_handle(data_received[1], serverSocket2, clientSocket);
//						Log.d("ServerTask", "JoinRequest from : " + data_received[1] + "HANDLED!!");
//						//inform everyone except 5554 to update
//						inform_everyone();
//						Log.d("ServerTask", "Informed Everyone -- returned");
//
//						avd_count++;
//						Log.d("ServerTask", "avd count increased to: "+avd_count);
//
//						if(avd_count>0){
//							Log.d("ServerTask", "No more single avd, avd count: "+avd_count);
//							single_avd = false;
//						}
//					}

//					if (data_received[0].equals("UpdateInfo")) {
//						pred = data_received[1];
//						suc = data_received[2];
//						global_min= data_received[3];
//						global_max = data_received[4];
//
//						Log.e("ServerTask","AfterUpdation - Mine: "+myPort+" Pred: "+pred+" Suc: "+suc+" global min: "
//								+global_min+" global max: "+global_max);
//
//						PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
//						out.println("updated here : " + myPort + "with" + pred + " " + suc);
////                        clientSocket.close();
//					}

					if (data_received[0].equals("Forwarded")) {
						String key_part = data_received[1];
						String value_part = data_received[2];
						Log.d("ServerTask", "Processing forwarded");
						Log.e("ServerTask","Received key value: "+key_part+" "+value_part );

						PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
						out.println("Key value received");

//                        ContentValues cv = new ContentValues();
//                        cv.put("key", key_part);
//                        cv.put("value", value_part);
//
//                        insert(mUri, cv);
//                        new JoinRequestTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);
						publishProgress(message);
						Log.d("ServerTask", "Return from progress update.. new thread spawned.");
					}
					if (data_received[0].equals("Immediate")) {
						String key_part = data_received[1];
						String value_part = data_received[2];
						Log.d("ServerTask", "Processing Immediate");
						Log.e("ServerTask","Received key value: "+key_part+" "+value_part );

						PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
						out.println("Key value received");

//                        ContentValues cv = new ContentValues();
//                        cv.put("key", key_part);
//                        cv.put("value", value_part);
//
//                        insert(mUri, cv);

						publishProgress(message);
						Log.d("ServerTask", "Return from progress update.. new thread spawned.");

					}
					if(data_received[0].equals("Query")) {
//                        String json_string = null;
						PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
						out.println("from server : will query and return");

						publishProgress(message);
						Log.d("ServerTask", "Return from progress update.. new thread spawned. I will handle query.");

					}
					if(data_received[0].equals("Delete")){//"Delete" + " " + myPort + " " + selection;
						PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

						Log.d("ServerTask", "Delete request received.--"+data_received[2]);

						String selection = data_received[2];

						Integer i = delete(mUri, selection+"+",null);

						Log.d("ServerTask", "local deletion done.");
						out.println("Deletion done");
						Log.d("ServerTask", "Responded to 'delete' request");

					}
					if(data_received[0].equals("Retrieve")) {//only 2 info in message.. retrieve and myport..so no need new thread.
						String its_port = Integer.toString (Integer.parseInt(data_received[1])*2);//stored in 11 format .. but myport is 55 format.
						Log.d("ServerTask","Received retrive message from : " + data_received[1] +" i.e. "+its_port);

						PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
						String message_to_return = null;
//						Log.d("ServerTask","all_resp_nodes_key_value - "+all_resp_nodes_key_value.toString());
						if(all_resp_nodes_key_value.containsKey(its_port)){
							HashMap all_key_node_resp_for = all_resp_nodes_key_value.get(its_port);


							Log.d("ServerTask","All key-vals of -"+data_received[1]);
							Set keyss = all_key_node_resp_for.keySet();
							for(Object key:keyss){
								Log.d("Server task",key +" "+all_key_node_resp_for.get(key).toString());
							}


							JSONObject parent_obj = new JSONObject();

							Set all_nodes = all_resp_nodes_key_value.keySet();
							for(Object each_node : all_nodes) {
								JSONObject child_obj = new JSONObject();
								HashMap keyvals_of_this_node = all_resp_nodes_key_value.get(each_node);
								Set all_keys = all_resp_nodes_key_value.get(each_node).keySet();
								for (Object each_key : all_keys){
									child_obj.put(each_key.toString(),keyvals_of_this_node.get(each_key));
								}
								parent_obj.put(each_node.toString(),child_obj.toString());
							}
							message_to_return = parent_obj.toString();
						}else{
							Log.d("ServerTask","This does not pertain to failure handling");
						}
//						Log.d("ServerTask","Returning replica info - "+message_to_return);
						out.println(message_to_return);
						Log.d("ServerTask", "Responded to 'retrieve' request");

					}
				}

			}catch (Exception e) {
				Log.e(TAG, "Error at server task.");
				e.printStackTrace();
			}finally {
				try {
					serverSocket2.close();
					clientSocket.close();

				} catch (Exception e) {
					Log.e(TAG, "error closing serverSocket/clientSocket");
					e.printStackTrace();

				}
				return null;
			}

		}

//		void inform_everyone(){
//			Set all_avd_keys = all_avds.keySet();
//			Log.d("InformEveryone:","Size of all_avds table:"+ all_avds.size());
//
//			for(Object curr_key: all_avd_keys){
//				each_avd_info cur = all_avds.get(curr_key);
//				Log.d("InformEveryone:","informing "+cur.mine+"  -- before if statement");
//				if(!curr_key.toString().equals("5554")){//because server on 5554 is already busy.
//					Log.d("InformEveryone:","informing "+cur.mine+"  -- before entering loop");
//					try {
////                        String myPort = msgs[0];
//
//						String remotePort = Integer.toString (Integer.parseInt(cur.mine)*2); // sending the request to an already hard coded node.
//						Log.d("InformEveryone:","informing "+cur.mine);
//						Log.d("InformEveryone:","informing "+remotePort);
//						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//								Integer.parseInt(remotePort));
//
//						String msgToSend = "UpdateInfo"+" "+cur.pred+" "+cur.suc+" "+global_min+" "+global_max;
//
//						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//						out.println(msgToSend);
//						String message_from_server = null;
//						BufferedReader in_from_server = null;
//						in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//						message_from_server = in_from_server.readLine();
//						Log.d("InformEveryone",message_from_server);
////                        String [] pred_suc = message_from_server.split(" ");
////                        pred = pred_suc[0];
////                        suc = pred_suc[1];
////                        String reply_to_pred_suc_info = "received pred_suc info";
////                        PrintWriter out2 = new PrintWriter(socket.getOutputStream(), true);
////                        out2.println(msgToSend);
//						socket.close();
//					} catch (UnknownHostException e) {
//						Log.e("JoinRequestTask", String.valueOf(e.getStackTrace()));
//						e.printStackTrace();
//					} catch (IOException e) {
//						Log.e("JoinRequestTask", String.valueOf(e.getStackTrace()));
//						e.printStackTrace();
//					}
//				}
//				else{
//					Log.d("InformEveryone:","informing (local setting) "+cur.mine);
//					pred = cur.pred;
//					suc = cur.suc;
//					//noneed to update global max and min.
//					Log.e("ServerTask","AfterUpdation - Mine: "+myPort+" Pred: "+pred+" Suc: "+suc);//works becoz we are on 5554.
//
//				}
//			}
//			return;
//		}

		void update_min_max(){
			try {
				Set all_avd_keys = all_avds.keySet();

				String min_loop = "zzzzz";
				String max_loop = "00000";

				String min = null;
				String max = null;

				for (Object curr1 : all_avd_keys) {
					if (genHash(curr1.toString()).compareTo(min_loop) < 0) {//curr object is least
						Log.d("JoinRequestHandle", "finding min: " + curr1.toString() + " < " + min);
						Log.d("JoinRequestHandle", "i.e. " + genHash(curr1.toString()) + " < " + min_loop);

						min = curr1.toString();
						min_loop = genHash(curr1.toString());
					}
				}
				for (Object curr1 : all_avd_keys) {
					if (genHash(curr1.toString()).compareTo(max_loop) > 0) {//curr object is least

						Log.d("JoinRequestHandle", "finding max: " + curr1.toString() + " > " + max);
						Log.d("JoinRequestHandle", "i.e. " + genHash(curr1.toString()) + " > " + max_loop);

						max = curr1.toString();
						max_loop = genHash(curr1.toString());

					}
				}
				Log.e("JoinrequestHandle", "minimum:" + min);
				Log.e("JoinrequestHandle", "maximum:" + max);

				global_max = max;//just to make it easier .. created extra variables.
				global_min = min;

				return;

			}catch (Exception e){
				e.printStackTrace();
			}
		}

//		void join_request_handle(String new_port, ServerSocket serverSocket, Socket clientSocket){
//			try {
//				Log.d("JoinRequestHandle","JoinRequest from : "+new_port+ " being handled");
//
//				String new_port_hash = genHash(new_port);
//				each_avd_info new_avd = new each_avd_info();
//				new_avd.mine = new_port;
//
////                Set all_avd_keys = all_avds.keySet();
////
////                String min_loop = "zzzzz";
////                String max_loop = "00000";
////
////                String min = null;
////                String max = null;
////
////                for (Object curr1: all_avd_keys){
////                    if(genHash(curr1.toString()).compareTo(min_loop)<0){//curr object is least
////                        Log.d("JoinRequestHandle","finding min: "+ curr1.toString() + " < "+min);
////                        Log.d("JoinRequestHandle","i.e. "+ genHash(curr1.toString()) + " < "+min_loop);
////
////                        min = curr1.toString();
////                        min_loop = genHash(curr1.toString());
////                    }
////                }
////                for (Object curr1: all_avd_keys){
////                    if(genHash(curr1.toString()).compareTo(max_loop)>0){//curr object is least
////
////                        Log.d("JoinRequestHandle","finding max: "+ curr1.toString() + " > "+max);
////                        Log.d("JoinRequestHandle","i.e. "+ genHash(curr1.toString()) + " > "+max_loop);
////
////                        max = curr1.toString();
////                        max_loop = genHash(curr1.toString());
////
////                    }
////                }
////                Log.e("JoinrequestHandle","minimum:"+min);
////                Log.e("JoinrequestHandle","maximum:"+max);
////
////                global_max = max;//just to make it easier .. created extra variables.
////                global_min = min;
//
////                update_min_max();
//
//
//				if(new_port.equals("5554")){
//					new_avd.pred = "5554";
//					new_avd.suc = "5554";
//					new_avd.set_hashes();
//					all_avds.put(new_avd.mine,new_avd);
//					update_min_max();
////                    System.out.println("Displaying added content:");
////                    each_avd_info x = all_avds.get(new_avd.mine);
////                    x.display_all();
//
//					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
//					out.println(new_avd.pred+" "+new_avd.suc+" "+global_min+" "+global_max);
//					BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//					System.out.println(in.readLine());
////                    clientSocket.close();
//					return;
//				}
//
//				if(all_avds.size() == 1 && !new_port.equals("5554")){//second part of & to make sure its not done after adding 11108 itself
//					Log.d("join request handle","New node" + new_port+"is the second avd");
//					new_avd.pred = "5554";
//					new_avd.suc = "5554";
//					new_avd.set_hashes();
//					all_avds.put(new_avd.mine,new_avd);
//					each_avd_info x = all_avds.get("5554");
//					x.suc = new_port;
//					x.pred = new_port;
//					x.set_hashes();
//
//					update_min_max();
//
////                    System.out.println("Displaying content after adding:");
////                    each_avd_info y = all_avds.get(new_avd.mine);
////                    y.display_all();
////                    System.out.println("Checking if 5554 changed:");
////                    each_avd_info z = all_avds.get("5554");
////                    z.display_all();
//
//					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
//					out.println(new_avd.pred+" "+new_avd.suc+" "+global_min+" "+global_max);
//					BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//					System.out.println(in.readLine());
////                    clientSocket.close();
//					return;
//				}
//
//				Set all_avd_keys = all_avds.keySet();
//
//				int visit_count = 0;
//				while(true) {//So that it works in ring. No other if will be exxecuted becoz conditions same except visit_count.
//					for (Object curr : all_avd_keys) {
//						visit_count++;
//						each_avd_info cur = all_avds.get(curr);
//						if (new_port_hash.compareTo(cur.mine_hash) > 0) {//conclusive
//							if (new_port_hash.compareTo(cur.suc_hash) < 0) {
//								Log.d("JoinRequestHandler","new port hash: "+new_port_hash+"greater than mine : "+cur.mine_hash+
//										" but less than my suc: "+ cur.suc_hash);
//
//								new_avd.pred = cur.mine;
//								new_avd.suc = cur.suc;
//								each_avd_info x = all_avds.get(cur.suc);
//								x.pred = new_port;
//								cur.suc = new_port;
//
//								all_avds.put(new_avd.mine,new_avd);
//
//								new_avd.set_hashes();
//								x.set_hashes();
//								cur.set_hashes();
//
//								update_min_max();
//
//								PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
//								out.println(new_avd.pred + " " + new_avd.suc+" "+global_min+" "+global_max);
//								BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//								Log.d("JoinRequestHandler","response from new node: " + in.readLine());
////                                clientSocket.close();
//								return;
//								// just update on master and close connection. i.e. master has all updated info.
//								// later on inform everyone -- involved or not.
//								// new node will probably end up updating its value twice. no harm.
//							}
//							if ((new_port_hash.compareTo(genHash(global_max)) > 0) && visit_count > all_avds.size()) {
//								Log.d("ServerTask",new_port + " its the max");
//
//								new_avd.pred=all_avds.get(global_max).mine;
//								new_avd.suc = all_avds.get(global_min).mine;
//
//								all_avds.get(global_max).suc = new_port;
//								all_avds.get(global_min).pred = new_port;
//
//								new_avd.set_hashes();
//								all_avds.get(global_max).set_hashes();
//								all_avds.get(global_min).set_hashes();
//
//								all_avds.put(new_avd.mine,new_avd);
//
//								update_min_max();
//
//								PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
//								out.println(new_avd.pred + " " + new_avd.suc+" "+global_min+" "+global_max);
//								BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//								System.out.println(in.readLine());
////                                clientSocket.close();
//								return;
//							}
//						}
//
//						if ((new_port_hash.compareTo(genHash(global_min)) < 0) && visit_count > all_avds.size()){
//							Log.d("ServerTask",new_port + " its the min");
//
//							new_avd.suc = all_avds.get(global_min).mine;
//							new_avd.pred = all_avds.get(global_max).mine;
//
//							all_avds.get(global_max).suc = new_port;
//							all_avds.get(global_min).pred = new_port;
//
//							new_avd.set_hashes();
//							all_avds.get(global_max).set_hashes();
//							all_avds.get(global_min).set_hashes();
//
//							all_avds.put(new_avd.mine,new_avd);
//
//							update_min_max();
//
//							PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
//							out.println(new_avd.pred + " " + new_avd.suc+" "+global_min+" "+global_max);
//							BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//							System.out.println(in.readLine());
////                            clientSocket.close();
//							return;
//
//						}
//					}
//				}
//
//
//
//			}catch(Exception e){
//				Log.e("join_request_handle", String.valueOf(e.getStackTrace()));
//				e.printStackTrace();
//			}
//
//			return;
//		}

		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}

		protected  void onProgressUpdate(String... strings) {

			String message = strings[0].trim();

			String[] data_received;

			data_received = message.split(" ");

			String key_part = data_received[1];
			String value_part = data_received[2];

			if(data_received[0].equals("Query")){

//				Log.d("ProgressUpdate", "In progress update - Query part");
//				Log.d("ProgressUpdate", "Spawning new thread. - Query Part");

				new JustQueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
//				Log.d("ProgressUpdate", "Spawned new thread. So returning. ");

				return;

			}
//			Log.d("ProgressUpdate", "In progress update");
			Log.e("ProgressUpdate", "In progress update : Received key value: " + key_part + " " + value_part);
//			Log.d("ProgressUpdate", "Spawning new thread.");

			new JustInsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
//			Log.d("ProgressUpdate", "Spawned new thread. So returning. ");

			return;
		}

	}

//	private class JoinRequestTask extends AsyncTask<String, Void, Void> {
//
//		@Override
//		protected Void doInBackground(String... msgs) {
//			try {
//
//
//				Log.d("JoinRequesttask","Sending join request to 5554");
//				String myPort = msgs[0];
//
//
//				String remotePort = remote_ports[0]; // sending the request to an already hard coded node.
//
////                if(myPort.equals("5556")) {
////                    Log.e("JoinRequestTask", "Sleeping");
////                    Thread.sleep(1000);
////                }
//
//				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//						Integer.parseInt(remotePort));
//				String msgToSend = "JoinRequest"+" "+myPort;
//
//				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//				out.println(msgToSend);
//				Log.d("JoinRequesttask","Join request Sent");
//
//				String message_from_server = null;
//				BufferedReader in_from_server = null;
//				in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//				message_from_server = in_from_server.readLine();
//				Log.d("JoinRequestTask","Pred & suc & min & max:"+message_from_server);
//				String[] pred_suc =null;
//				if(message_from_server != null) {
//					if(!myPort.equals("5554")) {
//						Log.d("JoinRequesttask", "No more single avd. I am not 5554. ");
//						single_avd = false;
//					}
////                    }else if(myPort.equals("5554")){
////                        if(avd_count>1){
////                            Log.d("JoinRequesttask","No more single avd. I am 5554. ");
////
////                            single_avd = false;
////                        }
////                    }
//					pred_suc = message_from_server.split(" ");
//					pred = pred_suc[0];
//					suc = pred_suc[1];
//					global_min = pred_suc[2];
//					global_max = pred_suc[3];
//				}
//
//
//				String reply_to_pred_suc_info = "received pred_suc info";
//				PrintWriter out2 = new PrintWriter(socket.getOutputStream(), true);
//				out2.println(msgToSend);
//				socket.close();
//			} catch (UnknownHostException e) {
//				Log.e("JoinRequestTask", String.valueOf(e.getStackTrace()));
//				e.printStackTrace();
//			} catch (IOException e) {
//				Log.e("JoinRequestTask", String.valueOf(e.getStackTrace()));
//				e.printStackTrace();
//			}catch ( Exception e){
//				Log.e("JoinRequestTask", String.valueOf(e.getStackTrace()));
//				e.printStackTrace();
//				return null; // TODO: 4/11/17 Solve this.
//			}
//
//			return null;
//		}
//
//	}


	//JustInsertTask does not need any versioning.
	private class JustInsertTask extends AsyncTask<String, Void, Void> {

		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}


		@Override
		protected Void doInBackground(String...  msgs) {
			try {

				Log.d("JustInsertTask", "JustInsertTask");
				Uri mUri;
				mUri = buildUri("content", "content://edu.buffalo.cse.cse486586.simpledynamo.provider");

				String[] data_received;
				String message;
				message = msgs[0];
				data_received = message.split(" ");
				String key_part = data_received[1];
				String value_part = data_received[2];

				Log.e("JustInsertTask","In JustInsertTask : Received key value: "+key_part+" "+value_part );


				String[] resp_extracted = {data_received[3],data_received[4],data_received[5]};

				update_replication_info(key_part.substring(0,key_part.length()-1),value_part,resp_extracted);


				ContentValues cv = new ContentValues();
				cv.put("key", key_part);//so that forwarded inserts are not processed again in insert.
				cv.put("value", value_part);

				insert(mUri, cv);

			}catch ( Exception e){
				Log.e("JustInsertTask", String.valueOf(e.getStackTrace()));
				e.printStackTrace();
				return null; // TODO: 4/11/17 Solve this.
			}

			return null;
		}

		void update_replication_info(String key_part, String value_part, String[] resp_avds){
			// TODO: 5/8/17 check version.
			//************processing the responsible avds part
			//The hasmap that stores the info is made global.
			HashMap temp = new HashMap<String,String>();

			if(hmap.containsKey(key_part)){
				int ver = Integer.parseInt(hmap.get(key_part).toString().split("@")[1]);
				ver = ver+1;
				temp.put(key_part,value_part+"@"+ver);
			}else{
				temp.put(key_part,value_part+"@"+1);
			}

			for(String each_avd:resp_avds) {
				if(all_resp_nodes_key_value.containsKey(each_avd)){
//					all_resp_nodes_key_value.get(each_avd).put(key_part,value_part);
					all_resp_nodes_key_value.get(each_avd).put(key_part,temp.get(key_part).toString());
				}else{
					all_resp_nodes_key_value.put(each_avd, temp);
				}
			}
			//*************************sufficient i guess.
			return;
		}

	}

	private class JustQueryTask extends AsyncTask<String, Void, Void> {

		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}


		@Override
		protected Void doInBackground(String...  msgs) {
			try {

				Log.d("JustQueryTask", "JustQueryTask in "+myPort);
				Uri mUri;
				mUri = buildUri("content", "content://edu.buffalo.cse.cse486586.simpledynamo.provider");

				String[] data_received;
				String message;
				message = msgs[0];
				data_received = message.split(" ");
				String json_string = null;
				String origin_port = data_received[1];

				if (data_received[1].equals(myPort)) {

					Log.d("JustQueryTask", "Query has reached its origin.. consolidating and releasing lock");
					Log.d("JustQueryTask", "Data Received - json-string- " + data_received[3]);

					json_string = data_received[3];
					JSONObject obj = new JSONObject(json_string);

					HashMap<String, String> temp_hmap = new HashMap<String, String>();

					Iterator json_keys = null;

					json_keys = obj.keys();//****************

					while (json_keys.hasNext()) {
						Object key = json_keys.next();
						temp_hmap.put(key.toString(), obj.get(key.toString()).toString());
						Log.d("JustQueryTask", "putting into hmap: " + key.toString() + " " + obj.get(key.toString()).toString());
					}
					Log.d("JustQueryTask", "Adding to blocking queue");

					pbq.add(temp_hmap);

				} else {


					Log.d("JustQueryTask", "Processing received forward query. ");
					Log.d("JustQueryTask", "Data Received - json-string- " + data_received[3]);
					String selection = data_received[2];

					json_string = data_received[3];
					JSONObject obj = new JSONObject(json_string);

					MatrixCursor cursor = (MatrixCursor) query(mUri, null, selection + "+", null, null);
					Log.d("JustQueryTask", "Displaying cursor after querying : " + DatabaseUtils.dumpCursorToString(cursor));
					if (cursor.moveToFirst()) {
						do {
							StringBuilder sb = new StringBuilder();

							sb.append(cursor.getString(0));
							sb.append(" ");
							sb.append(cursor.getString(1));
//							sb.append(" ");
//							sb.append(cursor.getString(2));

							Log.d("JustQueryTask Task", String.format("Values: %s", sb.toString()));
//							Log.d("JustQueryTask Task", "key" + sb.toString().split(" ")[0] + " value " + sb.toString().split(" ")[1]+" version " +
//									sb.toString().split(" ")[2]);
							Log.d("JustQueryTask Task", "key" + sb.toString().split(" ")[0] + " value " + sb.toString().split(" ")[1]);
							if (cursor.getString(1) != null) {
								Log.d("JustQueryTask Task", "Updated the result json.");
//								Log.d("JustQueryTask Task", "Jsons value part: "+ sb.toString().split(" ")[1]+sb.toString().split(" ")[2]);

								Log.d("JustQueryTask Task", "Jsons value part: "+ sb.toString().split(" ")[1]);

//								obj.put(sb.toString().split(" ")[0], sb.toString().split(" ")[1]+sb.toString().split(" ")[2]);
//								obj.put(sb.toString().split(" ")[0], sb.toString().split(" ")[1]+sb.toString().split(" ")[2]);
								obj.put(sb.toString().split(" ")[0], sb.toString().split(" ")[1]);
							} else {
								Log.d("JustQueryTask Task", "Did not update result json as the value for key already present.");

							}

						} while (cursor.moveToNext());
					}

					String remotePort = Integer.toString(Integer.parseInt(origin_port) * 2);
					Log.d("Query:", "Replying back sto: " + origin_port);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
					socket.setSoTimeout(300);


					String msgToSend = data_received[0] + " " + data_received[1] + " " + data_received[2] + " " + obj.toString();

					Log.d("Query:", "msgToSend" + msgToSend);

					PrintWriter out2 = new PrintWriter(socket.getOutputStream(), true);
					out2.println(msgToSend);
					String message_from_server = null;
					BufferedReader in_from_server = null;
					in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					message_from_server = in_from_server.readLine();
					Log.d("Query", message_from_server);

					socket.close();

				}



			}catch ( Exception e){
				Log.e("JustQueryTask", String.valueOf(e.getStackTrace()));
				e.printStackTrace();
				return null; // TODO: 4/11/17 Solve this.
			}

			return null;
		}

	}

	// TODO: 5/8/17 retrieve all avds replica info .. not just mine i guess.
	private class RetrieveMissedInfoTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
			for (String indi_avd : remote_ports_list) {
				if (!indi_avd.equals(Integer.toString(Integer.parseInt(myPort) * 2))) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indi_avd));
						socket.setSoTimeout(300);

						Log.d("RetriveMissedInfoTask", "Retrieving from : " + indi_avd);
						String msgToSend = "Retrieve" + " " + myPort;
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
						out.println(msgToSend);
						String message_from_server = null;
						BufferedReader in_from_server = null;
						in_from_server = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						message_from_server = in_from_server.readLine();
//						Log.d("Query", "The retrieved keyvals:"+message_from_server);
						//json string to hmap
//						if (message_from_server.equals(null) || message_from_server==null || message_from_server=="null" || message_from_server.equals("null")) {
						if (message_from_server == null || message_from_server == "null" || message_from_server.equals(null) || message_from_server.equals("null")) {
							Log.d("RetriveMissedInfoTask", "No keys returned from retrieval request from : " + indi_avd);
						} else {
							Log.d("RetriveMissedInfoTask", "Received keyvals from  : " + indi_avd);

							String received_json_string = message_from_server;

							JSONObject parent_obj = new JSONObject(received_json_string);
							Log.d("Query", "The retrieved keyvals pertaining to local avd :" + parent_obj.get(Integer.toString(Integer.parseInt(myPort) * 2)));

							Iterator parent_keys = null;

							parent_keys = parent_obj.keys();//****************

							while (parent_keys.hasNext()) {
								Object parent_key = parent_keys.next();
								JSONObject child_obj = new JSONObject(parent_obj.get(parent_key.toString()).toString());
								Iterator child_keys = child_obj.keys();
								while (child_keys.hasNext()) {
									Object child_key = child_keys.next();
									if (parent_key.toString().equals(Integer.toString(Integer.parseInt(myPort) * 2))) {
										check_and_insert_hmap(child_key.toString(), child_obj.get(child_key.toString()).toString());
										restore_replica_info(parent_key.toString(), child_key.toString(), child_obj.get(child_key.toString()).toString());
									} else {
										restore_replica_info(parent_key.toString(), child_key.toString(), child_obj.get(child_key.toString()).toString());
									}
								}
							}
						}

						socket.close();

					} catch (Exception e) {
						e.printStackTrace();
						Log.d("retrive messages task", "retrieving from a dead node");
						continue;
					}
				}
			}
			Log.d("RetrieveMissedInfoTask", "After retrieving...hmap status");
			Set keyss= hmap.keySet();
			for (Object key: keyss){
				Log.d("RetrieveMissedInfoTask", key+" "+hmap.get(key));
			}
//			Log.d("RestoreReplicaInfo","Restored replica info");
//			Log.d("RestoreReplicaInfo",all_resp_nodes_key_value.toString());
			return null; //ibFjxsWnQSLeLBpcq5vSzNdXXJ0fyulr  EeBieYeiiLGuMPJZCgclUT00zbbaATaa
		}

		void check_and_insert_hmap(String key, String value) throws NoSuchAlgorithmException {

			//if keys exists .. increment version.. both for hmap and all_resp_nodes_key_value
			if (iamresp(key)) {
				if (hmap.containsKey(key)) {
//			hmap.put(key,value+" "+Integer.parseInt((hmap.get(key).toString().split(" ")[1])+1));
					if (hmap.get(key).toString().split("@")[1].compareTo(value.split("@")[1]) < 0) {
						Log.d("Check and insert", "retrieving after failure. New version found");
						hmap.put(key, value);
					}
					return;
				}

				//else just do the below and and insert
				else {
					hmap.put(key, value);//version is already provided by the backed up info.
					return;
				}
			} else {
				return;
			}
		}

		void restore_replica_info(String node, String key, String value) {
			HashMap temp = new HashMap();
			temp.put(key, value);
			if (all_resp_nodes_key_value.containsKey(node)) {
				if (all_resp_nodes_key_value.get(node).containsKey(key)) {
					if (all_resp_nodes_key_value.get(node).get(key).split("@")[1].compareTo(value.split("@")[1]) < 1) {
						all_resp_nodes_key_value.get(node).put(key, temp.get(key).toString());
					} else {
						//leave as is
					}

				} else {
					all_resp_nodes_key_value.get(node).put(key, temp.get(key).toString());
				}
			} else {
				all_resp_nodes_key_value.put(node, temp);
			}

			return;
		}

		boolean iamresp(String key) throws NoSuchAlgorithmException {
			int count = 0;
			String key_hashed = genHash(key);
//			Log.d("iamresp", "processing request received in insert - key hashed - "+key_hashed);
			String max = getmax();
			String min = getmin();
			int my_index = remote_ports_list.indexOf(Integer.toString(Integer.parseInt(myPort) * 2));
			int resp_index = 0;
			int i = 0;
			while (count < remote_ports_hash.size() * 2) {
				String indi_port = remote_ports_hash.get(i % remote_ports_hash.size());
//				Log.d("iamresp", "key hashed - " + key_hashed + " - port hashed - " + indi_port);
//				Log.d("iamresp", "current port - " + remote_ports_list.get(i % remote_ports_hash.size()));

				i++;
				count++;
//				Log.d("iamresp", "count:" + count);


//				Log.d("Insert",key_hashed.compareTo(indi_port)>0?"key greater than mine":"key less than mine");
				if (key_hashed.compareTo(max) > 0) {
					if (myPort_hash.compareTo(min) == 0) {
						return true;
					} else {
//						Log.d("Insert", "key greater than max. Forwarding to 3.");
//						uri = forward_replicas(remote_ports_hash.indexOf(min), key, value, uri, 3);
//						return uri;
						resp_index = remote_ports_hash.indexOf(min);
						break;
					}
				}
				if (key_hashed.compareTo(min) < 0) {
					if (myPort_hash.compareTo(min) == 0) {
						return true;
					} else {
//						Log.d("Insert", "key less than min. Forwarding to 3");
//						uri = forward_replicas(remote_ports_hash.indexOf(min), key, value, uri, 3);
//						return uri;
						resp_index = remote_ports_hash.indexOf(min);
						break;
					}
				}
				if (key_hashed.compareTo(indi_port) > 0) {
//					Log.d("Insert", "key greater than curr. continuing.");
					continue;
				}
				if (key_hashed.compareTo(indi_port) < 0 && count > remote_ports_hash.size()) {
//					Log.d("Insert", "key less than mine. and count > ... so storing and forwarding to 2.");
//					hmap.put(key,value);
//					Log.d("Insert", "After inserting. Hashmap state."+hmap.toString());
//					forward_replicas(remote_ports_hash.indexOf(indi_port)+1,key,value,uri,2);
					if (indi_port.compareTo(myPort_hash) == 0) {
//						Log.d("Insert", "key less than mine. and i am me. and count > ... so storing and forwarding to 2.");
//						check_and_insert_hmap(key,value);
//						Log.d("Insert", "After inserting. Hashmap state."+hmap.toString());
//						uri = forward_replicas(remote_ports_hash.indexOf(indi_port) + 1, key, value, uri, 2);
//						return uri;
						return true;

					} else {
//						Log.d("Insert", "key less than mine. and count > ... But I am not it s0 forwarding to 3.");
//						uri = forward_replicas(remote_ports_hash.indexOf(indi_port), key, value, uri, 3);
//						return uri;
						resp_index = remote_ports_hash.indexOf(indi_port);
						break;
					}
				}
			}

			Log.d("iamresp", "resp index - "+resp_index);
			Log.d("imresp","my index - "+my_index);

			for (int x =resp_index;x<=resp_index+2;x++){
				int y = x%remote_ports_list.size();
				if(my_index==y){
					return true;
				}
			}
			Log.e("iamresp","I am not resp for - "+key+" hashed - "+key_hashed);
			return false;
		}

	}
}
