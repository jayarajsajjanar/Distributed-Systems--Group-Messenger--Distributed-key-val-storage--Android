package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.View;
import android.view.View.OnKeyListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
//23:58:33.331
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
//    static final String TAG = "PA2B";

//    static final String REMOTE_PORT0 = "11108";
//    static final String REMOTE_PORT1 = "11112";
//    static final String REMOTE_PORT2 = "11116";
//    static final String REMOTE_PORT3 = "11120";
//    static final String REMOTE_PORT4 = "11124";

    static final String[] remote_ports = {"11108","11112","11116","11120","11124"};

    static int seq_num = 0;

    static final int SERVER_PORT = 10000;
    public String myPort = null;

    static ServerSocket serverSocket = null;
    // the below code is for PA2B
    // pqg is the greatest proposed sequence number proposed by an avd.
    // aqg is the greatest agreed sequece number that the avd has seen.
    // a is the
    static int pqg = 0;
    static int aqg = 0;
    static int a = 0;
    // TODO: 3/25/17 Change priority queue to blocked.  
    Queue<hb_queue_data> HoldbackQueue = new PriorityQueue<>(7, idComparator);
    //    Hashtable<String, ArrayList<Integer>> all_props2 = new Hashtable<String, ArrayList<Integer>>();
    Hashtable<String, HashMap<Integer,Integer>> all_props = new Hashtable<String, HashMap<Integer,Integer>>() ;
    Queue<String> fifo_queue = new LinkedList<String>();

    public static Comparator<hb_queue_data> idComparator = new Comparator<hb_queue_data>(){
        @Override
        public int compare(hb_queue_data h1, hb_queue_data h2) {
            int i = Integer.compare(h1.getSeq(), h2.getSeq());
            if (i != 0) return i;

            int i1 = Integer.compare(h1.getAvdNo(), h2.getAvdNo());
            if (i1 != 0) return i1;
            return i1;
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        final Button send_button = (Button)findViewById(R.id.button4);
        final EditText editText = (EditText) findViewById(R.id.editText1);

        editText.setOnKeyListener(new OnKeyListener(){
            @Override
            public boolean onKey(View v, int keyCode, KeyEvent event) {
                if ((event.getAction() == KeyEvent.ACTION_DOWN) &&
                        (keyCode == KeyEvent.KEYCODE_ENTER)) {
                    String msg = editText.getText().toString() + "\n";
//                    fifo_queue.add(msg);
//                    System.out.println("Contents of fifo queue after adding new message(under key li):");
//
//                    for(String mesg : fifo_queue){
//                        System.out.println(mesg+" ");
//                    }
                    editText.setText("");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,"hi", myPort);
                    return true;
                }
                return false;
            }
        });
        send_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.d(TAG, "send button click listener called");
                String msg = editText.getText().toString();

                System.out.println(msg + "-time-"+ System.currentTimeMillis()+"-avd-"+myPort);
//                fifo_queue.add(msg);
//                System.out.println("Contents of fifo queue after adding new message(under send but):");
//
//                for(String mesg : fifo_queue){
//                    System.out.print(mesg+" ");
//                }

                Log.d(TAG,msg+"--Click");
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,"hi", myPort);
            }
        });

    }

    private class ServerTask extends AsyncTask<ServerSocket, ServerTask.Wrapper, Void> {
//    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        public class Wrapper{
            public Socket clientSocket;
            public String message;

            public Wrapper(Socket clientSocket, String message){
                this.clientSocket = clientSocket;
                this.message = message;
            }
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket2 = sockets[0];
            Socket clientSocket = null;
            BufferedReader in = null;
            String message = null;


            try{
                while (true){
                    Log.e(TAG, "waiting for client");
                    clientSocket = serverSocket2.accept();
                    Log.d(TAG, "Server was able to connect to client to receive message");
                    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    message = in.readLine();
                    System.out.println(message);
                    Wrapper wrapper_object = new Wrapper(clientSocket,message);
//                    System.out.println("Calling publish progress");
                    publishProgress(wrapper_object);
//                    publishProgress("hi");
                }
            }catch (Exception e) {
                Log.e(TAG, "Error with client connection...");
            }finally {
                try {
                    serverSocket.close();
                    clientSocket.close();

                } catch (Exception e) {
                    Log.e(TAG, "error closing serverSocket/clientSocket");
                }
                return null;
            }

        }

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        private void displayHbQueue(){

            Iterator<hb_queue_data> itr0 = HoldbackQueue.iterator();

            while(itr0.hasNext()){

                hb_queue_data hb_mesg = itr0.next();
                if(hb_mesg == null) break;
                System.out.println("Seqno:"+hb_mesg.getSeq()+"DelStatus:"+hb_mesg.getDelStatus()
                        +"Message:"+hb_mesg.getMesg()+"AvdNo:"+hb_mesg.getAvdNo());
            }

        }


        protected  void onProgressUpdate(Wrapper... params){
            Wrapper wrapper_object = params[0];
            
//            System.out.println("In progress update");
            String strReceived = wrapper_object.message;
            Socket clientSocket = wrapper_object.clientSocket;
            String [] data_received = new String [2];

            data_received = strReceived.split(" ");
//            System.out.println("message received:"+strReceived);
//            System.out.println("length of data_received:"+data_received.length);
            System.out.println("data_received:"+data_received[0]+" "+data_received[1]+" "+data_received[2]);
            //Above line is hadcoded because all message types contain 3 sub words.
            if(data_received[0].equals("1")) {
                new NewMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,wrapper_object);// TODO: 3/25/17 redundant parameter
                return;
            }
            else if(data_received[0].equals("3")) {
                new Agreement().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,wrapper_object);
                return;
            }
        }
    }
    //do in background ... on progress ... post ... parameters
    private class NewMessage extends AsyncTask<ServerTask.Wrapper, String, Void> {


        @Override
        protected Void doInBackground(ServerTask.Wrapper... params) {
            try {
                ServerTask.Wrapper wrapper_object = params[0];
                String strReceived = wrapper_object.message;
                Socket clientSocket = wrapper_object.clientSocket;
                String[] data_received = new String[2];
                data_received = strReceived.split(" ");

                System.out.println("Before pqg:" + pqg);
                System.out.println("Before aqg:" + aqg);
                pqg = Math.max(pqg, aqg) + 1;
                System.out.println("After modifying pqg = max(pqg,aqg)+1 :" + pqg);

                HoldbackQueue.add(new hb_queue_data(pqg, "u", data_received[2], Integer.parseInt(data_received[1])));
                Log.d(TAG, "entered into hb queue");

                displayHbQueue();

                PrintWriter out = null;
                out = new PrintWriter(clientSocket.getOutputStream(), true);


                //when message received. server needs to send pqg and myport num i.e. avd info  & mesg itself .
                String ack_to_client = null;
                ack_to_client = pqg + " " + myPort + " " + data_received[2];
                out.println(ack_to_client);

                //Waiting for the ack to be delivered before the socket is closed.
                BufferedReader in_temp = null;
                in_temp = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String ack_to_ack_temp = in_temp.readLine();
                System.out.println("Ack from client to the ack sent:" + ack_to_ack_temp);

            } catch(Exception e){
                Log.e(TAG, "error closing serverSocket/clientSocket");

            }
            return null;
        }


        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        private void displayHbQueue(){

            Iterator<hb_queue_data> itr0 = HoldbackQueue.iterator();

            while(itr0.hasNext()){

                hb_queue_data hb_mesg = itr0.next();
                if(hb_mesg == null) break;
                System.out.println("Seqno:"+hb_mesg.getSeq()+"DelStatus:"+hb_mesg.getDelStatus()
                        +"Message:"+hb_mesg.getMesg()+"AvdNo:"+hb_mesg.getAvdNo());
            }

        }



    }

    private class Agreement extends AsyncTask<ServerTask.Wrapper, String, Void> {

        @Override
        protected Void doInBackground(ServerTask.Wrapper... params) {

            try {
                ServerTask.Wrapper wrapper_object = params[0];
                String strReceived = wrapper_object.message;
                Socket clientSocket = wrapper_object.clientSocket;
                String[] data_received = new String[2];
                data_received = strReceived.split(" ");

                System.out.println("Receiving a sent by client i.e. message type 3");
                System.out.println("For Mesg:" + data_received[1] + ", corresponding a:" + data_received[2]);

                PrintWriter out = null;
                out = new PrintWriter(clientSocket.getOutputStream(), true);

                String ack_to_a_to_client = myPort;
                //CLient has sent "a" .. need to acknowledge so that it can close socket and multicast to others.
                out.println(ack_to_a_to_client);

                Integer received_a = Integer.parseInt(data_received[2]);
                aqg = Math.max(received_a, aqg); //aqg needs to be global .. "a" doesnt.

                //below code modifies holdback queue.
                Iterator<hb_queue_data> itr = HoldbackQueue.iterator();
                String its_mesg = null;
                int its_avd_no = 0;
                String its_del_status = null;
                int its_seq_no = 0;
                System.out.println("Modifying hold back queue");
                while (itr.hasNext()) {
                    hb_queue_data element = itr.next();
//                    System.out.println("element.getMesg():" + element.getMesg());
//                    System.out.println("mesg_and_a_from_client_split[0]" + data_received[1]);
//                    System.out.println(element.getMesg().toString().equals(data_received[1].toString()));

                    if (element.getMesg().toString().equals(data_received[1].toString())) {
                        its_mesg = element.getMesg();
                        its_avd_no = element.getAvdNo();
                        its_del_status = "d";
                        its_seq_no = element.getSeq();
                        System.out.println("Before removing content: Seq:" + its_seq_no + "mesg:" + its_mesg + " del_status:" + element.getDelStatus()
                                + "avd#:" + its_avd_no);
                        itr.remove();
                    }
                }
//                System.out.println("Adding modified data to holdbackqueue");


                HoldbackQueue.add(new hb_queue_data(received_a, its_del_status, its_mesg, its_avd_no));

                System.out.println("Displaying modified Holdbackqueue");
                displayHbQueue();

//                System.out.println("peeking the hb queue to check if head is deliverable");
//                System.out.println(HoldbackQueue.peek().getDelStatus());

                // TODO: 3/25/17 check if while or if
                while ((!HoldbackQueue.isEmpty()) && HoldbackQueue.peek().getDelStatus().equals("d") ) {
                    System.out.println("Head of holdbackqueue is deliverable. Polling and sending to content provider.");
                    String to_content_provider = HoldbackQueue.poll().getMesg();
                    publishProgress(to_content_provider);
                }



            } catch (Exception e){
                e.printStackTrace();
            }

            return null;

        }

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        private void displayHbQueue(){

            Iterator<hb_queue_data> itr0 = HoldbackQueue.iterator();

            while(itr0.hasNext()){

                hb_queue_data hb_mesg = itr0.next();
                if(hb_mesg == null) break;
                System.out.println("Seqno:"+hb_mesg.getSeq()+"DelStatus:"+hb_mesg.getDelStatus()
                        +"Message:"+hb_mesg.getMesg()+"AvdNo:"+hb_mesg.getAvdNo());
            }

        }

        protected void onProgressUpdate(String...strings) {

            String strReceived = strings[0].trim();
            Log.d(TAG,strReceived+"<- message received from server in progress update");
            final ContentResolver mContentResolver;
            final Uri mUri;
            final ContentValues cv = new ContentValues();

            mContentResolver = getContentResolver();
            mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
            cv.put("key", Integer.toString(seq_num++));
            cv.put("value", strReceived);

            try {
//                Log.d(TAG,"calling insert from progress update");
                mContentResolver.insert(mUri, cv);
//                Log.d(TAG,"return from insert");
//                Log.d(TAG,"calling query from progress update");

                Cursor resultCursor = mContentResolver.query(mUri, null, "0", null, null);
//                Log.d(TAG,"return from query ");

            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }


            return;

        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Log.d(TAG,msgs[0]+msgs[1]+"<-- received from listener in client.");
                for (int i=0; i<remote_ports.length;i++) {
                    System.out.println("entered for loop");
                    String remotePort = remote_ports[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(remotePort));
                    Log.d(TAG, remote_ports[i]+"<---This receiver opened new socket to connect to sender.");
                    String type_of_message_1 = "1 ";
                    String msgToSend = type_of_message_1.concat(msgs[2].toString().concat(" ").concat(msgs[0]));//2 -- port ... 0 -- message typed
                    Log.d(TAG,msgToSend+" <-- Sending this from client to server");

                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgToSend);
                    Log.d(TAG,msgToSend+" <-- Sent this from client to server");
                    String pqg_myport_msg = null;
                    BufferedReader pqg_myport_msg_in_ack = null;
                    try { pqg_myport_msg_in_ack = new BufferedReader(new InputStreamReader(socket.getInputStream()));}
                    catch (Exception e) {Log.e(TAG, "Error in buffered reader .. while waiting for ack");}

                    try {pqg_myport_msg = pqg_myport_msg_in_ack.readLine();}
                    catch (Exception e) {Log.e(TAG, "Error in in.readline... while waiting for ack");}
                    // sending ack to ack .. so that the client can close the socket
                    PrintWriter out_temp = new PrintWriter(socket.getOutputStream(), true);
                    out_temp.println("2 ack received");//1 space between words...message of type 2

                    //If server is busy with other task .. thread sleep will ensure client stays open.
                    try {Thread.sleep(600);}
                    catch (InterruptedException e) {e.printStackTrace();}

                    System.out.println("Closing socket on client side");
                    socket.close(); // message sent and ack received .. so closing socket

                    String[] ack_data = null;
                    ack_data = pqg_myport_msg.split(" ");
                    Log.e(TAG, "pqg from server in ack:"+ack_data[0]);
                    Log.e(TAG, "myport from server in ack:"+ack_data[1]);
                    Log.e(TAG, "mesg from server in ack:"+ack_data[2]);

                    //this below temp is for each message... message -- avd -- seq. temporarily
                    // stores info related to the received pgq .. i.e... on every receipt of server ack.
                    if(!all_props.containsKey(ack_data[2])){
                        HashMap<Integer,Integer> temp = new HashMap<Integer,Integer>();
                        temp.put(Integer.parseInt(ack_data[1]), Integer.parseInt(ack_data[0]));
                        all_props.put(ack_data[2],temp);
                    }
                    else{
                        HashMap<Integer,Integer> temp2 = all_props.get(ack_data[2]);
                        if(!temp2.containsKey(Integer.parseInt(ack_data[1]))){
                            temp2.put(Integer.parseInt(ack_data[1]), Integer.parseInt(ack_data[0]));
                        }
                    }


                    //****displaying the contents of all_props list
                    Set<String> key_set = all_props.keySet();
                    for (String indi_mesg : key_set){
                        System.out.println("displaying the contents of all_props list");
                        System.out.println("*******message:"+indi_mesg+"***************");
                        HashMap<Integer,Integer> temp3 = all_props.get(indi_mesg);
                        Set<Integer> key_set2 = temp3.keySet();
                        for (Integer j : key_set2){
                            System.out.println("avd:"+j);
                            System.out.println("sequence no:"+temp3.get(j));
                        }
                    }


                    Integer maxx = 0;
                    Integer all_props_dict_size = all_props.get(ack_data[2]).size();
                    System.out.println("all_props dict size for:"+ack_data[2]+"is"+all_props.get(ack_data[2]).size());
                    Integer remote_ports_length =  (remote_ports.length);
                    System.out.println("remote portslength: "+remote_ports.length);
                    System.out.println("dict size equals ports length?"+all_props_dict_size.equals(remote_ports_length));
                    //all acks have been received for a message. Hence
                    if(all_props_dict_size.equals(remote_ports_length)) {
                        HashMap<Integer, Integer> temp_to_find_max = all_props.get(ack_data[2]);
                        Set<Integer> key_set_to_find_max = temp_to_find_max.keySet();
                        for (Integer avds : key_set_to_find_max) {
                            if (temp_to_find_max.get(avds) > maxx) {
                                maxx = temp_to_find_max.get(avds);
                            }
                        }
                        a = maxx;
                        String multicast_mesg_and_a = ack_data[2]+" "+a;
                        System.out.println("Sending mesg and a from cli to serv");
                        System.out.println("testing the data being sent; mesg "+multicast_mesg_and_a.split(" ")[0]+
                                "a:"+multicast_mesg_and_a.split(" ")[1]);

                        //This socket is dedicated to sending "a" and receiving its ack.
                        //// TODO: 3/24/17 : Need to check if this needs to be done for all servers.
                        for (int k=0; k<remote_ports.length;k++) {

//                            String remotePort = remote_ports[i];
                            Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_ports[k]));
                            PrintWriter out2 = new PrintWriter(socket2.getOutputStream(), true);
                            BufferedReader in2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
                            String type_of_message_3 = "3 ";
                            out2.println(type_of_message_3+multicast_mesg_and_a);
                            System.out.println("multicasted mesg and a to: " + remote_ports[k]);
                            String ack_to_a = in2.readLine();
                            System.out.println("received ack to a from: " + ack_to_a);
                            socket2.close();
                        }
                    }

                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}

//qyeDYxtioCiY2eVNkdQDQOo3kM505TI
