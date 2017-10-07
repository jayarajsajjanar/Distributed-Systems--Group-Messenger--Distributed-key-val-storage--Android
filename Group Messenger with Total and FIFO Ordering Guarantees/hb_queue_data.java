package edu.buffalo.cse.cse486586.groupmessenger2;

/**
 * Created by j on 3/24/17.
 */

public class hb_queue_data {

    private int seq_no;
    private String del_status;
    private String mesg;
    private int avd_no;

    public hb_queue_data(int seq_no, String del_status, String mesg, int avd_no){

        this.seq_no = seq_no;
        this.del_status = del_status;
        this.mesg = mesg;
        this.avd_no = avd_no;

    }

    public int getSeq() {
        return seq_no;
    }

    public String getDelStatus() {
        return del_status;
    }

    public String getMesg() {
        return mesg;
    }

    public int getAvdNo() {
        return avd_no;
    }

}
