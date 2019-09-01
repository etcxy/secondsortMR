package com.etcxy.servicesort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComboKey implements WritableComparable<ComboKey> {

    private int userID;
    private String servername;
    private int charge;

    public ComboKey() {
    }

    public ComboKey(int userID, String serverName, int charge) {
        this.userID = userID;
        servername = serverName;
        this.charge = charge;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public String getServerName() {
        return servername;
    }

    public void setServerName(String serverName) {
        servername = serverName;
    }

    public int getCharge() {
        return charge;
    }

    public void setCharge(int charge) {
        this.charge = charge;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(userID);
        out.writeUTF(servername);
        out.writeInt(charge);
    }


    public void readFields(DataInput in) throws IOException {
        userID = in.readInt();
        servername = in.readUTF();
        charge = in.readInt();

    }

    @Override
    public String toString() {
        return "ComboKey{" +
                "userID=" + userID +
                ", servername='" + servername + '\'' +
                ", charge=" + charge +
                '}';
    }

    public int compareTo(ComboKey key) {
        if (key.userID == this.userID) {
            return key.charge - this.charge;
        }
        return key.userID - this.userID;
    }
}
