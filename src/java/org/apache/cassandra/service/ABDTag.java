package org.apache.cassandra.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ABDTag {
    private int logicalTIme;
    private String writerId ;
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    public ABDTag(){
        this.logicalTIme = 0;
        this.writerId = FBUtilities.getBroadcastAddressAndPort().toString();
    }


    public int getTime(){
        return logicalTIme;
    }

    public String getWriterId() {
        return writerId;
    }

    public ABDTag nextTag(){
        this.logicalTIme++;
        return this;
    }

    public static ByteBuffer serialize(ABDTag tag) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(tag);
            oos.flush();
            bytes = bos.toByteArray();
        } catch (IOException e1){
            logger.info("IOException");
        }

        try {
            if (oos != null) {
                oos.close();
            }
            if (bos != null) {
                bos.close();
            }
        } catch (IOException e1){
            logger.info("IOException");
        }

        return ByteBuffer.wrap(bytes);
    }

    public static ABDTag deserialize(ByteBuffer buf) {
        byte[] bytes = new byte[buf.capacity()];
        buf.get(bytes, 0, bytes.length);

        ABDTag res = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            res = (ABDTag) ois.readObject();
        } catch (ClassNotFoundException e){
            logger.info("ClassNotFoundException");
        } catch (IOException e1){
            logger.info("IOException");
        }

        try {
            if (bis != null)
                bis.close();
            if (ois != null)
                ois.close();
        } catch (IOException e1){
            logger.info("IOException");
        }
        return res;
    }

    public boolean isLarger(ABDTag other){
        if(this.logicalTIme != other.getTime()){
            return this.logicalTIme - other.getTime() > 0;
        } else {
            return this.writerId.compareTo(other.getWriterId()) > 0;
        }
    }

    public String toString() {
        return logicalTIme + " " + writerId;
    }
}
