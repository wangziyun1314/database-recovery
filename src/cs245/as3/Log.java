package cs245.as3;

import java.nio.ByteBuffer;

public class Log implements Constant{


    // 事务id
    private long txId;


    /**
     * 写日志 1
     * 提交日志 2
     * 其他产生日志
     */
    private int type;



    // 日志记录的大小
    private int size;

    // 修改的键值
    private long key;

    // 值
    private byte[] value;

    public Log (long txId, int type, long key, byte[] value) {
        this.txId = txId;
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getTxId() {
        return txId;
    }

    public void setTxId(long txId) {
        this.txId = txId;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    /**
     * 求出日志记录占用的字节数
     * @return
     */
    public int size() {
        size = BASI_LOG_SIZE;
        if (type == 1) {
            size = size + 8 + value.length;
        }
        return size;
    }
}
