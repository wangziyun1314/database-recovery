package cs245.as3;

import java.nio.ByteBuffer;

public class LogUtil {

    /**
     * 将日志转化为字节数组
     * @param log
     * @return
     */
    public static byte[] changeToByte(Log log) {
        // 分配内存
        ByteBuffer buffer = ByteBuffer.allocate(log.size());
        buffer.putLong(log.getTxId());
        buffer.putInt(log.getType());
        buffer.putInt(log.size());
        // 写入日志才由key、value
        if (log.getType() == 1) {
            buffer.putLong(log.getKey());
            buffer.put(log.getValue());
        }
        byte[] result = buffer.array();
        return result;
    }


    /**
     * 将字节数组转化为log
     * @param bytes
     * @return
     */
    public static Log changeToLogRecord(byte[] bytes) {
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        long txId = buff.getLong();
        int type = buff.getInt();
        Log log = new Log(txId, type, -1, null);
        int size = buff.getInt();
        if (log.getType() == 1) {
            long key = buff.getLong();
            log.setKey(key);
            // 减去 txid、type、size、key的大小
            int len = size - 24;
            byte[] temp = new byte[len];
            for (int i = 0; i < len; i++) {
                temp[i] = buff.get();
            }
            log.setValue(temp);
        }
        return log;
    }
}
