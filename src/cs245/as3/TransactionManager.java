package cs245.as3;

import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 * <p>
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 * <p>
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 * <p>
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager implements Constant {
    class WritesetEntry {
        public long key;
        public byte[] value;

        public WritesetEntry(long key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * Holds the latest value for each key.
     */
    private HashMap<Long, TaggedValue> latestValues;
    /**
     * Hold on to writesets until commit.
     */
    private HashMap<Long, ArrayList<WritesetEntry>> writesets;

    /**
     * 存储事务id和对应的日志列表
     */
    private HashMap<Long, ArrayList<Log>> transactions;

    /**
     * 存储日志的位置，用来设置checkpoint
     */
    private PriorityQueue<Long> queue;

    // 日志管理器
    private LogManager lm;
    // 模拟的磁盘管理器
    private StorageManager sm;


    public TransactionManager() {
        writesets = new HashMap<>();
        //see initAndRecover
        latestValues = null;
        transactions = new HashMap<>();
        queue = new PriorityQueue<>();
    }

    /**
     * Prepare the transaction manager to serve operations.
     * At this time you should detect whether the StorageManager is inconsistent and recover it.
     */
    public void initAndRecover(StorageManager sm, LogManager lm) {
        this.sm = sm;
        this.lm = lm;
        latestValues = sm.readStoredTable();
        // 日志列表、存储的日志中只有写入日志和commit日志两种
        ArrayList<Log> logs = new ArrayList<>();

        // 已经提交了的事务ids
        HashSet<Long> committedIds = new HashSet<>();
        // 记录日志的位置
        ArrayList<Integer> commitIdList = new ArrayList<>();
        // 从日志开头到结尾读取磁盘中的日志
        int start = lm.getLogTruncationOffset();
        int end = lm.getLogEndOffset();
        for (int i = start; i < end; ) {
            // 读取16字节的数据、分别解析出来为txid、type、size
            byte[] bytes = lm.readLogRecord(i, 16);
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            long txId = byteBuffer.getLong();
            int type = byteBuffer.getInt();
            int size = byteBuffer.getInt();
            // 由读取到的size读取完整的log数据进行转化
            ByteBuffer buffer = ByteBuffer.allocate(size);
            for (int j = 0; j < size; j += LOG_MAX_LENGTH) {
                int len = Math.min(size - j, LOG_MAX_LENGTH);
                buffer.put(lm.readLogRecord(i + j, len));
            }
            Log log = LogUtil.changeToLogRecord(buffer.array());
            logs.add(log);
            if (log.getType() == 2) {
                committedIds.add(log.getTxId());
            }
            i += log.size();
            commitIdList.add(i);
        }

        // 对提交的事务的日志进行重做，并且做持久化
        Iterator<Integer> iterator = commitIdList.iterator();
        for (Log lg : logs) {
            if (committedIds.contains(lg.getTxId()) && lg.getType() == 1) {
                long tag = iterator.next();
                queue.add(tag);
                latestValues.put(lg.getKey(), new TaggedValue(tag, lg.getValue()));
                sm.queueWrite(lg.getKey(), tag, lg.getValue());
            }
        }
    }

    /**
     * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
     */
    public void start(long txID) {
        // TODO: Not implemented for non-durable transactions, you should implement this
        // 为了简化操作start不生产日志
        transactions.put(txID, new ArrayList<>());
    }

    /**
     * Returns the latest committed value for a key by any transaction.
     */
    public byte[] read(long txID, long key) {
        TaggedValue taggedValue = latestValues.get(key);
        return taggedValue == null ? null : taggedValue.value;
    }

    /**
     * Indicates a write to the database. Note that such writes should not be visible to read()
     * calls until the transaction making the write commits. For simplicity, we will not make reads
     * to this same key from txID itself after we make a write to the key.
     */
    public void write(long txID, long key, byte[] value) {
        ArrayList<WritesetEntry> writeset = writesets.get(txID);
        if (writeset == null) {
            writeset = new ArrayList<>();
            writesets.put(txID, writeset);
        }
        writeset.add(new WritesetEntry(key, value));
        // 生成一条写日志
        Log writeLog = new Log(txID, 1, key, value);
        transactions.get(txID).add(writeLog);
    }

    /**
     * Commits a transaction, and makes its writes visible to subsequent read operations.\
     */
    public void commit(long txID) {
        // 创建提交日志
        Log commitLog = new Log(txID, 2, -1, null);
        transactions.get(txID).add(commitLog);

        // 先将日志进行持久化
        // 存储键和日志的大小
        HashMap<Long, Long> keyPos = new HashMap<>();
        for (Log lg : transactions.get(txID)) {
            long pos = 0;
            // 将日志转化为字节数组存放到磁盘
            byte[] bytes = LogUtil.changeToByte(lg);
            int size = bytes.length;
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            // 对长日志进行处理
            for (int i = 0; i < size; i += LOG_MAX_LENGTH) {
                int len = Math.min(size - i, LOG_MAX_LENGTH);
                byte[] temp = new byte[len];
                byteBuffer.get(temp, 0, len);
                pos = lm.appendLogRecord(temp);
            }
            // 只有写入日志才有key
            if (lg.getType() == 1) {
                keyPos.put(lg.getKey(), pos);
            }
        }
        // 将数据持久化
        ArrayList<WritesetEntry> writeset = writesets.get(txID);
        if (writeset != null) {
            for (WritesetEntry x : writeset) {
                // tag用来记录value存储的日志的位置
                long tag = keyPos.get(x.key);
                // 将数据进行持久化操作
                latestValues.put(x.key, new TaggedValue(tag, x.value));
                // 存储已经进行持久化的日志的位置
                queue.offer(tag);
                sm.queueWrite(x.key, tag, x.value);
            }
            writesets.remove(txID);
        }
    }

    /**
     * Aborts a transaction.
     */
    public void abort(long txID) {
        writesets.remove(txID);
        // 回滚事务不产生日志，直接把内存中的日志记录都给删除掉
        transactions.remove(txID);
    }

    /**
     * The storage manager will call back into this procedure every time a queued write becomes persistent.
     * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
     */
    public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
        // queue中记录的是已经持久化的日志位置信息，所以当他们相遇的时候就可以设置checkpoint
        if (persisted_tag == queue.peek()) {
            lm.setLogTruncationOffset((int) persisted_tag);
        }
        queue.remove(persisted_tag);
    }
}
