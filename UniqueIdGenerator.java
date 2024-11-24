package uniqueidgenerator;

//one machine can generate up to 4M ids per second
public class UniqueIdGenerator {
    private final long epoch = 1704047400000L; // 1 January 2024
    private long lastTimestamp = -1L;
    private final long datacenterIdBits = 5; //32 datacenters
    private final long machineIdBits = 5; //32 machines per DC
    private final long sequenceIdBits = 12; //4096 ids per millis

    private final long machineBitsShift = sequenceIdBits;
    private final long datacenterBitsShift = machineIdBits + sequenceIdBits;
    private final long timestampBitsShift = datacenterIdBits + machineIdBits + sequenceIdBits;

    private final long dataCenterId;
    private final long machineId;

    private long sequence;
    private final long sequenceMask = (1L << sequenceIdBits) - 1;

    public UniqueIdGenerator(long dataCenterId, long machineId) {
        if(dataCenterId < 0 || dataCenterId > (1L << datacenterIdBits) - 1) {
            throw new RuntimeException("Invalid datacenter id. Valid range is 0-" + ((1L << datacenterIdBits) - 1));
        }
        if(machineId < 0 || machineId > (1L << machineIdBits) - 1) {
            throw new RuntimeException("Invalid machine id. Valid range is 0-" + ((1L << machineIdBits) - 1));
        }
        this.dataCenterId = dataCenterId;
        this.machineId = machineId;
    }

    public synchronized long getUniqueId() {
        long currentTime = System.currentTimeMillis();
        if(currentTime < lastTimestamp) {
            throw new RuntimeException("Backward clock is set. Unable to generate unique id");
        }
        if(currentTime == lastTimestamp) {
            sequence = (sequence + 1) & sequenceMask; // seq not going beyond 4095/millis
            if(sequence == 0L) {
                System.out.println("Limit reached for current millisecond. Wait for next milli second");
                while(currentTime == lastTimestamp) {
                    currentTime = System.currentTimeMillis();
                }
            }
        }
        else {
            sequence = 0L; //reset for next milli
        }
        lastTimestamp = currentTime;
        return (currentTime - epoch) << timestampBitsShift |
                (dataCenterId << datacenterBitsShift) |
                (machineId << machineBitsShift) |
                sequence;

    }

}
