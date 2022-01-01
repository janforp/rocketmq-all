package org.apache.rocketmq.store;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class StoreUtil {

    public static final long TOTAL_PHYSICAL_MEMORY_SIZE = getTotalPhysicalMemorySize();

    @SuppressWarnings("restriction")
    public static long getTotalPhysicalMemorySize() {
        long physicalTotal = 1024 * 1024 * 1024 * 24L;
        OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
        if (osmxb instanceof com.sun.management.OperatingSystemMXBean) {
            physicalTotal = ((com.sun.management.OperatingSystemMXBean) osmxb).getTotalPhysicalMemorySize();
        }
        return physicalTotal;
    }

    public static void main(String[] args) {
        OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
        System.out.println(osmxb.getObjectName());
        System.out.println(osmxb.getName());
        System.out.println(osmxb.getAvailableProcessors());
        System.out.println(osmxb.getArch());
        System.out.println(osmxb.getVersion());
        System.out.println(osmxb.getSystemLoadAverage());
        if (osmxb instanceof com.sun.management.OperatingSystemMXBean) {
            com.sun.management.OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) osmxb;
            System.out.println(bean.getCommittedVirtualMemorySize());
            System.out.println(bean.getTotalSwapSpaceSize());
            System.out.println(bean.getFreeSwapSpaceSize());
            System.out.println(bean.getProcessCpuTime());
            System.out.println(bean.getFreePhysicalMemorySize());
            System.out.println(bean.getTotalPhysicalMemorySize());
            System.out.println(bean.getSystemCpuLoad());
            System.out.println(bean.getProcessCpuLoad());
        }

    }
}