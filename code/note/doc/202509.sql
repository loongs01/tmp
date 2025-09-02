在 Flink 中，长窗口（如数小时的滑动/滚动窗口）或复杂聚合（如多维度分组聚合） 可能导致 Checkpoint 耗时过长、状态过大，甚至阻塞作业。通过拆分任务（如分层聚合、预聚合、状态拆分）可以显著优化性能。以下是具体案例和实现方法：


IntelliJ IDEA：File → Invalidate Caches / Restart... → 选择 Invalidate and Restart
  
Command (m for help): p

Disk /dev/sda: 64.4 GB, 64424509440 bytes, 125829120 sectors
Units = sectors of 1 * 512 = 512 bytes
Sector size (logical/physical): 512 bytes / 512 bytes
I/O size (minimum/optimal): 512 bytes / 512 bytes
Disk label type: dos
Disk identifier: 0x000a0d34

   Device Boot      Start         End      Blocks   Id  System
/dev/sda1   *        2048      616447      307200   83  Linux
/dev/sda2          616448     4810751     2097152   82  Linux swap / Solaris
/dev/sda3         4810752    41943039    18566144   83  Linux
