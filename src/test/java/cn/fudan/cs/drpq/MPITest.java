package cn.fudan.cs.drpq;

import mpi.Group;
import mpi.Intracomm;
import mpi.MPI;

import java.util.concurrent.TimeUnit;

public class MPITest {
    public static void main(String[] args) throws Exception{
        MPI.Init(args);
//        MPI.COMM_WORLD.group();
//        long startTime = System.currentTimeMillis();

//        workers.Barrier();
        Group group = MPI.COMM_WORLD.Group().Excl(new int[]{0});
//        Intracomm workers = MPI.COMM_WORLD.Create(group);
//        group = Group.Union(MPI.COMM_WORLD.Group(), group);
        Intracomm group_Intracomm = MPI.COMM_WORLD.Create(group);
//        Intracomm.Compare()
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
//        for (int i = 1; i < size; i++) {
//            group;
//        }
        if(rank == 0) {
//            System.out.println(rank + " "+group.Rank());
//            group_Intracomm.Barrier();
//            group_Intracomm.
            for (int i = 1; i < size; i++) {
                MPI.COMM_WORLD.Isend(new int[]{1}, 0, 1, MPI.INT, i, -1);
            }
            System.out.println(rank + " wait barrier");
            MPI.COMM_WORLD.Barrier();
        }else {
//            System.out.println(rank + " " + group.Rank());
            group_Intracomm.Barrier();
            System.out.println(rank + "group barrier ok");
            TimeUnit.SECONDS.sleep(1);
//            System.out.println(rank + " wait barrier");
            MPI.COMM_WORLD.Barrier();
//            System.out.println(MPI.COMM_WORLD.Iprobe(0, -1));
//            TimeUnit.SECONDS.sleep(2);
            int[] a = new int[1];
            MPI.COMM_WORLD.Recv(a, 0, 1, MPI.INT, 0, -1);
            System.out.println(a[0]);
        }
        System.out.println(rank + " finish");
        MPI.Finalize();
    }
}
