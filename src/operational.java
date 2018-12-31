import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class operational {

    private static newT m1;
    private static long c;
    private static long time;
    private static final int THREADN=1999;
    private static ExecutorService threadPool,addHelpers,addHelpers2;
    private static DB db,db2;
    private static HTreeMap<Long,String> hm;
    private static HTreeMap<String,String> hashListMap;
    private static int[] tester=new int[3000];
    private static int counter=0;
    private static BufferedWriter br;

    static{
        db = DBMaker.fileDB(new File("/Volumes/Hackintosh/Users/ashay/Desktop/mainDb")).fileMmapEnable().fileMmapPreclearDisable().concurrencyScale(THREADN).make();
        db2 = DBMaker.fileDB(new File("/Volumes/Hackintosh/Users/ashay/Desktop/hListDb")).fileMmapEnable().fileMmapPreclearDisable().concurrencyScale(THREADN).make();
        hm=db.hashMap("test").keySerializer(Serializer.LONG).valueSerializer(Serializer.STRING).counterEnable().createOrOpen();
        hashListMap=db2.hashMap("hashListMap").keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).counterEnable().createOrOpen();
        threadPool = Executors.newFixedThreadPool(THREADN);
        addHelpers = Executors.newFixedThreadPool(THREADN);
        addHelpers2 = Executors.newFixedThreadPool(THREADN);
    }

    public static void main(String[] args){
        m1=new operational().new newT();
        m1.start();
        try{
            m1.join();
        }
        catch(Exception e){e.printStackTrace();}
        finally{
            if(!db.isClosed())
                db.close();
            if(!db2.isClosed())
                db2.close();
        }
        System.out.println("Time taken\t"+(System.currentTimeMillis()-time)/1000);
    }

    private void findCode(){

        File fl;
        LineIterator li=null;
        String line="";
        long time1=System.currentTimeMillis(),time2;
        Runnable r1;
        c=0;
        try{

        br=new BufferedWriter(new FileWriter("/Volumes/Storage/Dumps/Linkedin/final2.txt"));
        fl=new File("/Volumes/Storage/Dumps/Linkedin/creds.txt");
        li = FileUtils.lineIterator(fl);
        time=System.currentTimeMillis();
        while(li.hasNext()) {
            line = li.nextLine();
            c++;

            if(c%1_000_000==0)
            {
                time2=time1;
                time1=System.currentTimeMillis();
                System.out.println(c+"\t"+((ThreadPoolExecutor)threadPool).getActiveCount()+"\t"+((ThreadPoolExecutor)threadPool).getQueue().size()+"\t"+(time1-time2)/1000);

            }

            if(c%1000==0)
            {
                ((ThreadPoolExecutor) threadPool).purge();
            }
            while(((ThreadPoolExecutor) threadPool).getQueue().size()>100000)
                Thread.sleep(10);

            r1 = new MyRunnable(line);
            threadPool.submit(r1);
        }
        }
        catch (Exception e)
        {
        e.printStackTrace();
        System.out.println("Crash: "+line);
        }
        finally{
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                br.close();
                li.close();
            }
            catch (Exception e)
            {
                System.out.println("why tf would this crash");
            }
        }



    }

    private static String findHash(String code){
        String hash;
        String pass="";
        try{
            hash=hm.get(Long.parseLong(code));
            if(hash!=null)
                return findPass(hash);
            else
                return null;

        }
        catch(Exception e){
            e.printStackTrace();
            System.out.println("Number 4");
        }
        return pass;
    }

    private static String findPass(String hash){
        String pass=null;
        try{
            pass=hashListMap.get(hash);
        }
        catch(Exception e){
            e.printStackTrace();
            System.out.println("Number 5"+hash);
            System.out.println("Number 5"+hashListMap.get(hash));
        }
        return pass;
    }

    private static void createHM(){
        StringBuilder line=new StringBuilder("25524184:54da54e70572489589845f8907545773b5d41abb");
        addHelper3 adder=new operational().new addHelper3(line);
        addHelpers.execute(adder);
        addHelpers.shutdown();
        try {
            addHelpers.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }

        db.commit();
    }
    private static void createHashList(){
        String line="54da54e70572489589845f8907545773b5d41abb:passw123";
        addHelper2 adder=new operational().new addHelper2(line,line.indexOf(":"));
        addHelpers2.execute(adder);
        addHelpers2.shutdown();
        try {
            addHelpers2.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }

        db2.commit();
    }



class MyRunnable implements Runnable {

    String code,line,email,pass;

    private MyRunnable(String parameter) {
        this.line=parameter;
        code=line.substring(line.indexOf(":")+1);
        email=line.substring(0,line.indexOf(":"));
    }

    public void run() {
        pass=findHash(code);
        if(pass!=null) {
            //System.out.println("Email:" + email + "\tCode:" + code + "\tPassword:" + pass);
            try {
                br.write(email + ":" + pass + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}


    class addHelper3 implements Runnable{
        StringBuilder line;
        int loc,loc2;
        private addHelper3(StringBuilder parameter){
            line=parameter;
            line.trimToSize();
            loc=line.indexOf(":");
            loc2=line.length();
        }
        public void run(){
            hm.put(Long.parseLong(line.substring(0,loc)),line.substring(loc+1,loc2));
            line=null;
            c--;
            db.commit();
        }
    }

    class addHelper2 implements Runnable{
        String parameter,first;
        int loc;
        private addHelper2(String parameter,int loc){
            this.parameter=parameter;
            this.loc=loc;
        }
        public void run(){
            try {
                hashListMap.put(parameter.substring(0,loc), parameter.substring(loc + 1));
            }
            catch (Exception e){
                e.printStackTrace();
            }
            parameter=null;
            c--;
        }
    }


    class newT extends Thread{
        public void run(){
            try{
                new operational().findCode();
            }
            catch(Exception e){
                e.printStackTrace();
                System.out.println("THIS just crashed the entire system");
            }
        }

    }

}
