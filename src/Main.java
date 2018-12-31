import org.apache.commons.io.*;
import org.mapdb.*;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.lang.String.*;

public class Main{

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

static{
        db = DBMaker.fileDB(new File("/Volumes/Storage/mainDb")).fileMmapEnable().fileMmapPreclearDisable().concurrencyScale(THREADN).make();
        db2 = DBMaker.fileDB(new File("/Volumes/Storage/hListDb-extra")).fileMmapEnable().fileMmapPreclearDisable().concurrencyScale(THREADN).make();
        hm=db.hashMap("test").keySerializer(Serializer.LONG).valueSerializer(Serializer.STRING).counterEnable().create();
        hashListMap=db2.hashMap("hashListMap").keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).counterEnable().create();
        threadPool = Executors.newFixedThreadPool(1);
        addHelpers = Executors.newFixedThreadPool(THREADN);
        addHelpers2 = Executors.newFixedThreadPool(THREADN);
        }

public static void main(String[] args){
        m1=new Main().new newT();
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
        LineIterator li;
        String line="";
        createHM();
        System.gc();
        //createHashList();
        /*
        OPERATIONAL CODE

        try{

        fl=new File("F:/Dumps/Linkedin/creds.txt");
        li = FileUtils.lineIterator(fl);
        time=System.currentTimeMillis();
        while(li.hasNext())
        {
        line=li.nextLine();
        c++;
        Runnable r1=new MyRunnable(line);
        threadPool.submit(r1);

        while(c==THREADN)
        {
        try{
        Thread.sleep(1000000000);
        }
        catch(InterruptedException e){
            e.printStackTrace();
        }
        }
        }

        li.close();
        threadPool.shutdown();
        }
        catch (Exception e)
        {
        e.printStackTrace();
        System.out.println("Crash: "+line);
        System.exit(0);
        }



        */
        }

private static String findHash(String code){
        String hash;
        String pass="";
        try{
        hash=hm.get(Long.parseLong(code));
        pass=findPass(hash);
        }
        catch(Exception e){
        e.printStackTrace();
        System.out.println("Number 4");
        }
        return pass;
        }

private static String findPass(String hash){
        String pass="";
        try{
        pass=hashListMap.get(hash);
        }
        catch(Exception e){
        e.printStackTrace();
        System.out.println("Number 5");
        }
        return pass;
        }
private static void createHM(){
        c=0;
        StringBuilder line = null;
        File[] tot;
        long time1=System.currentTimeMillis(),time2;
        Main m=new Main();
        counter=0;
        addHelper3 adder;
        LineIterator brfile;
        try{
            tot=new File("/Volumes/Storage/Dumps/Linkedin/Data").listFiles();
            if(tot==null)
                throw new Exception("Folder empty");
            for(File f:tot)
            {
                if(f.isFile()&&!f.getName().equals("1.sql.txt")&&!f.getName().equals(".DS_Store"))//&&(f.getName().startsWith("1")||f.getName().startsWith("2"))
                {
                System.out.println(f.getName());
                brfile=FileUtils.lineIterator(f);

                while(brfile.hasNext())
                {
                line=new StringBuilder(brfile.nextLine());
                c++;
                counter++;
                if(line.indexOf("null")>-1||line.indexOf("xxx")==(line.length()-3)||line.indexOf("@")>-1||line.indexOf("->")>-1){
                    continue;
                }
//                if(c>THREADN){
//                    try{
//                        Thread.sleep(10);
//                    }
//                    catch(InterruptedException e){
//                    }
//                }
                while(((ThreadPoolExecutor) addHelpers).getQueue().size()>1000000)
                    Thread.sleep(10);
                if(counter%100_000_000==0){
                    System.out.println(counter+"\t");
                    Runnable r=new Runnable() {
                        @Override
                        public void run() {
                            db.commit();
                        }
                    };
                    new Thread(r).start();
                }

                if(counter%100000==0) {
                    time2=time1;
                    time1=System.currentTimeMillis();
                    System.out.println(counter + "\t" + ((ThreadPoolExecutor) addHelpers).getActiveCount() + "\t" + ((ThreadPoolExecutor) addHelpers).getQueue().size()+"\t"+(time1-time2));
                }

                if(counter%1_000_000==0)
                    ((ThreadPoolExecutor) addHelpers).purge();
                adder = m.new addHelper3(line);
                addHelpers.execute(adder);

                }

                brfile.close();
                System.gc();
                }
            }
            addHelpers.shutdown();
            try {
                addHelpers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
            }
            finally {
                db.commit();
                db.close();
            }
        }
        catch(NullPointerException e){
            e.printStackTrace();
            System.out.println("here2"+line);
        }
        catch(Exception e){
        e.printStackTrace();
        System.out.println("Number 1:\t");
        }
        }
private static void createHashList(){

        c=0;
        counter=0;
        String line;
        Main m=new Main();
        addHelper2 adder;
        System.out.println("Creating hashmap");
        try{
        File f=new File("/Volumes/Storage/Dumps/Linkedin/hashlist.txt");

        LineIterator brfile=FileUtils.lineIterator(f);


        while(brfile.hasNext())
        {

        line=brfile.nextLine();

//        if(c>THREADN*10){
//            try{
//                Thread.sleep(10);
//            }
//            catch(InterruptedException e){
//
//            }
//        }

        while(((ThreadPoolExecutor) addHelpers2).getQueue().size()>1000000)
            Thread.sleep(10);
        c++;
        counter++;

        adder=m.new addHelper2(line);
        addHelpers2.execute(adder);

        if(counter%100000==0)
            System.out.println(counter+"\t"+((ThreadPoolExecutor) addHelpers2).getActiveCount()+"\t"+((ThreadPoolExecutor) addHelpers2).getQueue().size());
        }

        brfile.close();
        System.gc();
        addHelpers2.shutdown();
        try {
        addHelpers2.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {

        }
        }
        catch(Exception e){
        e.printStackTrace();
        System.out.println("Number 2:\t");
        }
        finally {
            db2.commit();
            db2.close();
        }
        }


/*
class MyRunnable implements Runnable {

    String code,line,email,pass;

    private MyRunnable(String parameter) {
        this.line=parameter;
        code=line.substring(line.indexOf(":")+1);
        email=line.substring(0,line.indexOf(":"));
    }

    public void run() {
        pass=findHash(code);
        System.out.print("Email:"+email+"\tCode:"+code+"\tPassword:"+pass);
        c--;
        m1.interrupt();
    }
}*/


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
        try {
            hm.put(Long.parseLong(line.substring(0, loc)), line.substring(loc + 1, loc2));
        }
        catch (Exception e){}
        line=null;
        c--;
    }
}

class addHelper2 implements Runnable{
    String parameter,first;
    int loc=1;
    private addHelper2(String parameter){
        this.parameter=parameter;
        loc=parameter.indexOf(':');
        first=parameter.substring(0,loc);
    }
    public void run(){
        try {
            hashListMap.put(first, parameter.substring(loc + 1));
        }
        catch (Exception e){}
        parameter=null;
        c--;
    }
}


class newT extends Thread{
    public void run(){
        try{
            new Main().findCode();
        }
        catch(Exception e){
            e.printStackTrace();
            System.out.println("THIS just crashed the entire system");
        }
    }

}

}
