package tech.novagen.Hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Created by hubz on 06/12/18.
 */
public class Lanceur {

    private TableName table1 = TableName.valueOf("my_table");
    private byte[] cf_date = Bytes.toBytes("Date");
    private byte[] cf_sport = Bytes.toBytes("Sport");
    private byte[] yy = Bytes.toBytes("yy");
    private byte[] mois = Bytes.toBytes("mois");
    private byte[] sport = Bytes.toBytes("sport");

    public static void main(String[] args) throws IOException, ServiceException {

        Lanceur lanceur = new Lanceur();
//        lanceur.createTable();
//        lanceur.addData("haja");
//        lanceur.getData();
//        lanceur.scanData();
        lanceur.insertionMassive();
    }

    public void addData(String mot) throws IOException, ServiceException {

        byte[] row1 = Bytes.toBytes("2018foott");


        Put p = new Put(row1);
        //TODO avec la méthode addColumn de la classe Put, ajouter des colonnes et des valeurs aux différentes Colonne Family
        p.addColumn(cf_date, yy, Bytes.toBytes(mot));
        Connection connection = getConnection();
        Table table = connection.getTable(table1);
        table.put(p);
    }
    private void getData() throws IOException,ServiceException{
        Get row= new Get(Bytes.toBytes("2018foot"));
        Connection connection = getConnection();
        Table table = connection.getTable(table1);
        Result result = table.get(row);
        String r=Bytes.toString(result.getColumnCells(cf_date,mois).get(0).getValue());
        System.out.println(r);

        }

    private void scanData() throws IOException,ServiceException {
        Connection connection = getConnection();
        Table table = connection.getTable(table1);
        Scan scan = new Scan();
        //TODO avec un objet Scan, chercher les enregistrements dont la rowkey commence par ...setRowPrefixFilter ...
        scan.setRowPrefixFilter(Bytes.toBytes("2017"));
        ResultScanner scanner = table.getScanner(scan);
//        Result next = scanner.next();
        try {

            for (Result next : scanner) {
                System.out.println("next.getRow() = " + new String(next.getRow()));
            }
        }
        finally {
        scanner.close();}
    }

    private void createTable() throws IOException, ServiceException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        HTableDescriptor desc = new HTableDescriptor(table1);
        desc.addFamily(new HColumnDescriptor(cf_date));
        desc.addFamily(new HColumnDescriptor(cf_sport));
        admin.createTable(desc);

    }

    private Connection getConnection() throws ServiceException, IOException {

        Configuration config = HBaseConfiguration.create();


        // récupérer le chemin absolu du fichier ressource hbase-site.xml
        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));

        HBaseAdmin.checkHBaseAvailable(config);

        return ConnectionFactory.createConnection(config);
    }


    private void insertionMassive() throws IOException, ServiceException {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(40, 60, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());
        Connection connection = getConnection();
        Table table = connection.getTable(table1);
        // on lance une boucle externe qui consiste à lancer 10 threads en même temps
        int thread_number = 10;
        for (int j= 0;j<thread_number;j++) {
            // problème d'accessibilité d'un itérateur au sein de la classe interne
            int fiJ = j;
            int finalJ = j;
            threadPoolExecutor.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i <= 100; i++) {
                        byte[] row1 = Bytes.toBytes("row " + i+ "thread "+ finalJ);
                        Put p = new Put(row1);
                        p.addColumn(cf_date, yy, Bytes.toBytes("column " + i));
                        try {
                            table.put(p);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        threadPoolExecutor.shutdown();

        // threadPoolExecutor.execute(...);

    }
}
