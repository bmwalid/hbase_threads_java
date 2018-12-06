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

    private TableName table1 = TableName.valueOf("<Nom table>");
    private String family1 = "<nom Colonne Family 1>";
    private String family2 = "<nom Colonne Family 2>";

    public static void main(String[] args) throws IOException, ServiceException {

        Lanceur lanceur = new Lanceur();
        lanceur.createTable();
        //lanceur.addData();
    }

    private void addData() throws IOException, ServiceException {

        byte[] row1 = Bytes.toBytes("<rowkey>");
        byte[] qualifier1 = Bytes.toBytes("<nomColonne1>");
        byte[] qualifier2 = Bytes.toBytes("<nomColonne2>");
        Put p = new Put(row1);

        //TODO avec la méthode addColumn de la classe Put, ajouter des colonnes et des valeurs aux différentes Colonne Family


        Connection connection = getConnection();
        Table table = connection.getTable(table1);
        table.put(p);


        //TODO : avec la  classe Get, retrouver l'objet correspondant à la rowkey row1


        //TODO afficher à l'écran la valeur d'une des cellules de cet enregistrement getValue

        Scan scan = new Scan();
        //TODO avec un objet Scan, chercher les enregistrements dont la rowkey commence par ...setRowPrefixFilter ...

        ResultScanner scanner = table.getScanner(scan);
        Result next = scanner.next();
        while(next != null ){
            System.out.println("next.getRow() = " + new String(next.getRow()));
            next = scanner.next();
        }

    }

    private void createTable() throws IOException, ServiceException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        HTableDescriptor desc = new HTableDescriptor(table1);
        desc.addFamily(new HColumnDescriptor(family1));
        desc.addFamily(new HColumnDescriptor(family2));
        admin.createTable(desc);

    }

    private Connection getConnection() throws ServiceException, IOException {

        Configuration config = HBaseConfiguration.create();

        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));

        HBaseAdmin.checkHBaseAvailable(config);

        return ConnectionFactory.createConnection(config);
    }


    private void insertionMassive() {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(40, 60, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());

        // threadPoolExecutor.execute(...);

    }
}
