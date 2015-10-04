import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable {

	public static void main(String[] args) throws IOException {

		// Instantiate Configuration class
		Configuration con = HBaseConfiguration.create();

		// Instaniate HBaseAdmin class
		HBaseAdmin admin = new HBaseAdmin(con);
		// Instantiate table descriptor class
		HTableDescriptor tableDescriptor = new HTableDescriptor(
				TableName.valueOf("powers"));
		// Add column families to table descriptor
		tableDescriptor.addFamily(new HColumnDescriptor("personal"));
		tableDescriptor.addFamily(new HColumnDescriptor("professional"));
		// Execute the table through admin
		admin.createTable(tableDescriptor);
		// Instantiating HTable class
		HTable hTable = new HTable(con, "powers");
		// Repeat these steps as many times as necessary

		// Instantiating Put class
		// Hint: Accepts a row name

		// Add values using add() method
		// Hints: Accepts column family name, qualifier/row name ,value

		Put p = new Put(Bytes.toBytes("row1"));
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"),
				Bytes.toBytes("superman"));
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("power"),
				Bytes.toBytes("strength"));
		p.add(Bytes.toBytes("professional"), Bytes.toBytes("name"),
				Bytes.toBytes("clark"));
		p.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"),
				Bytes.toBytes("100"));

		Put p1 = new Put(Bytes.toBytes("row2"));
		p1.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"),
				Bytes.toBytes("batman"));
		p1.add(Bytes.toBytes("personal"), Bytes.toBytes("power"),
				Bytes.toBytes("money"));
		p1.add(Bytes.toBytes("professional"), Bytes.toBytes("name"),
				Bytes.toBytes("bruce"));
		p1.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"),
				Bytes.toBytes("50"));

		Put p2 = new Put(Bytes.toBytes("row3"));
		p2.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"),
				Bytes.toBytes("wolverine"));
		p2.add(Bytes.toBytes("personal"), Bytes.toBytes("power"),
				Bytes.toBytes("healing"));
		p2.add(Bytes.toBytes("professional"), Bytes.toBytes("name"),
				Bytes.toBytes("logan"));
		p2.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"),
				Bytes.toBytes("75"));

		// Save the table
		hTable.put(p);
		hTable.put(p1);
		hTable.put(p2);
		// Instantiate the Scan class
		Scan scan = new Scan();
		// Scan the required columns
		scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
		// scan.addColumn(Bytes.toBytes("personal"),Bytes.toBytes("power"));
		// scan.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("name"));
		// scan.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("xp"));
		// Get the scan result
		ResultScanner scanner = hTable.getScanner(scan);
		// Read values from scan result
		for (Result result = scanner.next(); result != null; result = scanner
				.next())
			// Print scan result
			System.out.println(result);
		// Close the scanner
		scanner.close();
		// Htable closer
		hTable.close();
	}
}
