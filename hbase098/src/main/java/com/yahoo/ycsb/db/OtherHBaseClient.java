package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

public class OtherHBaseClient extends com.yahoo.ycsb.DB {
    private static final Configuration config = new Configuration();
    
    static {
        config.addResource("hbase-default.xml");
        config.addResource("hbase-site.xml");
    }

    public boolean _debug = false;

    public String _table = "";
    public HTable _hTable = null;
    public String _columnFamily = "";
    public byte _columnFamilyBytes[];

    public static final int Ok = 0;
    public static final int ServerError = -1;
    public static final int HttpError = -2;
    public static final int NoMatchingRecord = -3;

    public static final Object tableLock = new Object();

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        if ((getProperties().getProperty("debug") != null)
                && (getProperties().getProperty("debug").compareTo("true") == 0)) {
            _debug = true;
        }

        _columnFamily = getProperties().getProperty("columnfamily");
        if (_columnFamily == null) {
            System.err
                    .println("Error, must specify a columnfamily for HBase table");
            throw new DBException("No columnfamily specified");
        }
        _columnFamilyBytes = Bytes.toBytes(_columnFamily);
        
        // read hbase client settings.
        for (Object key : getProperties().keySet()) {
            String pKey = key.toString();
            if (pKey.startsWith("hbase.")) {
                String pValue = getProperties().getProperty(pKey);
                if (pValue != null) {
                    config.set(pKey, pValue);
                }
            }
        }
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void cleanup() throws DBException {
        try {
            if (_hTable != null) {
                _hTable.flushCommits();
            }
        } catch (IOException e) {
            throw new DBException(e);
        }
    }

    public void getHTable(String table) throws IOException {
        synchronized (tableLock) {
            _hTable = new HTable(config, table);
        }

    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to read.
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public Status read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        // if this is a "new" table, init HTable object. Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try {
                getHTable(table);
                _table = table;
            } catch (IOException e) {
                System.err.println("Error accessing HBase table: " + e);
                return Status.ERROR;
            }
        }

        Result r = null;
        try {
            if (_debug) {
                System.out.println("Doing read from HBase columnfamily "
                        + _columnFamily);
                System.out.println("Doing read for key: " + key);
            }
            Get g = new Get(Bytes.toBytes(key));
            if (fields == null) {
                g.addFamily(_columnFamilyBytes);
            } else {
                for (String field : fields) {
                    g.addColumn(_columnFamilyBytes, Bytes.toBytes(field));
                }
            }
            r = _hTable.get(g);
        } catch (IOException e) {
            System.err.println("Error doing get: " + e);
            return Status.ERROR;
        } catch (ConcurrentModificationException e) {
            // do nothing for now...need to understand HBase concurrency model
            // better
            return Status.ERROR;
        }

        for (KeyValue kv : r.raw()) {
            result.put(
                    Bytes.toString(kv.getQualifier()),
                    new ByteArrayByteIterator(kv.getValue()));
            if (_debug) {
                System.out.println("Result for field: "
                        + Bytes.toString(kv.getQualifier()) + " is: "
                        + Bytes.toString(kv.getValue()));
            }

        }
        return Status.OK;
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param startkey
     *            The record key of the first record to read.
     * @param recordcount
     *            The number of records to read
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A Vector of HashMaps, where each HashMap is a set field/value
     *            pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public Status scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        // if this is a "new" table, init HTable object. Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try {
                getHTable(table);
                _table = table;
            } catch (IOException e) {
                System.err.println("Error accessing HBase table: " + e);
                return Status.ERROR;
            }
        }

        Scan s = new Scan(Bytes.toBytes(startkey));
        // HBase has no record limit. Here, assume recordcount is small enough
        // to bring back in one call.
        // We get back recordcount records
        s.setCaching(recordcount);

        // add specified fields or else all fields
        if (fields == null) {
            s.addFamily(_columnFamilyBytes);
        } else {
            for (String field : fields) {
                s.addColumn(_columnFamilyBytes, Bytes.toBytes(field));
            }
        }

        // get results
        ResultScanner scanner = null;
        try {
            scanner = _hTable.getScanner(s);
            int numResults = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                // get row key
                String key = Bytes.toString(rr.getRow());
                if (_debug) {
                    System.out.println("Got scan result for key: " + key);
                }

                HashMap<String, ByteIterator> rowResult = new HashMap<String, ByteIterator>();

                for (KeyValue kv : rr.raw()) {
                    rowResult.put(
                            Bytes.toString(kv.getQualifier()),
                            new ByteArrayByteIterator(kv.getValue()));
                }
                // add rowResult to result vector
                result.add(rowResult);
                numResults++;
                if (numResults >= recordcount) // if hit recordcount, bail out
                {
                    break;
                }
            } // done with row

        }

        catch (IOException e) {
            if (_debug) {
                System.out
                        .println("Error in getting/parsing scan result: " + e);
            }
            return Status.ERROR;
        }

        finally {
            scanner.close();
        }

        return Status.OK;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to write
     * @param values
     *            A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        // if this is a "new" table, init HTable object. Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try {
                getHTable(table);
                _table = table;
            } catch (IOException e) {
                System.err.println("Error accessing HBase table: " + e);
                return Status.ERROR;
            }
        }

        if (_debug) {
            System.out.println("Setting up put for key: " + key);
        }
        Put p = new Put(Bytes.toBytes(key));
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            byte[] value = entry.getValue().toArray();
            if (_debug) {
                System.out.println("Adding field/value " + entry.getKey() + "/"+
                  Bytes.toStringBinary(value) + " to put request");
            }
            p.add(_columnFamilyBytes,Bytes.toBytes(entry.getKey()), value);
        }

        try {
            _hTable.put(p);
        } catch (IOException e) {
            if (_debug) {
                System.err.println("Error doing put: " + e);
            }
            return Status.ERROR;
        } catch (ConcurrentModificationException e) {
            // do nothing for now...hope this is rare
            return Status.ERROR;
        }

        return Status.OK;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to insert.
     * @param values
     *            A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        return update(table, key, values);
    }

    /**
     * Delete a record from the database.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public Status delete(String table, String key) {
        // if this is a "new" table, init HTable object. Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try {
                getHTable(table);
                _table = table;
            } catch (IOException e) {
                System.err.println("Error accessing HBase table: " + e);
                return Status.ERROR;
            }
        }

        if (_debug) {
            System.out.println("Doing delete for key: " + key);
        }

        Delete d = new Delete(Bytes.toBytes(key));
        try {
            _hTable.delete(d);
        } catch (IOException e) {
            if (_debug) {
                System.err.println("Error doing delete: " + e);
            }
            return Status.ERROR;
        }

        return Status.OK;
    }
}
