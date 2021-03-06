package org.bigmart.sink;

import com.typesafe.config.Config;
import kong.unirest.Unirest;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.bigmart.util.AES;
import org.bigmart.util.FetchLpCardNo;
import org.bigmart.util.MongoBullkInsert;
import org.bigmart.util.Partition;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class HttpRequestBigmart {
    private Logger log = Logger.getLogger(HttpRequestBigmart.class);

    private Config config = Launcher.config;
    private HttpURLConnection con;

    private String startDate;
    private String endDate;
    private int limit;
    private int offset;
    private String decrypt_key;
    private String env;
    private String rerun;
    private FetchLpCardNo fetchLpCardNo = new FetchLpCardNo();
    private MongoBullkInsert insertIntoMongo = new MongoBullkInsert();
    private List<String> lpcardno_fetched;
    private int total_records_fetched;
    private String lpcardno_filepath = "";
    private Calendar cal = Calendar.getInstance();
    private final DateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy");
    private final DateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd");
    private long transaction_id;

    public HttpRequestBigmart() {
        // validating the system properties
        validateRequest();

        // Initializing variables; default if any...
        initializeInstanceVariables();
    }

    private void initializeInstanceVariables() {
        fetchLpCardNo.connect(config);
        insertIntoMongo.connect(config);

        lpcardno_fetched = fetchLpCardNo.fetch();

        decrypt_key = config.getString("decrypt_key");

        startDate = System.getProperty("start_date");
        endDate = System.getProperty("end_date");
        limit = Integer.parseInt(System.getProperty("limit", "5000"));
    }

    private void validateRequest() {
        if(System.getProperty("env") == null){
            throw new RuntimeException("env property not defined, set env: dev/live");
        }else {
            env = System.getProperty("env");
        }

        if(System.getProperty("rerun") == null){
            rerun = "no";
        }else{
            rerun = System.getProperty("rerun");
        }

        if(env.equals("dev")){
            lpcardno_filepath = "/etc/bigmart_fetchdata/dev";
        }else if(env.equals("live")){
            lpcardno_filepath = "/etc/bigmart_fetchdata/live";
        }else{
            throw new RuntimeException("env should be dev or live...");
        }
    }

    private void writeToOutputStream(String payload) throws IOException {
        try (OutputStream out = con.getOutputStream()) {
            byte[] input = payload.getBytes("utf-8");
            out.write(input, 0, input.length);
            out.flush();
            out.close();
        }
    }

    private void sendMessageToMattermost(String msg){
        try {
            String h_url = config.getString("hook_url");
            JSONArray jar = new JSONArray();
            jar.put(new JSONObject().put("text", msg));
            JSONObject job = new JSONObject();
            job.put("attachments", jar);
//    HttpResponse<JsonNode> response =
            Unirest.post(h_url)
                    .header("Content-Type", "application/json")
                    .header("Charset", "UTF-8")
                    .body(job.toString())
                    .asJson();
        }catch (Exception e){
            log.error(e.getMessage(), e);
        }
    }

    private void write_lpcardno(String lpcardno_filepath, List<String> lpcardno_fetched) {
        try{
            BufferedWriter bfw = new BufferedWriter(new FileWriter(lpcardno_filepath, true));
            for(String lpcardno: lpcardno_fetched){
                bfw.write(lpcardno+"\n");
            }
            bfw.close();
        }catch (IOException e){
            log.error(e.getMessage(), e);
        }
    }

    public void request() throws InterruptedException{
        sendMessageToMattermost("Started to fetch records from bigmart for environment: " + env);

        transaction_id = new Date().getTime();

        String start_date = startDate;
        String final_date = endDate;

        if(startDate == null || endDate == null){
            final_date = dateFormat.format(new Date());
            try {
                Date parsedDate = dateFormat.parse(final_date);
                cal.setTime(parsedDate);
                cal.add(Calendar.DATE, -1);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            start_date = dateFormat.format(cal.getTime());
        }

        List<String> recorded_lpcardno = new ArrayList<>();

        try{
            BufferedReader bfr = new BufferedReader(new FileReader(lpcardno_filepath));
            String lpcardno;
            while((lpcardno = bfr.readLine()) != null){
                recorded_lpcardno.add(lpcardno);
            }
            bfr.close();
        }catch (IOException e){
            log.error(e.getMessage(), e);
        }

        if(rerun.equals("no")){
            if(recorded_lpcardno.size() == 0){
                // no lpcardno is recorded!!!
                startDate = "01-Jan-18";
                endDate = final_date;

                log.info("no lpcardno is recorded, initial phase!!!");
                sendMessageToMattermost("no lpcardno is recorded, initial phase!!!");

                for(List<String> lp_lst: Partition.ofSize(lpcardno_fetched, 500)){
                    boolean fetched = fetch_records("("+ lp_lst.stream().collect(Collectors.joining("','", "'", "'")) + ")");
                    if(fetched){
                        write_lpcardno(lpcardno_filepath, lp_lst);
                    }
                }
            }else{
                lpcardno_fetched.removeAll(recorded_lpcardno);
                if(lpcardno_fetched.size() == 0){
                    // same value in both recorded and nlpcardno!!!
                    startDate = start_date;
                    endDate = final_date;

                    log.info("same value in both recorded and nlpcardno!!!");
                    sendMessageToMattermost("same value in both recorded and nlpcardno!!!");

                    for(List<String> lp_lst: Partition.ofSize(recorded_lpcardno, 500)){
                        fetch_records("("+ lp_lst.stream().collect(Collectors.joining("','", "'", "'")) + ")");
                    }
                }else{
                    // new lpcardno found!!!
                    startDate = start_date;
                    endDate = final_date;

                    log.info("new lpcardno is found!!!");
                    log.info("sending request for old lpcardno!!!");

                    sendMessageToMattermost("new lpcardno is found!!!");
                    sendMessageToMattermost("sending request for old lpcardno!!!");

                    for(List<String> lp_lst: Partition.ofSize(recorded_lpcardno, 500)){
                        fetch_records("("+ lp_lst.stream().collect(Collectors.joining("','", "'", "'")) + ")");
                    }

                    startDate = "01-Jan-18";
                    endDate = final_date;

                    log.info("sending request for new lpcardno!!!");
                    sendMessageToMattermost("sending request for new lpcardno!!!");

                    for(List<String> lp_lst: Partition.ofSize(lpcardno_fetched, 500)){
                        boolean fetched = fetch_records("("+ lp_lst.stream().collect(Collectors.joining("','", "'", "'")) + ")");
                        if(fetched){
                            write_lpcardno(lpcardno_filepath, lp_lst);
                        }
                    }
                }
            }
        }else if (rerun.equals("for_lp")){
            try {
                startDate = start_date;
                endDate = final_date;

                if(System.getProperty("lpcard") == null){
                    throw new RuntimeException("No lpcard key found");
                }

                String lpcard = System.getProperty("lpcard");

                log.info("re-sending request for specific lpcard!!!");

                sendMessageToMattermost("re-sending request for specific lpcardno!!!");

                fetch_records("('"+lpcard+"')");
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        else{
            try {
                startDate = start_date;
                endDate = final_date;

                List<String> lp = fetchLpCardNo.fetch(dateFormat2.format(dateFormat.parse(start_date)), dateFormat2.format(dateFormat.parse(final_date)));
                recorded_lpcardno.removeAll(lp);
                log.info("re-sending request for old lpcardno!!!");

                sendMessageToMattermost("re-sending request for old lpcardno!!!");

                for(List<String> lp_lst: Partition.ofSize(recorded_lpcardno, 500)){
                    fetch_records("("+ lp_lst.stream().collect(Collectors.joining("','", "'", "'")) + ")");
                }
            } catch (ParseException e) {
                log.error(e.getMessage(), e);
            }
        }

        log.info("sending a trigger to calculate purchase history");
        sendMessageToMattermost("sending a trigger to calculate purchase history");
        try {
            URL url = new URL(config.getString("api_url"));
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            String auth = config.getString("api_user") + ":" + config.getString("api_pass");
            byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));

            String authHeaderValue = "Basic " + new String(encodedAuth);

            con.setRequestMethod("GET");
            con.setConnectTimeout(300000);
            con.setRequestProperty("token", config.getString("api_token"));
            con.setRequestProperty("env", config.getString("api_env"));
            con.setRequestProperty("Authorization", authHeaderValue);
            int responseCode = con.getResponseCode();
            String responseMsg = con.getResponseMessage();
            if (responseCode == 200){
                log.info("Response Code :: " + responseCode);
                log.info("Response Msg :: " + responseMsg);
            }else{
                log.error("Response Code :: " + responseCode);
                log.info("Response Msg :: " + responseMsg);
            }
            sendMessageToMattermost("Response Code :: " + responseCode + ", Response Msg :: " + responseMsg);
        }catch (IOException e){
            log.error(e.getMessage(), e);
            sendMessageToMattermost(e.getMessage());
        }
    }

    private boolean fetch_records(String lpcardno) throws InterruptedException {
        boolean fetched = true;

        List<Document> records = new ArrayList<>();
        log.info("start_date: " + startDate);
        log.info("end_date: " + endDate);

        offset = Integer.parseInt(System.getProperty("offset", "0"));
        int request_count = 0;
        String error_msg = "";
        String status = "working";
        try {
            while (!(status.equals("done"))) {

                if(request_count >= 3){
                    // send error message if request count is greater than 3
                    log.error(error_msg);
                    sendMessageToMattermost(error_msg);
                    Thread.sleep(5000);
                    break;
                }

                URL url = new URL(config.getString("request_url"));
                con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("POST");
                con.setRequestProperty("Content-Type", "application/json");
                con.setRequestProperty("Accept", "application/json");
                con.setRequestProperty("Username", "F<DmkAt)KJqWYeRF&e?w+S5B3eFX!w?^");
                con.setRequestProperty("Password", "bY(x7+%VDy7dRrEKUf]v<5RLAk&t=xS3");
                con.setConnectTimeout(3600000);
                con.setReadTimeout(3600000);
                con.setDoOutput(true);

                JSONObject payload= new JSONObject();

                payload.put("start_date", startDate);
                payload.put("end_date", endDate);
                payload.put("lpcardno", lpcardno);
                payload.put("limit", limit);
                payload.put("offset_value", offset);

                writeToOutputStream(payload.toString());

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuilder content = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();

                try{
                    log.info("\n############### Response Fetched ################\n");

                    JSONObject rs = new JSONObject(Objects.requireNonNull(AES.decrypt(content.toString(), decrypt_key)));
                    System.out.println(rs.toString());
                    JSONArray jr = rs.getJSONArray("columns");

                    log.info("rs count: " + rs.getInt("count"));
                    log.info("columns count: " + jr.length());

                    if(rs.getInt("count") != jr.length()){
                        request_count++;
                        error_msg = "column count didn't match with the count sent!!!\n" +
                                "count=" + rs.getInt("count") +", column_count="+jr.length();
                        fetched = false;
                        continue;
                    }

                    for (Object obj : jr) {
                        JSONObject record = new JSONObject();
                        JSONArray items = new JSONArray();
                        JSONObject cols = (JSONObject) obj;
                        record.put("transaction_id", transaction_id);
                        Iterator<String> columns = cols.keys();
                        while (columns.hasNext()) {
                            try {
                                String column_name = columns.next();

                                if (column_name.equals("items")) {
                                    String val = cols.getString(column_name);
                                    String[] rows = val.split(";;");

                                    for (String row: rows) {
                                        String[] items_row = row.split("!");
                                        JSONObject item_struct = new JSONObject();
                                        item_struct.put("cat1", items_row[0]);
                                        item_struct.put("icode", items_row[1]);
                                        item_struct.put("mrp", Float.parseFloat(items_row[2]));
                                        item_struct.put("saleqty", Float.parseFloat(items_row[3]));
                                        item_struct.put("basicamt",Float.parseFloat(items_row[4]));
                                        item_struct.put("promoamt", Float.parseFloat(items_row[5]));
                                        item_struct.put("saleamt", Float.parseFloat(items_row[6]));
                                        item_struct.put("returnamt", Float.parseFloat(items_row[7]));
                                        item_struct.put("totaldiscountamt", Float.parseFloat(items_row[8]));
                                        item_struct.put("netamt", Float.parseFloat(items_row[9]));
                                        item_struct.put("taxdescription", (items_row[10]));
                                        item_struct.put("taxpercent", Float.parseFloat(items_row[11]));
                                        item_struct.put("taxableamt", Float.parseFloat(items_row[12]));
                                        item_struct.put("taxamt", Float.parseFloat(items_row[13]));
                                        try {
                                            item_struct.put("loyaltydiscount", items_row[14].equals("") ? 0 : Float.parseFloat(items_row[14]));
                                        }catch (ArrayIndexOutOfBoundsException e){
                                            item_struct.put("loyaltydiscount", 0);
                                            item_struct.put("promoname", "N/A");
                                        }
                                        try {
                                            item_struct.put("promoname", items_row[15]);
                                        }catch (ArrayIndexOutOfBoundsException e){
                                            item_struct.put("promoname", "N/A");
                                        }
                                        // adding items in array list
                                        items.put(item_struct);
                                        record.put(column_name, items);
                                    }
                                } else {
                                    record.put(column_name, cols.get(column_name));
                                }
                            } catch (Exception e) {
                                // error while parsing data
                                System.out.println("\n\n######################### Exception Caught #########################\n\n");
                                System.out.println(cols);
                                log.error("Error column: " + cols);
                                log.error(e.getMessage(), e);
                                error_msg = e.getMessage();
                                request_count++;
                                fetched = false;
                            }
                        }
                        records.add(Document.parse(record.toString()));
                    }
                    status = rs.getString("status");
                    offset = rs.getInt("offset_value");
                    total_records_fetched = total_records_fetched + rs.getInt("count");
                    log.info("status: " + status);

                    sendMessageToMattermost("start_date: " + startDate +
                            "\nend_date: " + endDate +
                            "\nrs count: " + rs.getInt("count") +
                            "\ncolumns count: " + jr.length() +
                            "\nstatus: " + status);

                }catch (Exception e){
                    log.error(e.getMessage(), e);
                    error_msg = e.getMessage();
                    request_count++;
                    fetched = false;
                    Thread.sleep(500);
                }
            }
        } catch (Exception e) {
            log.error("HTTP call execution failed " + e.getMessage(), e);
            fetched = false;
            Thread.sleep(500);
        } finally {
            Thread.sleep(500);
        }

        if(records.size() > 0){
            insertIntoMongo.sink(records);

            sendMessageToMattermost("Task Completed" +
                    "\nTotal records fetched: " + total_records_fetched);
        }
        return fetched;
    }
}
