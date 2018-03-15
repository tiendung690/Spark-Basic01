package com.company.Conversion;

import java.io.*;
import java.util.ArrayList;
import java.util.TreeSet;

/**
 * Created by as on 11.01.2018.
 */
public class SequentionalConverter {

    public static boolean isNumerical(String s) {
        boolean isValidInteger = false;
        try {
            //Integer.parseInt(s);
            Double.parseDouble(s);
            // s is a valid integer
            isValidInteger = true;
        } catch (NumberFormatException ex) {
            // s is not an integer
        }
        return isValidInteger;
    }

    public static void main(String[] args) {
        String csvFile = "data/mllib/kdd.txt"; // Plik z danymi csv
        String libsvmFile = "data/mllib/libsvm.txt"; // Plik na wyjsciu
        String libsvmFileWithNewCols = "data/mllib/libsvmWithNewCols.txt"; // Plik na wyjściu

        // Konwersja wartosci symbolicznych na numeryczne w kolumnach

        symbolicalToNumerical(csvFile,libsvmFile);

        // Konwersja wartości symbolicznych na numeryczne,
        // każda nowa wartość symboliczna z kolumny tworzy nową kolumne, następnie dane przyjmuja wartości 1/0

        symbolicalToNumericalWithNewCols(csvFile, libsvmFileWithNewCols);

    }

    public static void symbolicalToNumericalWithNewCols(String csvFile, String libsvmFile) {

        System.out.println("SYMBOLICAL VALUES TO NUMERICAL");
        //String csvFile = "kdd.txt";
        //String libsvmFile = "libsvm.txt";
        String line = "";
        String cvsSplitBy = ",";

        /*
        PART 1 - Collecting all classes
         */

        // Pobranie z kazdej kolumny wartosci symbolicznych
        ArrayList<ArrayList<String>> listClasses = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                String[] element = line.split(cvsSplitBy);
                int class_position = element.length - 1;

                ///////////DANE//////////////////////////////
                //   <= z ostatna wartoscia(klasa), < bez
                for (int j = 0; j < class_position; j++) {

                    ArrayList<String> row_list;
                    // start czyli nie ma seta w liscie
                    if (listClasses.size() <= j) {
                        row_list = new ArrayList<String>();
                        listClasses.add(row_list);
                    } else {
                        row_list = listClasses.get(j);
                    }

                    if (isNumerical(element[j])) {

                    } else {
                        if (!row_list.contains(element[j])) {
                            row_list.add(element[j]);
                        }
                    }
                }
            }
            System.out.println("All classes: " + listClasses.toString());
            System.out.println("Non-numerical classes index: ");
            int ii = 1;
            for (int i = 0; i < listClasses.size(); i++) {
                if (listClasses.get(i).size() == 0) {
                    ii++;
                } else {
                    for (int j = 0; j < listClasses.get(i).size(); j++) {
                        System.out.println(ii + " -> " + listClasses.get(i).get(j).toString());
                        ii++;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        /*
        PART 2
         */

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            ArrayList<String> list_class = new ArrayList<String>();
            ArrayList<ArrayList<String>> list = new ArrayList<>();

            // FILE
            File the_file = new File(libsvmFile);
            PrintWriter the_output = new PrintWriter(the_file);
            // avoid header line
            // br.readLine();

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] element = line.split(cvsSplitBy);
                int class_position = element.length - 1;
                //////////////KLASYFIKATOR////////////////////////
                if (!list_class.contains(element[class_position])) {
                    list_class.add(element[class_position]);
                }
                int decision_class = 0;
                if (list_class.contains(element[class_position])) {
                    decision_class = list_class.indexOf(element[class_position]);
                } else {
                }
                the_output.print(decision_class + " ");
                ///////////DANE///////////////////////////////////
                //   <= z ostatna wartoscia(klasa), < bez
                int index = 1;
                for (int j = 0; j < class_position; j++) {
                    ArrayList<String> row_list;
                    row_list = listClasses.get(j);
                    if (isNumerical(element[j])) {
                        the_output.print(index + ":" + element[j] + " ");
                        index++;
                    } else {
                        for (int k = 0; k < row_list.size(); k++) {
                            String elem = row_list.get(k);
                            if (elem.equals(element[j])) {
                                the_output.print(index + ":" + "1" + " ");
                                index++;
                            } else {
                                the_output.print(index + ":" + "0" + " ");
                                index++;
                            }

                        }
                    }
                }
                the_output.println();
            }
            the_output.close();
        } catch (
                IOException e)

        {
            e.printStackTrace();
        }
    }

        /*
output

All classes: [[], [tcp, udp, icmp], [http, smtp, domain_u, auth, finger, telnet, eco_i, ftp, ntp_u, ecr_i, other, urp_i, private, pop_3, ftp_data, netstat, daytime, ssh, echo, time, name, whois, domain, mtp, gopher, remote_job, rje, ctf, supdup, link, systat, discard, X11, shell, login, imap4, nntp, uucp, pm_dump, IRC, Z39_50, netbios_dgm, ldap, sunrpc, courier, exec, bgp, csnet_ns, http_443, klogin, printer, netbios_ssn, pop_2, nnsp, efs, hostnames, uucp_path, sql_net, vmnet, iso_tsap, netbios_ns, kshell, urh_i, http_2784, harvest, aol, tftp_u, http_8001, tim_i, red_i], [SF, S2, S1, S3, OTH, REJ, RSTO, S0, RSTR, RSTOS0, SH], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []]
Non-numerical classes index:
2 -> tcp
3 -> udp
4 -> icmp
5 -> http
6 -> smtp
7 -> domain_u
8 -> auth
9 -> finger
10 -> telnet
11 -> eco_i
12 -> ftp
13 -> ntp_u
14 -> ecr_i
15 -> other
...

 */


    public static void symbolicalToNumerical(String csvFile, String libsvmFile) {

        System.out.println("SYMBOLICAL VALUES TO NUMERICAL WITH NEW COLUMNS");
        //String csvFile = "kdd.txt";
        //String libsvmFile = "libsvmWithNewCols.txt";
        String line = "";
        String cvsSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            ArrayList<String> list_class = new ArrayList<String>();
            ArrayList<ArrayList<String>> list = new ArrayList<>();

            // FILE
            File the_file = new File(libsvmFile);
            PrintWriter the_output = new PrintWriter(the_file);
            // avoid header line
            // br.readLine();

            int i = 0;
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] element = line.split(cvsSplitBy);
                int class_position = element.length - 1;

                //////////////KLASYFIKATOR////////////////////////////////////
                if (!list_class.contains(element[class_position])) {
                    list_class.add(element[class_position]);
                }

                int decision_class = 0;
                if (list_class.contains(element[class_position])) {
                    decision_class = list_class.indexOf(element[class_position]);
                } else {
                }
                the_output.print(decision_class + " ");

                ///////////DANE//////////////////////////////////////////////
                //   <= z ostatna wartoscia(klasa), < bez
                for (int j = 0; j < class_position; j++) {

                    TreeSet<String> row_set;
                    ArrayList<String> row_list;
                    // start czyli nie ma seta w liscie
                    if (list.size() <= j) {
                        row_list = new ArrayList<String>();
                        list.add(row_list);
                    } else {
                        row_list = list.get(j);
                    }

                    if (isNumerical(element[j])) {
                        the_output.print((j + 1) + ":" + element[j] + " ");
                    } else {
                        if (!row_list.contains(element[j])) {
                            row_list.add(element[j]);
                        }
                        int decision = 0;
                        if (row_list.contains(element[j])) {
                            decision = row_list.indexOf(element[j]);
                        } else {
                        }
                        the_output.print((j + 1) + ":" + decision + " ");
                    }
                }
                the_output.println();
                i++;
            }

            // close
            the_output.close();
            System.out.println("All classes: " + list.toString());
            System.out.println("\nDecision classes: " + list_class.toString());
            System.out.println();

            for (int j = 0; j < list.size(); j++) {
                System.out.println("------------------------");
                System.out.println("col " + j + "" + list.get(j).toString() + "\n");
                for (int k = 0; k < list.get(j).size(); k++) {
                    System.out.println("index " + k + " -> " + list.get(j).get(k).toString());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
output

All classes: [[], [tcp, udp, icmp], [http, smtp, domain_u, auth, finger, telnet, eco_i, ftp, ntp_u, ecr_i, other, urp_i, private, pop_3, ftp_data, netstat, daytime, ssh, echo, time, name, whois, domain, mtp, gopher, remote_job, rje, ctf, supdup, link, systat, discard, X11, shell, login, imap4, nntp, uucp, pm_dump, IRC, Z39_50, netbios_dgm, ldap, sunrpc, courier, exec, bgp, csnet_ns, http_443, klogin, printer, netbios_ssn, pop_2, nnsp, efs, hostnames, uucp_path, sql_net, vmnet, iso_tsap, netbios_ns, kshell, urh_i, http_2784, harvest, aol, tftp_u, http_8001, tim_i, red_i], [SF, S2, S1, S3, OTH, REJ, RSTO, S0, RSTR, RSTOS0, SH], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []]

Decision classes: [normal., buffer_overflow., loadmodule., perl., neptune., smurf., guess_passwd., pod., teardrop., portsweep., ipsweep., land., ftp_write., back., imap., satan., phf., nmap., multihop., warezmaster., warezclient., spy., rootkit.]

------------------------
col 0[]

------------------------
col 1[tcp, udp, icmp]

index 0 -> tcp
index 1 -> udp
index 2 -> icmp
------------------------
col 2[http, smtp, domain_u, auth, finger, telnet, eco_i, ftp, ntp_u, ecr_i, other, urp_i, private, pop_3, ftp_data, netstat, daytime, ssh, echo, time, name, whois, domain, mtp, gopher, remote_job, rje, ctf, supdup, link, systat, discard, X11, shell, login, imap4, nntp, uucp, pm_dump, IRC, Z39_50, netbios_dgm, ldap, sunrpc, courier, exec, bgp, csnet_ns, http_443, klogin, printer, netbios_ssn, pop_2, nnsp, efs, hostnames, uucp_path, sql_net, vmnet, iso_tsap, netbios_ns, kshell, urh_i, http_2784, harvest, aol, tftp_u, http_8001, tim_i, red_i]

index 0 -> http
index 1 -> smtp
index 2 -> domain_u
index 3 -> auth
index 4 -> finger
index 5 -> telnet
index 6 -> eco_i
index 7 -> ftp
index 8 -> ntp_u
index 9 -> ecr_i
index 10 -> other
index 11 -> urp_i
index 12 -> private
index 13 -> pop_3
...

 */
}