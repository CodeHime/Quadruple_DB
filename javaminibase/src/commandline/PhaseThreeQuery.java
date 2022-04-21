package commandline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class PhaseThreeQuery {
    private static void stringProcess(String[] strs) {
        for (int i = 0; i < strs.length; i++) {
            if (strs[i].equals("*")) {
                strs[i] = null;
            } else {
                strs[i] = strs[i].substring(2, strs[i].length() - 1);
            }
        }
    }

    private static String stringProcess(String str) {
        if (str.equals("*")) {
            str = null;
        } else {
            str = str.substring(2, str.length() - 1);
        }

        return str;
    }

    private static List<Integer> processLONP(String str) {
        str = str.substring(1, str.length() - 1);
        
        List<Integer> l = new LinkedList<>();
        String[] strs = str.split(":");
        for (String s : strs) {
            l.add(Integer.parseInt(s));
        }

        return l;
    }

    private static Double processDouble(String s) {
        if (s.equals("*")) {
            return null;
        } 
        
        return Double.parseDouble(s);
    }

    private static Integer processInteger(String s) {
        if (s.equals("*")) {
            return null;
        } 
        
        return Integer.parseInt(s);
    }

    public static void main(String[] argvs) {
        // Verify if input is valid
        if (argvs.length != 3) {
            System.out.println("Invalid arguments");
            return;
        }

        // Read Input
        String rdfDBName = argvs[0];
        String queryfile = argvs[1];
        int numBuf = Integer.parseInt(argvs[2]);

        System.out.println(rdfDBName + queryfile + numBuf);

        // Process query file
        try {
            String query = new String(Files.readAllBytes(Paths.get(queryfile)));

            // Remove newlines and whitespaces
            query = query.replace("\n", "").replace("\r", "");
            query = query.replaceAll("\\s+","");
                    
            String[] strs = query.split("\\),");
            String[] firstStrs = strs[0].split("],");
            String[] temp1 = firstStrs[0].split("\\[");
            String[] temp2 = temp1[1].split(",");
            stringProcess(temp2);

            String sf1 = temp2[0], pf1 = temp2[1], of1 = temp2[2], cf1 = temp2[3];
            System.out.println("SF1: " + sf1);
            System.out.println("PF1: " + pf1);
            System.out.println("OF1: " + of1);
            System.out.println("CF1: " + cf1);

            // JNP,JONO,RSF,RPF,ROF,RCF,LONP,ORS,ORO
            String[] temp3 = firstStrs[1].split(",");
            Integer jnp = processInteger(temp3[0]);
            Integer jono = processInteger(temp3[1]);
            String rsf = stringProcess(temp3[2]);
            String rpf = stringProcess(temp3[3]);
            String rof = stringProcess(temp3[4]);
            Double rcf = processDouble(temp3[5]);
            List<Integer> lonp = processLONP(temp3[6]);
            int ors = Integer.parseInt(temp3[7]);
            int oro = Integer.parseInt(temp3[8]);

            System.out.println("jnp: " + jnp);
            System.out.println("jono: " + jono);
            System.out.println("rsf: " + rsf);
            System.out.println("rpf: " + rpf);
            System.out.println("rof: " + rof);
            System.out.println("rcf: " + rcf);
            System.out.println("lonp: " + lonp);
            System.out.println("ors: " + ors);
            System.out.println("oro: " + oro);

            String[] secondStrs = strs[1].split("\\)");

            // JNP,JONO,RSF,RPF,ROF,RCF,LONP,ORS,ORO
            String[] secondSet = secondStrs[0].split(",");
            // stringProcess(secondSet);
            // System.out.println(Arrays.toString(secondSet));

            Integer jnp2 = processInteger(secondSet[0]);
            Integer jono2 = processInteger(secondSet[1]);
            String rsf2 = stringProcess(secondSet[2]);
            String rpf2 = stringProcess(secondSet[3]);
            String rof2 = stringProcess(secondSet[4]);
            Double rcf2 = processDouble(secondSet[5]);
            List<Integer> lonp2 = processLONP(secondSet[6]);
            Integer ors2 = processInteger(secondSet[7]);
            Integer oro2 = processInteger(secondSet[8]);

            System.out.println("jnp2: " + jnp2);
            System.out.println("jono2: " + jono2);
            System.out.println("rsf2: " + rsf2);
            System.out.println("rpf2: " + rpf2);
            System.out.println("rof2: " + rof2);
            System.out.println("rcf2: " + rcf2);
            System.out.println("lonp2: " + lonp2);
            System.out.println("ors2: " + ors2);
            System.out.println("oro2: " + oro2);


            String[] lastStrs = secondStrs[1].split(",");
            Integer so = processInteger(lastStrs[0]), snp = processInteger(lastStrs[1]), np = processInteger(lastStrs[2]);
            System.out.println("so: " + so);
            System.out.println("so: " + snp);
            System.out.println("np: " + np);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
