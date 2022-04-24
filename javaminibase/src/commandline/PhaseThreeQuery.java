package commandline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class PhaseThreeQuery {
    private static void stringProcess(String[] strs) {
        for (int i = 0; i < strs.length - 1; i++) {
            if (!strs[i].equals("*")) {
                strs[i] = strs[i].substring(1, strs[i].length() - 1);
            }
        }
    }

    private static String stringProcess(String str) {
        if (!str.equals("*")) {
            str = str.substring(1, str.length() - 1);
        }

        return str;
    }

    public static void processFile(String queryfile) {
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
            int BPJoinNodePosition = Integer.parseInt(temp3[0]);
            int JoinOnSubjectorObject = Integer.parseInt(temp3[1]);
            String RightSubjectFilter = stringProcess(temp3[2]);
            String RightPredicateFilter = stringProcess(temp3[3]);
            String RightObjectFilter = stringProcess(temp3[4]);
            String RightConfidenceFilter = temp3[5];

            List<Integer> lonpAL = new ArrayList<Integer>();
            
            // If length is 9 then there is no LONP given
            if(temp3.length != 9)
            {
                int i = 6;

                // drop the bracket of the first LONP value
                temp3[i] = temp3[i].substring(1);

                while(!temp3[i].contains("}")){
                    lonpAL.add(Integer.parseInt(temp3[i]));
                    i++;
                }
                // remove closing }
                lonpAL.add(Integer.parseInt(temp3[i].substring(0, 1)));
            }
            int[] LeftOutNodePositions = lonpAL.stream().mapToInt(i -> i).toArray();

            int OutputRightSubject = Integer.parseInt(temp3[temp3.length - 2]);
            int OutputRightObject = Integer.parseInt(temp3[temp3.length - 1]);

            System.out.println("jnp: " + BPJoinNodePosition);
            System.out.println("jono: " + JoinOnSubjectorObject);
            System.out.println("rsf: " + RightSubjectFilter);
            System.out.println("rpf: " + RightPredicateFilter);
            System.out.println("rof: " + RightObjectFilter);
            System.out.println("rcf: " + RightConfidenceFilter);
            System.out.println("lonp: " + Arrays.toString(LeftOutNodePositions));
            System.out.println("ors: " + OutputRightSubject);
            System.out.println("oro: " + OutputRightObject);

            String[] secondStrs = strs[1].split("\\)");

            // JNP,JONO,RSF,RPF,ROF,RCF,LONP,ORS,ORO
            String[] secondSet = secondStrs[0].split(",");
            // stringProcess(secondSet);
            // System.out.println(Arrays.toString(secondSet));

            Integer BPJoinNodePosition2 = Integer.parseInt(secondSet[0]);
            Integer JoinOnSubjectorObject2 = Integer.parseInt(secondSet[1]);
            String RightSubjectFilter2 = stringProcess(secondSet[2]);
            String RightPredicateFilter2 = stringProcess(secondSet[3]);
            String RightObjectFilter2 = stringProcess(secondSet[4]);
            String RightConfidenceFilter2 = secondSet[5];

            // List<Integer> lonp2 = processLONP(secondSet[6]);
            List<Integer> lonpAL2 = new ArrayList<Integer>();
            
            // If length is 9 then there is no LONP given
            if(secondSet.length != 9)
            {
                int i = 6;

                // drop the bracket of the first LONP value
                secondSet[i] = secondSet[i].substring(1);

                while(!secondSet[i].contains("}")){
                    lonpAL2.add(Integer.parseInt(secondSet[i]));
                    i++;
                }
                // remove closing }
                lonpAL2.add(Integer.parseInt(secondSet[i].substring(0, secondSet[i].indexOf("}"))));
            }
            int[] LeftOutNodePositions2 = lonpAL2.stream().mapToInt(i -> i).toArray();


            int OutputRightSubject2 = Integer.parseInt(secondSet[secondSet.length - 2]);
            int OutputRightObject2 = Integer.parseInt(secondSet[secondSet.length - 1]);

            System.out.println("jnp2: " + BPJoinNodePosition2);
            System.out.println("jono2: " + JoinOnSubjectorObject2);
            System.out.println("rsf2: " + RightSubjectFilter2);
            System.out.println("rpf2: " + RightPredicateFilter2);
            System.out.println("rof2: " + RightObjectFilter2);
            System.out.println("rcf2: " + RightConfidenceFilter2);
            System.out.println("lonp2: " + Arrays.toString(LeftOutNodePositions2));
            System.out.println("ors2: " + OutputRightSubject2);
            System.out.println("oro2: " + OutputRightObject2);


            String[] lastStrs = secondStrs[1].split(",");
            int sort_order = Integer.parseInt(lastStrs[0]), SortNodeIDPos = Integer.parseInt(lastStrs[1]), n_pages = Integer.parseInt(lastStrs[2]);
            System.out.println("so: " + sort_order);
            System.out.println("snp: " + SortNodeIDPos);
            System.out.println("np: " + n_pages + "\n\n");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}