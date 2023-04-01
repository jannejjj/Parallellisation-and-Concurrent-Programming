import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.concurrent.ConcurrentHashMap;

public class App {

    static class Runner implements Runnable {
        int linesToSkip;
        int readCount;
        ConcurrentHashMap<String, Integer> wordMap;
        String filename;
        String runnerName;

        public Runner(int skip, int linesToRead, ConcurrentHashMap<String, Integer> map, String fName, String rName) {
            this.linesToSkip = skip;
            this.readCount = linesToRead;
            this.wordMap = map;
            this.filename = fName;
            this.runnerName = rName;
        }

        @Override
        public void run() {

            System.out.println(runnerName + " starting..");
            ConcurrentHashMap<String, Integer> threadMap = new ConcurrentHashMap<>();
            File file = new File(filename);

            Scanner scanner;
            try {

                /* Skip lines */
                scanner = new Scanner(file);
                for (int i = 0; (i < this.linesToSkip) && scanner.hasNextLine(); i++) {
                    scanner.nextLine();
                }

                /* Read readcount amount of lines */
                while (scanner.hasNextLine() && this.readCount > 0) {

                    /* Get next line and split into words */
                    String line = scanner.nextLine();
                    String[] words = line.split(" ");

                    for (String word : words) {

                        /* Convert to lowercase, remove whitespaces and ,.; and ignore "" */
                        word = word.toLowerCase().trim().replaceAll("[ ,.;]", "");
                        if (word != "") {
                            if (threadMap.containsKey(word)) {
                                threadMap.put(word, threadMap.get(word) + 1);
                            } else {
                                threadMap.put(word, 1);
                            }
                        }
                    }
                    this.readCount--;
                }
                scanner.close();

            } catch (FileNotFoundException e) {
                System.out.println("File " + filename + " was not found!");
                return;
            }

            /* Add values to the "big" map */
            threadMap.forEach((key, value) -> wordMap.merge(key, value, (v1, v2) -> v1 + v2));
            System.out.println(runnerName + " done.");
            return;
        }
    }

    public static void main(String[] args) throws Exception {

        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();

        /* User inputs */
        Scanner sc = new Scanner(System.in);

        System.out.println("Specify file name: ");
        String filename = sc.nextLine();

        System.out.println("Specify how many chunks the file should be divided into: ");
        int chunks = sc.nextInt();
        sc.close();

        /* Count lines in file */
        int lines = 0;
        try {

            BufferedReader reader = new BufferedReader(new FileReader(filename));
            while (reader.readLine() != null)
                lines++;

            reader.close();
            System.out.println("Lines: " + lines);

        } catch (FileNotFoundException e) {
            System.out.println("File " + filename + " was not found!");
            return;
        }

        /* Calculate chunk size */
        int chunksize = lines / chunks;
        System.out.println("Chunksize: " + chunksize);

        /*
         * If the # of lines can't be equally divided, increase chunksize by 1 to make
         * sure no lines are missed
         */

        if ((lines - chunks * chunksize) != 0) {
            chunksize += 1;
        }

        /* Start runners */
        ExecutorService executor = Executors.newFixedThreadPool(chunks);
        for (int i = 0; i < chunks; i++) {
            executor.submit(new Runner(chunksize * i, chunksize, map, filename, Integer.toString(i + 1)));
        }

        try {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /* Print values */
        System.out.println("\n* * * Values from hashmap: * * *\n");
        map.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " -- " + entry.getValue());
        });

        return;
    }
}
