package com.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MonReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        final Map<String, Integer> occurences = new HashMap<String, Integer>();
        int totalOccurences = 0;

        for (Text value : values) {
            String sValue = value.toString();
            occurences.put(sValue, occurences.containsKey(sValue) ? occurences.get(sValue) + 1 : 1);
            totalOccurences++;
        }

        List<Map.Entry<String, Integer>> produits = new ArrayList<>(occurences.entrySet());

        Collections.sort(produits, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        StringBuilder sProduits = new StringBuilder();
        for (Map.Entry<String, Integer> entry : produits) {
            String produit = entry.getKey();
            int count = entry.getValue();
            double percentage = (count * 100.0) / totalOccurences;
            sProduits.append(String.format("%s (%.2f%%), ", produit, percentage));
        }

        // Supprimer la virgule et l'espace final
        if (sProduits.length() > 0) {
            sProduits.setLength(sProduits.length() - 2);
        }

        context.write(key, new Text(sProduits.toString()));
    }
}
