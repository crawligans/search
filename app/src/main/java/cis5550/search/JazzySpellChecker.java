package cis5550.search;

import com.swabunga.spell.engine.SpellDictionaryHashMap;
import com.swabunga.spell.engine.Word;
import com.swabunga.spell.event.SpellChecker;
import com.swabunga.spell.event.StringWordTokenizer;
import com.swabunga.spell.event.WordTokenizer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JazzySpellChecker {
  SpellChecker spellChecker;
  SpellDictionaryHashMap dictionaryHashMap;
  public JazzySpellChecker() {
    spellChecker = new SpellChecker();
    try {
      dictionaryHashMap = new SpellDictionaryHashMap(
        new File("dict/english.0"), new File("dict/phonet.en"));
    } catch (IOException e) {
      System.err.println("Error: Dictionary file not found!");
      // print pwd
      File dir = new File(".");
      String pwd = "";
      try {
        pwd = dir.getCanonicalPath();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      System.err.println("Current working directory : " + pwd);
      System.exit(1);
    }

    spellChecker.addDictionary(dictionaryHashMap);
  }

  public boolean correctSpelling(String text) {
    WordTokenizer tokenizer = new StringWordTokenizer(text);
    return spellChecker.checkSpelling(tokenizer) == SpellChecker.SPELLCHECK_OK;
  }

  public String fixSpelling(String text) {
    WordTokenizer tokenizer = new StringWordTokenizer(text);
    StringBuilder sb = new StringBuilder();
    while (tokenizer.hasMoreWords()) {
      String word = tokenizer.nextWord();
      if (!correctSpelling(word)) {
        List<Word> suggestions = spellChecker.getSuggestions(word, 0);
        if (suggestions.size() > 0) {
          sb.append(suggestions.get(0));
        } else {
          sb.append(word);
        }
        sb.append(" ");
      } else {
        sb.append(word).append(" ");
      }
    }

    return sb.toString();
  }

  public static void main(String[] args) throws IOException {
    String text = "This is a swfit commited adress on god's given earth.";
    JazzySpellChecker spellChecker = new JazzySpellChecker();
    System.out.println(spellChecker.correctSpelling(text));
    System.out.println(spellChecker.fixSpelling(text));
  }

}

