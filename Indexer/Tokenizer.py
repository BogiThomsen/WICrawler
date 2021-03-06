from langdetect import detect
import re
import nltk
from nltk.stem.snowball import DanishStemmer, EnglishStemmer

eng_stop = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", 
"are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", 
"but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", 
"doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", 
"hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
"herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in",
"into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", 
"myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", 
"ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", 
"shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", 
"then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", 
"through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", 
"we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", 
"who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", 
"you've", "your", "yours", "yourself", "yourselves"] 

dk_stop = ["af", "alle", "andet", "andre", "at", "begge", "da", "de", "den", "denne", "der",
"deres", "det", "dette", "dig", "din", "dog", "du", "ej", "eller", "en", "end", "ene", "eneste",
"enhver", "et", "fem", "fire", "flere", "fleste", "for", "fordi", "forrige", "fra", "få", "før",
"god", "han", "hans", "har", "hendes", "her", "hun", "hvad", "hvem", "hver", "hvilken", "hvis", 
"hvor", "hvordan", "hvorfor", "hvornår" "i", "ikke", "ind", "ingen", "intet", "jeg", "jeres", "kan", 
"kom", "kommer", "lav", "lidt", "lille", "man", "mand", "mange", "med", "meget", "men", "mens", 
"mere", "mig", "ned", "ni", "nogen", "noget", "ny", "nyt", "nær", "næste", "næsten", "og", "op", "otte", 
"over", "på", "se" "seks", "ses", "som", "stor", "store", "syv", "ti", "til", "to", "tre", "ud", "var"]

valid_languages = ["da", "en"]


def tokenize_and_stem(text):
  ## check if eng or dk
  filtered_text = re.sub('[^\w\-\'/]', ' ', text)
  lang = detect(filtered_text)
  if lang in valid_languages:  
    tokens = filtered_text.lower().split(" ")
    processed = []

    stopwords = []
    if lang == "da":
      snowball = DanishStemmer()
      stopwords = dk_stop
    if lang == "en":
      snowball = EnglishStemmer()
      stopwords = eng_stop

    for token in tokens:
      if token in stopwords:
        continue
      elif "\n" in token:
        continue
      elif "\\n" in token:
        continue
      elif token == "":
        continue
      elif token.isdigit():
        continue
      else:
        processed.append(token)  
    stemmed = []
    for token in processed:
      stemmed.append(snowball.stem(token))
    return stemmed

  

