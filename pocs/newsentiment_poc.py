from NewsSentiment import TargetSentimentClassifier
# import 
tsc = TargetSentimentClassifier()

sentiment = tsc.infer_from_text("I don't like", "Robert.", "")
print(sentiment[0])

sentiment = tsc.infer_from_text("" ,"Mark Meadows", "'s coverup of Trump’s coup attempt is falling apart.")
print(sentiment[0])

# %%
# goal is to break up sentence into people entities and run newssentment on it
import spacy
# import pandas as pd
import neuralcoref
import logging
logging.basicConfig(level=logging.INFO)
nlp = spacy.load("en_core_web_md")
neuralcoref.add_to_pipe(nlp)
#%%
doc = nlp(u'My sister has a dog. She loves him.')
nlp.remove_pipe(name='neuralcoref')
coref = neuralcoref.NeuralCoref(nlp.vocab, conv_dict={'Lam': ['woman', "Carrie", 'executive']})
nlp.add_pipe(coref, name='neuralcoref')
doc = nlp(u'Carrie Lam passed the extradition bill, which Ted Hui said will ruin Hong Kong. Lam disagrees with him.')
doc._.has_coref
doc._.coref_clusters[1].main.text#.mentions[0].text
doc._.coref_scores
doc._.coref_resolved

#%%
df = pd.read_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\2021.csv")
row = df.iloc[3]
r
doc = nlp(row.Body)

ppl = [ent for ent in doc.ents if ent.label_ == "PERSON"]
ppl[-1].start
ppl[-1].end
sent_start = ppl[-1].sent.start
off_start = ppl[-1].start - sent_start
off_end = ppl[-1].end
ppl[-1].sent
[p.sent for p in ppl][-1].start
[0]
#.start
# [1]

doc.ents[0].label_ == "PERSON"