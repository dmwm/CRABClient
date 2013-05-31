'''
Created on Nov 14, 2011

See http://norvig.com/spell-correct.html for documentation
'''

import re, collections
import string


def words(text): return re.findall('[a-z]+', text.lower())

def train(features):
    model = collections.defaultdict(lambda: 1)
    for f in features:
        model[f] += 1
    return model

DICTIONARY = []

def edits1(word):
    splits     = [(word[:i], word[i:]) for i in range(len(word) + 1)]
    deletes    = [a + b[1:] for a, b in splits if b]
    transposes = [a + b[1] + b[0] + b[2:] for a, b in splits if len(b)>1]
    replaces   = [a + c + b[1:] for a, b in splits for c in string.lowercase if b]
    inserts    = [a + c + b     for a, b in splits for c in string.lowercase]
    return set(deletes + transposes + replaces + inserts)

def known_edits2(word):
    return set(e2 for e1 in edits1(word) for e2 in edits1(e1) if e2 in DICTIONARY)

def known(words): return set(w for w in words if w in DICTIONARY)

def correct(word):
    candidates = known([word]) or known(edits1(word)) or known_edits2(word) or [word]
    return max(candidates, key=DICTIONARY.get)

def is_correct(word):
    return word in DICTIONARY

