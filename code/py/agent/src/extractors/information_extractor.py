import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InformationExtractor:
    def __init__(self, model_name="en_core_web_sm"):
        """
        Initialize the Information Extractor with a Spacy model.
        Falls back to regex if Spacy fails.
        """
        self.use_spacy = False
        try:
            import spacy
            try:
                self.nlp = spacy.load(model_name)
                self.use_spacy = True
                logger.info(f"Loaded Spacy model: {model_name}")
            except OSError:
                logger.warning(f"Model '{model_name}' not found. Downloading...")
                from spacy.cli import download
                download(model_name)
                self.nlp = spacy.load(model_name)
                self.use_spacy = True
        except Exception as e:
            logger.error(f"Failed to load Spacy: {e}. Switching to Regex-based extraction.")
            self.use_spacy = False

    def extract_entities(self, text):
        """
        Extract named entities from the text.
        Returns a list of tuples (text, label).
        """
        if self.use_spacy:
            try:
                doc = self.nlp(text)
                entities = [(ent.text, ent.label_) for ent in doc.ents]
                return entities
            except Exception as e:
                logger.error(f"Spacy extraction failed: {e}")
                return self._fallback_extract_entities(text)
        else:
            return self._fallback_extract_entities(text)

    def _fallback_extract_entities(self, text):
        # Fallback: extract capitalized words/phrases as entities
        # Matches: "Apple", "Steve Jobs", "YouTube", "United States"
        pattern = r'\b[A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*\b'
        matches = re.findall(pattern, text)
        # Filter out common stopwords that might be capitalized at start of sentence if needed
        # For now, just return unique matches
        return list(set([(m, "ENTITY") for m in matches if len(m) > 1]))

    def extract_triples(self, text):
        """
        Extract triples (subject, predicate, object) from the text.
        """
        if self.use_spacy:
            try:
                doc = self.nlp(text)
                triples = []
                for sent in doc.sents:
                    triples.extend(self._extract_triples_from_sent(sent))
                return triples
            except Exception as e:
                logger.error(f"Spacy extraction failed: {e}")
                return self._fallback_extract_triples(text)
        else:
            return self._fallback_extract_triples(text)

    def _extract_triples_from_sent(self, sent):
        triples = []
        for token in sent:
            if token.dep_ == "ROOT" and token.pos_ == "VERB":
                subject = None
                obj = None
                for child in token.children:
                    if child.dep_ in ("nsubj", "nsubjpass"):
                        subject = child
                    if child.dep_ in ("dobj", "attr", "acomp"):
                        obj = child
                
                if subject and obj:
                    subj_text = self._get_compound(subject)
                    obj_text = self._get_compound(obj)
                    pred_text = token.lemma_
                    triples.append((subj_text, pred_text, obj_text))
        return triples

    def _fallback_extract_triples(self, text):
        # Simple fallback: Subject Verb Object
        triples = []
        
        # Entity pattern: Capitalized words (possibly multiple)
        ent_pat = r'(?P<subj>[A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*)'
        
        # Predicate pattern: specific verbs or phrases
        # Expanded list
        verbs = [
            "is", "was", "are", "were",
            "owns", "owned",
            "leads", "led",
            "founded", "founded by",
            "located in", "is located in", "was located in",
            "produced", "produces",
            "created", "created by",
            "is the CEO of", "served as CEO of"
        ]
        # Sort by length descending to match longest first
        verbs.sort(key=len, reverse=True)
        verb_regex = "|".join([re.escape(v) for v in verbs])
        
        pred_pat = r'(?P<pred>\s+(?:' + verb_regex + r')\s+(?:a|an|the)?\s*)'
        
        # Case 1: Entity - Verb - Entity (e.g. Google owns YouTube)
        pat1 = ent_pat + pred_pat + r'(?P<obj>[A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*)'
        
        for match in re.finditer(pat1, text):
            triples.append((match.group('subj'), match.group('pred').strip(), match.group('obj')))
            
        # Case 2: Entity - Verb - Noun Phrase (e.g. Tesla produces electric cars)
        # We match until a period or comma
        pat2 = ent_pat + pred_pat + r'(?P<obj>[^.,\n]+)'
        
        for match in re.finditer(pat2, text):
            subj = match.group('subj')
            pred = match.group('pred').strip()
            obj = match.group('obj').strip()
            
            # Heuristic: Object shouldn't be too long (e.g. < 10 words)
            if len(obj.split()) < 10:
                # Avoid duplicates from pat1 (pat1 is subset of pat2 effectively if obj is capitalized)
                # But pat2 captures more. We can just add all and dedup later.
                triples.append((subj, pred, obj))

        return list(set(triples))

    def _get_compound(self, token):
        """
        Helper to get compound noun phrases.
        """
        compound = [child for child in token.children if child.dep_ == "compound"]
        compound.append(token)
        compound.sort(key=lambda x: x.i)
        return " ".join([c.text for c in compound])

if __name__ == "__main__":
    # Test
    extractor = InformationExtractor()
    text = "Apple Inc. was founded by Steve Jobs. It is located in Cupertino."
    print("Entities:", extractor.extract_entities(text))
    print("Triples:", extractor.extract_triples(text))
