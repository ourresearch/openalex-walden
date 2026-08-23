import json
import re
import unicodedata

def name_to_keep_ind(groups):
    """
    Function to determine if a text should be kept or not.

    Input:
    groups: list of character groups

    Output:
    0: if text should be not used
    1: if text should be used
    """
    # Groups of characters that do not perform well. CJK/HIRAGANA/KATAKANA removed
    # (oxjob #859) - mBERT reads them natively; stripping them emptied the input.
    groups_to_skip = ['ARABIC', 'HANGUL', 'THAI','DEVANAGARI','BENGALI',
                      'THAANA','GUJARATI','CYRILLIC']
    
    if any(x in groups_to_skip for x in groups):
        return 0
    else:
        return 1
    
def remove_non_latin_characters(text):
    """
    Function to remove non-latin characters.

    Input:
    text: string of characters

    Output:
    final_char: string of characters with non-latin characters removed
    """
    # Handle None/Null values
    if text is None:
        return None
    
    final_char = []
    groups_to_skip = ['ARABIC', 'HANGUL', 'THAI','DEVANAGARI','BENGALI',
                      'THAANA','GUJARATI','CYRILLIC']
    for char in text:
        try:
            script = unicodedata.name(char).split(" ")[0]
            if script not in groups_to_skip:
                final_char.append(char)
        except:
            pass
    return "".join(final_char)
    
def group_non_latin_characters(text):
    """
    Function to group non-latin characters and return the number of latin characters.

    Input:
    text: string of characters

    Output:
    groups: list of character groups
    latin_chars: number of latin characters
    """
    groups = []
    latin_chars = []
    text = text.replace(".", "").replace(" ", "")
    for char in text:
        try:
            script = unicodedata.name(char).split(" ")[0]
            if script == 'LATIN':
                latin_chars.append(script)
            else:
                if script not in groups:
                    groups.append(script)
        except:
            if "UNK" not in groups:
                groups.append("UNK")
    return groups, len(latin_chars)

def check_for_non_latin_characters(text):
    """
    Function to check if non-latin characters are dominant in a text.

    Input:
    text: string of characters

    Output:
    0: if text should be not used
    1: if text should be used
    """
    groups, latin_chars = group_non_latin_characters(str(text))
    if name_to_keep_ind(groups) == 1:
        return 1
    elif latin_chars > 20:
        return 1
    else:
        return 0
    

def is_heavily_stripped(original_text, cleaned_text, threshold=0.5):
    """
    Check if cleaning removed more than `threshold` fraction of the text,
    indicating the original was dominated by non-Latin characters (e.g. Japanese).
    Classifying the leftover Latin fragments produces garbage predictions.

    Input:
    original_text: string before clean_title/clean_abstract
    cleaned_text: string after clean_title/clean_abstract
    threshold: fraction of characters stripped above which we reject (default 0.5)

    Output:
    True if too much text was stripped, False otherwise
    """
    if not isinstance(original_text, str) or not original_text:
        return False
    original_len = len(original_text.replace(" ", ""))
    if original_len == 0:
        return False
    cleaned_len = len(cleaned_text.replace(" ", "")) if isinstance(cleaned_text, str) and cleaned_text else 0
    stripped_ratio = 1 - (cleaned_len / original_len)
    return stripped_ratio > threshold

def clean_title(old_title):
    """
    Function to check if title should be kept and then remove non-latin characters. Also
    removes some HTML tags from the title.
    
    Input:
    old_title: string of title
    
    Output:
    new_title: string of title with non-latin characters and HTML tags removed, or None if title is None
    """
    # Handle None/Null values
    if old_title is None:
        return None
    
    keep_title = check_for_non_latin_characters(old_title)
    if keep_title == 1:
        new_title = remove_non_latin_characters(old_title)
        if '<' in new_title:
            new_title = new_title.replace("<i>", "").replace("</i>","")\
                                 .replace("<sub>", "").replace("</sub>","") \
                                 .replace("<sup>", "").replace("</sup>","") \
                                 .replace("<em>", "").replace("</em>","") \
                                 .replace("<b>", "").replace("</b>","") \
                                 .replace("<I>", "").replace("</I>", "") \
                                 .replace("<SUB>", "").replace("</SUB>", "") \
                                 .replace("<scp>", "").replace("</scp>", "") \
                                 .replace("<font>", "").replace("</font>", "") \
                                 .replace("<inf>","").replace("</inf>", "") \
                                 .replace("<i /> ", "") \
                                 .replace("<p>", "").replace("</p>","") \
                                 .replace("<![CDATA[<B>", "").replace("</B>]]>", "") \
                                 .replace("<italic>", "").replace("</italic>","")\
                                 .replace("<title>", "").replace("</title>", "") \
                                 .replace("<br>", "").replace("</br>","").replace("<br/>","") \
                                 .replace("<B>", "").replace("</B>", "") \
                                 .replace("<em>", "").replace("</em>", "") \
                                 .replace("<BR>", "").replace("</BR>", "") \
                                 .replace("<title>", "").replace("</title>", "") \
                                 .replace("<strong>", "").replace("</strong>", "") \
                                 .replace("<formula>", "").replace("</formula>", "") \
                                 .replace("<roman>", "").replace("</roman>", "") \
                                 .replace("<SUP>", "").replace("</SUP>", "") \
                                 .replace("<SSUP>", "").replace("</SSUP>", "") \
                                 .replace("<sc>", "").replace("</sc>", "") \
                                 .replace("<subtitle>", "").replace("</subtitle>", "") \
                                 .replace("<emph/>", "").replace("<emph>", "").replace("</emph>", "") \
                                 .replace("""<p class="Body">""", "") \
                                 .replace("<TITLE>", "").replace("</TITLE>", "") \
                                 .replace("<sub />", "").replace("<sub/>", "") \
                                 .replace("<mi>", "").replace("</mi>", "") \
                                 .replace("<bold>", "").replace("</bold>", "") \
                                 .replace("<mtext>", "").replace("</mtext>", "") \
                                 .replace("<msub>", "").replace("</msub>", "") \
                                 .replace("<mrow>", "").replace("</mrow>", "") \
                                 .replace("</mfenced>", "").replace("</math>", "")

            if '<mml' in new_title:
                all_parts = [x for y in [i.split("mml:math>") for i in new_title.split("<mml:math")] for x in y if x]
                final_parts = []
                for part in all_parts:
                    if re.search(r"\>[$%#!^*\w.,/()+-]*\<", part):
                        pull_out = re.findall(r"\>[$%#!^*\w.,/()+-]*\<", part)
                        final_pieces = []
                        for piece in pull_out:
                            final_pieces.append(piece.replace(">", "").replace("<", ""))
                        
                        final_parts.append(" "+ "".join(final_pieces) + " ")
                    else:
                        final_parts.append(part)
                
                new_title = "".join(final_parts).strip()
            else:
                pass

            if '<xref' in new_title:
                new_title = re.sub(r"\<xref[^/]*\/xref\>", "", new_title)

            if '<inline-formula' in new_title:
                new_title = re.sub(r"\<inline-formula[^/]*\/inline-formula\>", "", new_title)

            if '<title' in new_title:
                new_title = re.sub(r"\<title[^/]*\/title\>", "", new_title)

            if '<p class=' in new_title:
                new_title = re.sub(r"\<p class=[^>]*\>", "", new_title)
            
            if '<span class=' in new_title:
                new_title = re.sub(r"\<span class=[^>]*\>", "", new_title)

            if 'mfenced open' in new_title:
                new_title = re.sub(r"\<mfenced open=[^>]*\>", "", new_title)
            
            if 'math xmlns' in new_title:
                new_title = re.sub(r"\<math xmlns=[^>]*\>", "", new_title)

        if '<' in new_title:
            new_title = new_title.replace(">i<", "").replace(">/i<", "") \
                                 .replace(">b<", "").replace(">/b<", "") \
                                 .replace("<inline-formula>", "").replace("</inline-formula>","")

        return new_title
    else:
        return ''
    
def clean_abstract(raw_abstract, inverted=False):
    """
    Function to clean abstract and return it in a format for the model.
    
    Input:
    raw_abstract: string of abstract
    inverted: boolean to determine if abstract is inverted index or not
    
    Output:
    final_abstract: string of abstract in format for model
    """
    # Handle None/Null abstracts (both Python None and Spark null)
    try:
        if raw_abstract is None:
            return None
    except:
        return None
    
    if inverted:
        if isinstance(raw_abstract, dict) | isinstance(raw_abstract, str):
            if isinstance(raw_abstract, dict):
                invert_abstract = raw_abstract
            else:
                invert_abstract = json.loads(raw_abstract)
            
            if invert_abstract.get('IndexLength'):
                ab_len = invert_abstract['IndexLength']

                if ab_len > 20:
                    abstract = [" "]*ab_len
                    for key, value in invert_abstract['InvertedIndex'].items():
                        for i in value:
                            abstract[i] = key
                    final_abstract = " ".join(abstract)[:2500]
                    keep_abs = check_for_non_latin_characters(final_abstract)
                    if keep_abs == 1:
                        pass
                    else:
                        final_abstract = None
                else:
                    final_abstract = None
            else:
                if len(invert_abstract) > 20:
                    abstract = [" "]*1200
                    for key, value in invert_abstract.items():
                        for i in value:
                            try:
                                abstract[i] = key
                            except:
                                pass
                    final_abstract = " ".join(abstract)[:2500].strip()
                    keep_abs = check_for_non_latin_characters(final_abstract)
                    if keep_abs == 1:
                        pass
                    else:
                        final_abstract = None
                else:
                    final_abstract = None
                
        else:
            final_abstract = None
    else:
        try:
            ab_len = len(raw_abstract)
            if ab_len > 10:
                final_abstract = raw_abstract[:3000]
                keep_abs = check_for_non_latin_characters(final_abstract)
                if keep_abs == 1:
                    pass
                else:
                    final_abstract = None
            else:
                final_abstract = None
        except:
            final_abstract = None
            
    return final_abstract
