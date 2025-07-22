import os
import pickle
import json
import random
import pandas as pd
import numpy as np
import pathlib
import unicodedata
import tensorflow as tf
from transformers import TFAutoModelForSequenceClassification, pipeline, AutoTokenizer
from sentence_transformers import SentenceTransformer


def name_to_keep_ind(groups):
    """
    Function to determine if a text should be kept or not.

    Input:
    groups: list of character groups

    Output:
    0: if text should be not used
    1: if text should be used
    """
    # Groups of characters that do not perform well
    groups_to_skip = ['HIRAGANA', 'CJK', 'KATAKANA','ARABIC', 'HANGUL', 'THAI','DEVANAGARI','BENGALI',
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
    final_char = []
    groups_to_skip = ['HIRAGANA', 'CJK', 'KATAKANA','ARABIC', 'HANGUL', 'THAI','DEVANAGARI','BENGALI',
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
    

def get_journal_emb(journal_name):
    """
    Function to get journal embedding using SentenceTransformer.

    Input:
    journal_name: string of journal name

    Output:
    journal_emb: journal embedding
    """
    # Strip white space
    if isinstance(journal_name, str):
        journal_name = journal_name.strip()

        # Removing all journal names with eBook (most are not descriptive)
        if 'eBooks' in journal_name:
            return np.zeros(384, dtype=np.float32)

        # Check if non-latin characters are dominant (embedding model not good for that)
        elif check_for_non_latin_characters(journal_name) == 1:
            return emb_model.encode(journal_name)

        elif journal_name == '':
            return np.zeros(384, dtype=np.float32)

        else:
            return np.zeros(384, dtype=np.float32)
    else:
        return np.zeros(384, dtype=np.float32)
    
def tokenize(seq):
    """
    Function to tokenize text using model tokenizer.
    
    Input:
    seq: string of text
    
    Output:
    tok_data: dictionary of tokenized text
    """
    tok_data = tokenizer(seq, max_length=512, truncation=True, padding='max_length')
    return [tok_data['input_ids'], tok_data['attention_mask']]

def move_level_0_to_1(level_0, level_1):
    """
    Function to move level 0 citations to level 1 citations.
    
    Input:
    level_0: list of level 0 citations
    level_1: list of level 1 citations
    
    Output:
    list of final level 1 citations"""
    return list(set(level_0 + level_1))

def get_final_citations_for_model(list_of_links, num_to_take):
    """
    Function to get final citations for model if there are more than num_to_take citations.
    
    Input:
    list_of_links: list of citations
    num_to_take: number of citations to take
    
    Output:
    list of final citations
    """
    if len(list_of_links) <= num_to_take:
        return list_of_links
    else:
        return random.sample(list_of_links, num_to_take)

def get_final_citations_feature(citations, num_to_keep):
    """
    Function to get final citations for model if there are more than num_to_take citations
    and also to map the citations to gold citation ids.

    Input:
    citations: list of citations
    num_to_keep: number of citations to take

    Output:
    list of final citations
    """
    if citations:
        new_citations = get_final_citations_for_model(citations, num_to_keep)
        mapped_cites = [gold_to_label_mapping.get(x) for x in new_citations 
                        if gold_to_label_mapping.get(x)]
        temp_feature = [citation_feature_vocab[x] for x in mapped_cites]
    
        if len(temp_feature) < num_to_keep:
            return temp_feature + [0]*(num_to_keep - len(temp_feature))
        else:
            return temp_feature
    else:
        return [1] + [0]*(num_to_keep - 1)
    
def merge_title_and_abstract(title, abstract):
    """
    Function to merge title and abstract together for model input.
    
    Input:
    title: string of title
    abstract: string of abstract
    
    Output:
    string of title and abstract merged together"""
    if isinstance(title, str):
        if isinstance(abstract, str):
            if len(abstract) >=30:
                return f"<TITLE> {title}\n<ABSTRACT> {abstract[:2500]}"
            else:
                return f"<TITLE> {title}"
        else:
            return f"<TITLE> {title}"
    else:
        if isinstance(abstract, str):
            if len(abstract) >=30:
                return f"<TITLE> NONE\n<ABSTRACT> {abstract[:2500]}"
            else:
                return ""
        else:
            return ""

def clean_title(old_title):
    """
    Function to check if title should be kept and then remove non-latin characters. Also
    removes some HTML tags from the title.
    
    Input:
    old_title: string of title
    
    Output:
    new_title: string of title with non-latin characters and HTML tags removed
    """
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
        ab_len = len(raw_abstract)
        if ab_len > 30:
            final_abstract = raw_abstract[:2500]
            keep_abs = check_for_non_latin_characters(final_abstract)
            if keep_abs == 1:
                pass
            else:
                final_abstract = None
        else:
            final_abstract = None
            
    return final_abstract

def create_input_feature(features):
    """
    Function to create input feature for model.
    
    Input:
    features: list of features
    
    Output:
    input_feature: list of features in format for model"""
    # Convert to a tensorflow feature
    input_feature = [tf.expand_dims(tf.convert_to_tensor(x), axis=0) for x in [features[0], 
                                                         features[1], 
                                                         np.array(features[2], dtype=np.int32), 
                                                         np.array(features[3], dtype=np.int32), 
                                                         features[4]]]

    return input_feature

def get_gold_citations_from_all_citations(all_citations, gold_dict, non_gold_dict):
    """
    Function to get gold citations from all citations.
    
    Input:
    all_citations: list of all citations
    gold_dict: dictionary of gold citations
    non_gold_dict: dictionary of non-gold citations
    
    Output:
    level_0_gold: list of level 0 gold citations
    level_1_gold: list of level 1 gold citations
    """
    if isinstance(all_citations, list):
        if len(all_citations) > 200:
            all_citations = random.sample(all_citations, 200)
        
        level_0_gold_temp = [[x, gold_dict.get(x)] for x in all_citations if gold_dict.get(x)]

        level_1_gold_temp = [non_gold_dict.get(x) for x in all_citations if non_gold_dict.get(x)]

        level_0_gold = [x[0] for x in level_0_gold_temp]
        level_1_gold = [y for z in [x[1] for x in level_0_gold_temp] for y in z] + \
                        [x for y in level_1_gold_temp for x in y]

        return level_0_gold, level_1_gold
    else:
        return [], []

def get_lang_model_output(input_ids, attention_mask):
    """
    Returning XLA optimized output from language model
    
    Input:
    input_ids: tokenized title/abstract
    attention_mask: tokenized title/abstract attention mask
    
    Output:
    last layer output from language model
    """
    return xla_predict_lang_model(input_ids=input_ids, attention_mask=attention_mask).hidden_states[-1]
    

def create_model(num_classes, emb_table_size, model_chkpt, topk=5):
    """
    Function to create full model.
    
    Input:
    num_classes: number of classes
    emb_table_size: size of embedding table
    model_chkpt: path to model checkpoint
    topk: number of predictions to return
    
    Output:
    model: full model
    """
    # Inputs
    citation_0 = tf.keras.layers.Input((16,), dtype=tf.int64, name='citation_0')
    citation_1 = tf.keras.layers.Input((128,), dtype=tf.int64, name='citation_1')
    journal = tf.keras.layers.Input((384,), dtype=tf.float32, name='journal_emb')
    language_model_output = tf.keras.layers.Input((512, 768,), dtype=tf.float32, name='lang_model_output')
    
    # Create a multi-class classification model using functional API
    pooled_language_model_output = tf.keras.layers.GlobalAveragePooling1D()(language_model_output)
    citation_emb_layer = tf.keras.layers.Embedding(input_dim=emb_table_size, output_dim=256, mask_zero=True, 
                                                   trainable=True, name='citation_emb_layer')

    citation_0_emb = citation_emb_layer(citation_0)
    citation_1_emb = citation_emb_layer(citation_1)

    pooled_citation_0 = tf.keras.layers.GlobalAveragePooling1D()(citation_0_emb)
    pooled_citation_1 = tf.keras.layers.GlobalAveragePooling1D()(citation_1_emb)

    concat_data = tf.keras.layers.Concatenate(name='concat_data', axis=-1)([pooled_language_model_output, pooled_citation_0, 
                                                                            pooled_citation_1, journal])

    # Dense layer 1
    dense_output = tf.keras.layers.Dense(2048, activation='relu', kernel_regularizer='L2', name="dense_1")(concat_data)
    dense_output = tf.keras.layers.Dropout(0.20, name="dropout_1")(dense_output)
    dense_output = tf.keras.layers.LayerNormalization(epsilon=1e-6, name="layer_norm_1")(dense_output)
    
    # Dense layer 2
    dense_output = tf.keras.layers.Dense(1024, activation='relu', kernel_regularizer='L2', name="dense_2")(dense_output)
    dense_output = tf.keras.layers.Dropout(0.20, name="dropout_2")(dense_output)
    dense_output = tf.keras.layers.LayerNormalization(epsilon=1e-6, name="layer_norm_2")(dense_output)

    # Dense layer 3
    dense_output_l3 = tf.keras.layers.Dense(512, activation='relu', kernel_regularizer='L2', name="dense_3")(dense_output)
    dense_output = tf.keras.layers.Dropout(0.20, name="dropout_3")(dense_output_l3)
    dense_output = tf.keras.layers.LayerNormalization(epsilon=1e-6, name="layer_norm_3")(dense_output)
    
    output_layer = tf.keras.layers.Dense(num_classes, activation='sigmoid', name='output_layer')(dense_output)

    # Safely apply top-k using TFOpLambda
    # topk_outputs = tf.keras.layers.TFOpLambda(function="math.top_k", arguments={"k": topk}, name="tf.math.top_k")(output_layer)

    # topk_outputs = tf.math.top_k(output_layer, k=topk)
    
    model = tf.keras.Model(inputs=[citation_0, citation_1, journal, language_model_output], 
                           outputs=output_layer)

    model.load_weights(model_chkpt, skip_mismatch=True)
    model.trainable = False

    return model

# def create_model(num_classes, emb_table_size, model_chkpt, topk=5):
#     """
#     Function to create full model.
    
#     Input:
#     num_classes: number of classes
#     emb_table_size: size of embedding table
#     model_chkpt: path to model checkpoint
#     topk: number of predictions to return
    
#     Output:
#     model: full model
#     """
#     # Load finetuned language model
#     language_model = TFAutoModelForSequenceClassification.from_pretrained(language_model_name, output_hidden_states=True)
#     language_model.trainable = False
#     xla_predict_lang_model = tf.function(language_model, jit_compile=True)

#     # Inputs
#     ids = tf.keras.layers.Input((512,), dtype=tf.int64, name='ids')
#     mask = tf.keras.layers.Input((512,), dtype=tf.int64, name='mask')
#     citation_0 = tf.keras.layers.Input((16,), dtype=tf.int64, name='citation_0')
#     citation_1 = tf.keras.layers.Input((128,), dtype=tf.int64, name='citation_1')
#     journal = tf.keras.layers.Input((384,), dtype=tf.float32, name='journal_emb')
    
#     # Create a multi-class classification model using functional API
#     # language_model_output = language_model(input_ids=ids, attention_mask=mask).hidden_states[-1]
#     language_model_output = xla_predict_lang_model(input_ids=ids, attention_mask=mask).hidden_states[-1]
#     pooled_language_model_output = tf.keras.layers.GlobalAveragePooling1D()(language_model_output)
    
#     citation_emb_layer = tf.keras.layers.Embedding(input_dim=emb_table_size, output_dim=256, mask_zero=True, 
#                                                    trainable=True, name='citation_emb_layer')

#     citation_0_emb = citation_emb_layer(citation_0)
#     citation_1_emb = citation_emb_layer(citation_1)

#     pooled_citation_0 = tf.keras.layers.GlobalAveragePooling1D()(citation_0_emb)
#     pooled_citation_1 = tf.keras.layers.GlobalAveragePooling1D()(citation_1_emb)

#     concat_data = tf.keras.layers.Concatenate(name='concat_data', axis=-1)([pooled_language_model_output, pooled_citation_0, 
#                                                                             pooled_citation_1, journal])

#     # Dense layer 1
#     dense_output = tf.keras.layers.Dense(2048, activation='relu', kernel_regularizer='L2', name="dense_1")(concat_data)
#     dense_output = tf.keras.layers.Dropout(0.20, name="dropout_1")(dense_output)
#     dense_output = tf.keras.layers.LayerNormalization(epsilon=1e-6, name="layer_norm_1")(dense_output)
    
#     # Dense layer 2
#     dense_output = tf.keras.layers.Dense(1024, activation='relu', kernel_regularizer='L2', name="dense_2")(dense_output)
#     dense_output = tf.keras.layers.Dropout(0.20, name="dropout_2")(dense_output)
#     dense_output = tf.keras.layers.LayerNormalization(epsilon=1e-6, name="layer_norm_2")(dense_output)

#     # Dense layer 3
#     dense_output_l3 = tf.keras.layers.Dense(512, activation='relu', kernel_regularizer='L2', name="dense_3")(dense_output)
#     dense_output = tf.keras.layers.Dropout(0.20, name="dropout_3")(dense_output_l3)
#     dense_output = tf.keras.layers.LayerNormalization(epsilon=1e-6, name="layer_norm_3")(dense_output)
    
#     output_layer = tf.keras.layers.Dense(num_classes, activation='sigmoid', name='output_layer')(dense_output)
#     topk_outputs = tf.math.top_k(output_layer, k=topk)
    
#     model = tf.keras.Model(inputs=[ids, mask, citation_0, citation_1, journal], outputs=[topk_outputs, dense_output_l3])

#     model.load_weights(model_chkpt)
#     model.trainable = False

#     return model

def get_final_ids_and_scores(topic_ids, score, labels, title, abstract, threshold=0.04):
    """
    Function to apply some rules to get the final prediction (some clusters performed worse than others).
    
    Input:
    topic_ids: all ids for raw prediction output
    score: all scores for raw prediction output
    labels: all labels for raw prediction output
    title: title of the work
    abstract: abstract of the work
    
    Output:
    final_ids: post-processed final ids
    final_scores: post-processed final scores
    final_labels: post-processed final labels
    """
    final_ids = [-1]
    final_scores = [0.0]
    final_labels = [None]
    if any(topic_id in topic_ids for topic_id in [13241]):
        return final_ids, final_scores, final_labels
    elif any(topic_id in topic_ids for topic_id in [12705,13003]):
        if title != '':
            if check_for_non_latin_characters(title) == 1:
                if len(title.split(" ")) > 9:
                    if not isinstance(abstract, str):
                        final_ids = [x for x,y in zip(topic_ids, score) if y > threshold]
                        final_scores = [y for y in score if y > threshold]
                        final_labels = [x for x,y in zip(labels, score) if y > threshold]
                        if final_ids:
                            return final_ids, final_scores, final_labels
                        else:
                            return [-1], [0.0], [None]
                    elif isinstance(abstract, str):
                        if check_for_non_latin_characters(abstract) == 1:
                            final_ids = [x for x,y in zip(topic_ids, score) if y > threshold]
                            final_scores = [y for y in score if y > 0.05]
                            final_labels = [x for x,y in zip(labels, score) if y > threshold]
                            if final_ids:
                                return final_ids, final_scores, final_labels
                            else:
                                return [-1], [0.0], [None]
                        else:
                            return final_ids, final_scores, final_labels
                    else:
                        return final_ids, final_scores, final_labels
                else:
                    return final_ids, final_scores, final_labels
            else:
                return final_ids, final_scores, final_labels
        else:
            return final_ids, final_scores, final_labels
    else:
        if any(topic_id in topic_ids for topic_id in [12718,14377,13686,13723]):
            final_ids = [x for x,y in zip(topic_ids, score) if (x not in [12718,14377,13686,13723]) & (y > 0.80)]
            final_scores = [y for x,y in zip(topic_ids, score) if (x not in [12718,14377,13686,13723]) & (y > 0.80)]
            final_labels = [y for x,y,z in zip(topic_ids, labels, score) if (x not in [12718,14377,13686,13723]) & (z > 0.80)]
            if final_ids:
                return final_ids, final_scores, final_labels
            else:
                return [-1], [0.0], [None]
        elif any(topic_id in topic_ids for topic_id in [13064, 13537]):
            if title == 'Frontmatter':
                return [-1], [0.0], [None]
            else:
                final_ids = [x for x,y in zip(topic_ids, score) if (((x in [13064, 13537]) & (y > 0.95)) | 
                                                                ((x not in [13064, 13537]) & (y > threshold)))]
                final_scores = [y for x,y in zip(topic_ids, score) if (((x in [13064, 13537]) & (y > 0.95)) | 
                                                                    ((x not in [13064, 13537]) & (y > threshold)))]
                final_labels = [z for x,y,z in zip(topic_ids, score, labels) if (((x in [13064, 13537]) & (y > 0.95)) | 
                                                                    ((x not in [13064, 13537]) & (y > threshold)))]
                if final_ids:
                    return final_ids, final_scores, final_labels
                else:
                    return [-1], [0.0], [None]
        elif any(topic_id in topic_ids for topic_id in [11893, 13459]):
            test_scores = [y for x,y in zip(topic_ids, score) if (x in [11893, 13459])]
            if topic_ids[0] in [11893, 13459]:
                first_pred = 1
            else:
                first_pred = 0
            
            if [x for x in test_scores if x > 0.95] & (first_pred == 1):
                final_ids = [x for x,y in zip(topic_ids, score) if y > threshold]
                final_scores = [y for y in score if y > 0.05]
                final_labels = [x for x,y in zip(labels, score) if y > threshold]

                if final_ids:
                    return final_ids, final_scores, final_labels
                else:
                    return [-1], [0.0], [None]
            elif first_pred == 0:
                final_ids = [x for x,y in zip(topic_ids, score) if y > threshold]
                final_scores = [y for y in score if y > threshold]
                final_labels = [x for x,y in zip(labels, score) if y > threshold]

                if final_ids:
                    return final_ids, final_scores, final_labels
                else:
                    return [-1], [0.0], [None]
            else:
                return [-1], [0.0], [None]
        else:
            if isinstance(abstract, str) & (title != ''):
                if (check_for_non_latin_characters(title) == 1) & (check_for_non_latin_characters(abstract) == 1):
                    final_ids = [x for x,y in zip(topic_ids, score) if y > threshold]
                    final_scores = [y for y in score if y > threshold]
                    final_labels = [x for x,y in zip(labels, score) if y > threshold]
    
                    if final_ids:
                        return final_ids, final_scores, final_labels
                    else:
                        return [-1], [0.0], [None]
                else:
                    return [-1], [0.0], [None]
            elif title != '':
                if (check_for_non_latin_characters(title) == 1):
                    final_ids = [x for x,y in zip(topic_ids, score) if y > threshold]
                    final_scores = [y for y in score if y > threshold]
                    final_labels = [x for x,y in zip(labels, score) if y > threshold]
    
                    if final_ids:
                        return final_ids, final_scores, final_labels
                    else:
                        return [-1], [0.0], [None]
                else:
                    return [-1], [0.0], [None]
            elif isinstance(abstract, str):
                if (check_for_non_latin_characters(abstract) == 1):
                    final_ids = [x for x,y in zip(topic_ids, score) if y > threshold]
                    final_scores = [y for y in score if y > threshold]
                    final_labels = [x for x,y in zip(labels, score) if y > threshold]
    
                    if final_ids:
                        return final_ids, final_scores, final_labels
                    else:
                        return [-1], [0.0], [None]
                else:
                    return [-1], [0.0], [None]
            else:
                return [-1], [0.0], [None]

def process_data_as_df(new_df):
    """
    Function to process data as a dataframe (in batch).
    
    Input:
    new_df: dataframe of data
    
    Output:
    input_df: dataframe of data with predictions
    """
    input_df = new_df.copy()
    # Get citations into integer format
    input_df['referenced_works'] = input_df['referenced_works'].apply(lambda x: [int(i.split("https://openalex.org/W")[1]) for 
                                                                             i in x])

     # Process title and abstract and tokenize
    input_df['title'] = input_df['title'].apply(lambda x: clean_title(x))
    input_df['abstract_inverted_index'] = input_df.apply(lambda x: clean_abstract(x.abstract_inverted_index, x.inverted), axis=1)
    title_abstract = input_df.apply(lambda x: merge_title_and_abstract(x.title, x.abstract_inverted_index), axis=1).tolist()
    tok_title_abstract = tokenize(title_abstract)
    input_df['ids'] = tok_title_abstract[0]
    input_df['attention_mask'] = tok_title_abstract[1]
    
    # Take citations and return only gold citations (and then convert to label ids)
    input_df['referenced_works'] = input_df['referenced_works'].apply(lambda x: get_gold_citations_from_all_citations(x, gold_dict, 
                                                                                                                      non_gold_dict))
    input_df['citation_0'] = input_df['referenced_works'].apply(lambda x: get_final_citations_feature(x[0], 16))
    input_df['citation_1'] = input_df['referenced_works'].apply(lambda x: get_final_citations_feature(x[1], 128))    
    
    # Take in journal name and output journal embedding
    input_df['journal_emb'] = input_df['journal_display_name'].apply(get_journal_emb)

    # Check completeness of input data
    input_df['score_data'] = input_df\
        .apply(lambda x: 0 if ((x.title == "") & 
                               (not x.abstract_inverted_index) & 
                               (x.citation_0[0]==1) & 
                               (x.citation_1[0]==1)) else 1, axis=1)

    data_to_score = input_df[input_df['score_data']==1].copy()
    data_to_not_score = input_df[input_df['score_data']==0][['UID']].copy()

    if data_to_score.shape[0] > 0:
        # Transform into output for model
        data_to_score['input_feature'] = data_to_score.apply(lambda x: create_input_feature([x.ids, x.attention_mask, x.citation_0, 
                                                                                             x.citation_1, x.journal_emb]), axis=1)
    
        all_rows = [tf.convert_to_tensor([x[0][0] for x in data_to_score['input_feature'].tolist()]), 
                    tf.convert_to_tensor([x[1][0] for x in data_to_score['input_feature'].tolist()]),
                     tf.convert_to_tensor([x[2][0] for x in data_to_score['input_feature'].tolist()]), 
                    tf.convert_to_tensor([x[3][0] for x in data_to_score['input_feature'].tolist()]), 
                    tf.convert_to_tensor([x[4][0] for x in data_to_score['input_feature'].tolist()])]
        
        lang_model_output = get_lang_model_output(*all_rows[:2])
        preds = xla_predict((*all_rows[2:], lang_model_output))
    
        data_to_score['preds'] = preds.indices.numpy().tolist()
        data_to_score['scores'] = preds.values.numpy().tolist()
    else:
        data_to_score['preds'] = [[-1]]*data_to_not_score.shape[0]
        data_to_score['scores'] = [[0.0000]]*data_to_not_score.shape[0]
    
    data_to_not_score['preds'] = [[-1]]*data_to_not_score.shape[0]
    data_to_not_score['scores'] = [[0.0000]]*data_to_not_score.shape[0]
    
    return input_df[['UID','title','abstract_inverted_index']].merge(pd.concat([data_to_score[['UID','preds','scores']], 
                                              data_to_not_score[['UID','preds','scores']]], axis=0), 
                                   how='left', on='UID')