# model_config.py
model_path = "/Volumes/openalex/works/models/topic_classifier_v1/"
weights_path = "/Volumes/openalex/works/models/topic_classifier_v1/model_checkpoint/citation_part_only.keras"
weights_h5_path = "/Volumes/openalex/works/models/topic_classifier_v1/model_checkpoint/model.weights.h5"
model_checkpoint = weights_path
language_model_name = "OpenAlex/bert-base-multilingual-cased-finetuned-openalex-topic-classification-title-abstract"

target_vocab = None
inv_target_vocab = None
citation_feature_vocab = None
gold_to_label_mapping = None
gold_dict = None
non_gold_dict = None
emb_model = None
tokenizer = None
xla_predict = None
xla_predict_lang_model = None
