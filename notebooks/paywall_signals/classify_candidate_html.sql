-- oxjob #695: HTML-only paywall classifier v3.
-- TaxiCab outcomes calibrated these rules, but no TaxiCab field is a feature.
-- Re-run after refreshing pdf_candidate_html; the output is intentionally thin.
-- v2 2026-07-29: linkinghub.elsevier.com carries ScienceDirect DOM (SD rules extended;
-- drain must rewrite pdf_url to sciencedirect.com/science/article/pii/{PII}/pdf —
-- linkinghub URLs have 0 wins in 149K taxicab attempts).
-- v3 2026-07-30: jbs.elsevierhealth.com fingerprint generalizes the Lancet/Cell purchase
-- widget to 307 Elsevier journal-branded hosts; Cambridge buttonGetAccess split
-- (no-getaccess = 81/81 live PDFs); LWW purchase block; researchsquare host prior
-- (84/87); image-extension pdf_urls are parser artifacts, not candidates.

CREATE OR REPLACE TABLE openalex.parseland.pdf_candidate_classification
CLUSTER BY AUTO
COMMENT 'HTML-only PDF accessibility classifier for oxjob #695'
AS
WITH features AS (
  SELECT
    file_key, work_key, work_key_ns, native_id, native_id_namespace,
    pdf_url, url_host, publisher,
    CASE
      WHEN url_host = 'www.sciencedirect.com'
        AND html RLIKE '(?i)(openaccesslabel|class=["''][^"'']*licenseinfo)'
        THEN 'likely_free'
      WHEN url_host = 'www.sciencedirect.com'
        AND html RLIKE '(?i)(class=["''][^"'']*PurchasePDF|remoteaccessbutton)'
        THEN 'paywalled'

      WHEN url_host = 'link.springer.com'
        AND html RLIKE '(?i)data-test=["'']open-access'
        THEN 'likely_free'
      WHEN url_host = 'link.springer.com'
        AND html RLIKE '(?i)(sprcom-buybox|data-test-id=["'']buy-article|c-article-buy-box)'
        THEN 'paywalled'

      WHEN url_host = 'www.nature.com'
        AND html RLIKE '(?i)data-test=["'']open-access'
        THEN 'likely_free'
      WHEN url_host = 'www.nature.com'
        AND html RLIKE '(?i)readcube-buybox'
        THEN 'paywalled'

      WHEN url_host = 'academic.oup.com'
        AND html RLIKE '(?i)(get-access-jumplink|no-access-message)'
        THEN 'paywalled'
      WHEN url_host = 'www.tandfonline.com'
        AND html RLIKE '(?i)(accessDenialWidget|purchase-options)'
        THEN 'paywalled'

      WHEN url_host = 'linkinghub.elsevier.com'
        AND html RLIKE '(?i)(openaccesslabel|class=["''][^"'']*licenseinfo)'
        THEN 'likely_free'
      WHEN url_host = 'linkinghub.elsevier.com'
        AND html RLIKE '(?i)(class=["''][^"'']*PurchasePDF|remoteaccessbutton)'
        THEN 'paywalled'

      WHEN LOWER(pdf_url) RLIKE '\\.(jpg|jpeg|png|gif|svg)([?#].*)?$'
        THEN 'bad_candidate_url'
      WHEN url_host = 'www.researchsquare.com'
        THEN 'likely_free'
      WHEN url_host = 'www.cambridge.org' AND html RLIKE 'buttonGetAccess'
        THEN 'paywalled'
      WHEN url_host = 'www.cambridge.org'
        THEN 'likely_free'
      WHEN url_host = 'journals.lww.com'
        AND html RLIKE '(?i)(liPurchase|ejp-access-options)'
        THEN 'paywalled'
      WHEN html LIKE '%jbs.elsevierhealth.com%'
        AND html RLIKE '(?i)article-tools__purchase'
        THEN 'paywalled'

      ELSE 'needs_validation'
    END AS class,
    CASE
      WHEN url_host = 'www.sciencedirect.com'
        AND html RLIKE '(?i)(openaccesslabel|class=["''][^"'']*licenseinfo)'
        THEN 'sciencedirect_open_access_dom_v1'
      WHEN url_host = 'www.sciencedirect.com'
        AND html RLIKE '(?i)(class=["''][^"'']*PurchasePDF|remoteaccessbutton)'
        THEN 'sciencedirect_purchase_dom_v1'
      WHEN url_host = 'link.springer.com'
        AND html RLIKE '(?i)data-test=["'']open-access'
        THEN 'springer_open_access_dom_v1'
      WHEN url_host = 'link.springer.com'
        AND html RLIKE '(?i)(sprcom-buybox|data-test-id=["'']buy-article|c-article-buy-box)'
        THEN 'springer_buybox_dom_v1'
      WHEN url_host = 'www.nature.com'
        AND html RLIKE '(?i)data-test=["'']open-access'
        THEN 'nature_open_access_dom_v1'
      WHEN url_host = 'www.nature.com'
        AND html RLIKE '(?i)readcube-buybox'
        THEN 'nature_buybox_dom_v1'
      WHEN url_host = 'academic.oup.com'
        AND html RLIKE '(?i)(get-access-jumplink|no-access-message)'
        THEN 'oup_access_message_dom_v1'
      WHEN url_host = 'www.tandfonline.com'
        AND html RLIKE '(?i)(accessDenialWidget|purchase-options)'
        THEN 'tandfonline_access_denial_dom_v1'
      WHEN url_host = 'linkinghub.elsevier.com'
        AND html RLIKE '(?i)(openaccesslabel|class=["''][^"'']*licenseinfo)'
        THEN 'linkinghub_sd_open_access_dom_v2'
      WHEN url_host = 'linkinghub.elsevier.com'
        AND html RLIKE '(?i)(class=["''][^"'']*PurchasePDF|remoteaccessbutton)'
        THEN 'linkinghub_sd_purchase_dom_v2'
      WHEN LOWER(pdf_url) RLIKE '\\.(jpg|jpeg|png|gif|svg)([?#].*)?$'
        THEN 'image_pdf_url_v3'
      WHEN url_host = 'www.researchsquare.com'
        THEN 'researchsquare_host_v3'
      WHEN url_host = 'www.cambridge.org' AND html RLIKE 'buttonGetAccess'
        THEN 'cambridge_getaccess_dom_v3'
      WHEN url_host = 'www.cambridge.org'
        THEN 'cambridge_no_getaccess_dom_v3'
      WHEN url_host = 'journals.lww.com'
        AND html RLIKE '(?i)(liPurchase|ejp-access-options)'
        THEN 'lww_purchase_dom_v3'
      WHEN html LIKE '%jbs.elsevierhealth.com%'
        AND html RLIKE '(?i)article-tools__purchase'
        THEN 'jbs_purchase_widget_dom_v3'
      ELSE 'no_calibrated_html_rule_v1'
    END AS classifier_rule
  FROM openalex.landing_page.pdf_candidate_html
  WHERE status = 'ok'
)
SELECT *,
  CASE classifier_rule
    WHEN 'sciencedirect_open_access_dom_v1' THEN 1.000
    WHEN 'springer_open_access_dom_v1' THEN 0.995
    WHEN 'nature_open_access_dom_v1' THEN 0.995
    WHEN 'sciencedirect_purchase_dom_v1' THEN 0.055
    WHEN 'springer_buybox_dom_v1' THEN 0.005
    WHEN 'nature_buybox_dom_v1' THEN 0.005
    WHEN 'oup_access_message_dom_v1' THEN 0.000
    WHEN 'tandfonline_access_denial_dom_v1' THEN 0.000
    WHEN 'linkinghub_sd_open_access_dom_v2' THEN 0.981
    WHEN 'linkinghub_sd_purchase_dom_v2' THEN 0.055
    WHEN 'jbs_purchase_widget_dom_v3' THEN 0.008
    WHEN 'image_pdf_url_v3' THEN 0.000
    WHEN 'researchsquare_host_v3' THEN 0.966
    WHEN 'cambridge_getaccess_dom_v3' THEN 0.000
    WHEN 'cambridge_no_getaccess_dom_v3' THEN 0.995
    WHEN 'lww_purchase_dom_v3' THEN 0.000
    ELSE NULL
  END AS calibrated_pdf_yield,
  'html-dom-v3-2026-07-30' AS classifier_version,
  current_timestamp() AS classified_at
FROM features;
