-- oxjob #695: HTML-only paywall classifier v2.
-- TaxiCab outcomes calibrated these rules, but no TaxiCab field is a feature.
-- Re-run after refreshing pdf_candidate_html; the output is intentionally thin.
-- v2 2026-07-29: linkinghub.elsevier.com carries ScienceDirect DOM (SD rules extended;
-- drain must rewrite pdf_url to sciencedirect.com/science/article/pii/{PII}/pdf —
-- linkinghub URLs have 0 wins in 149K taxicab attempts); Lancet/Cell (Elsevier jbs
-- platform) article-tools__purchase widget = paywalled (0.8% live leak, 119/120).

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
      WHEN url_host IN ('www.thelancet.com', 'www.cell.com')
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
      WHEN url_host IN ('www.thelancet.com', 'www.cell.com')
        AND html RLIKE '(?i)article-tools__purchase'
        THEN 'jbs_purchase_widget_dom_v2'
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
    WHEN 'jbs_purchase_widget_dom_v2' THEN 0.008
    ELSE NULL
  END AS calibrated_pdf_yield,
  'html-dom-v2-2026-07-29' AS classifier_version,
  current_timestamp() AS classified_at
FROM features;
