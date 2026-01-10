#!/usr/bin/env python3
"""
Verify NEES Elsevier DOIs:
1. Does Parseland actually extract affiliations?
2. Does the live OpenAlex API still lack affiliations?
"""

import json
import os
import time
import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NEES_DOIS = os.path.join(BASE_DIR, "runs/2025-01-03_preloaded_state_fix/nees_test/nees_dois.txt")

TAXICAB_BASE = "http://harvester-load-balancer-366186003.us-east-1.elb.amazonaws.com/taxicab"
PARSELAND_BASE = "http://parseland-load-balancer-667160048.us-east-1.elb.amazonaws.com/parseland"
OPENALEX_API = "https://api.openalex.org/works"


def load_nees_dois():
    """Load NEES DOIs from file."""
    with open(NEES_DOIS, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def is_elsevier_doi(doi):
    """Check if DOI is from Elsevier (10.1016 prefix)."""
    return doi.startswith("10.1016/")


def get_taxicab_html_uuid(doi):
    """Get HTML UUID from Taxicab for a DOI."""
    try:
        url = f"{TAXICAB_BASE}/doi/{doi}"
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            return None, f"Taxicab error: {response.status_code}"

        data = response.json()
        html_entries = data.get("html", [])

        # Find non-PDF entry
        for entry in html_entries:
            entry_url = entry.get("url", "")
            if "/pdf" not in entry_url.lower() and not entry_url.endswith(".pdf"):
                # UUID can be in download_url as /taxicab/{uuid} or /uuid/{uuid}
                download_url = entry.get("download_url", "")
                entry_id = entry.get("id", "")
                if entry_id:
                    return entry_id, None
                elif "/uuid/" in download_url:
                    uuid = download_url.split("/uuid/")[-1]
                    return uuid, None
                elif "/taxicab/" in download_url:
                    uuid = download_url.split("/taxicab/")[-1]
                    return uuid, None

        if html_entries:
            return None, "Only PDF entries found"
        return None, "No HTML entries"
    except Exception as e:
        return None, str(e)


def get_parseland_result(uuid):
    """Get Parseland extraction result for a UUID."""
    try:
        url = f"{PARSELAND_BASE}/{uuid}"
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            return None, "UUID not found (404)"
        if response.status_code != 200:
            return None, f"Parseland error: {response.status_code}"

        data = response.json()
        return data, None
    except Exception as e:
        return None, str(e)


def get_openalex_affiliations(doi):
    """Get affiliation data from live OpenAlex API."""
    try:
        url = f"{OPENALEX_API}/https://doi.org/{doi}"
        headers = {"User-Agent": "OpenAlexEvals/2.0 (mailto:team@ourresearch.org)"}
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 404:
            return None, "Not found in OpenAlex"
        if response.status_code != 200:
            return None, f"OpenAlex error: {response.status_code}"

        data = response.json()
        authorships = data.get("authorships", [])

        authors_with_affiliations = []
        for authorship in authorships:
            author_name = authorship.get("author", {}).get("display_name", "Unknown")
            raw_affs = authorship.get("raw_affiliation_strings", []) or []
            if raw_affs:
                authors_with_affiliations.append({
                    "name": author_name,
                    "affiliations": raw_affs
                })

        return {
            "total_authors": len(authorships),
            "authors_with_affiliations": len(authors_with_affiliations),
            "sample": authors_with_affiliations[:2] if authors_with_affiliations else []
        }, None
    except Exception as e:
        return None, str(e)


def has_affiliations_parseland(data):
    """Check if Parseland result has any affiliations."""
    if not data:
        return False
    authors = data.get("authors", [])
    for author in authors:
        affiliations = author.get("affiliations", [])
        for aff in affiliations:
            # Handle both string and object affiliations
            if isinstance(aff, dict):
                aff_name = aff.get("name", "")
            else:
                aff_name = str(aff)
            if aff_name and aff_name.strip():
                return True
    return False


def count_affiliations_parseland(data):
    """Count authors with affiliations in Parseland result."""
    if not data:
        return 0, 0
    authors = data.get("authors", [])
    total = len(authors)
    with_aff = 0
    for author in authors:
        affiliations = author.get("affiliations", [])
        for aff in affiliations:
            if isinstance(aff, dict):
                aff_name = aff.get("name", "")
            else:
                aff_name = str(aff)
            if aff_name and aff_name.strip():
                with_aff += 1
                break
    return total, with_aff


def main():
    print("="*80)
    print("NEES ELSEVIER DOI VERIFICATION")
    print("="*80)

    # Load NEES DOIs and filter to Elsevier
    all_dois = load_nees_dois()
    elsevier_dois = [doi for doi in all_dois if is_elsevier_doi(doi)]

    print(f"\nTotal NEES DOIs: {len(all_dois)}")
    print(f"Elsevier DOIs (10.1016): {len(elsevier_dois)}")

    # Sample for detailed check
    sample_size = min(30, len(elsevier_dois))
    sample_dois = elsevier_dois[:sample_size]

    print(f"\nChecking {sample_size} Elsevier DOIs...")
    print("-"*80)

    results = {
        "taxicab_has_html": 0,
        "taxicab_no_html": 0,
        "parseland_has_affiliations": 0,
        "parseland_no_affiliations": 0,
        "parseland_error": 0,
        "openalex_has_affiliations": 0,
        "openalex_no_affiliations": 0,
        "openalex_error": 0,
        "parseland_yes_openalex_no": 0,  # The key metric
    }

    detailed_results = []

    for i, doi in enumerate(sample_dois):
        print(f"\n[{i+1}/{sample_size}] {doi}")

        result = {"doi": doi}

        # Check Taxicab
        uuid, taxicab_err = get_taxicab_html_uuid(doi)
        if uuid:
            results["taxicab_has_html"] += 1
            result["taxicab"] = f"UUID: {uuid[:8]}..."

            # Check Parseland
            parseland_data, parseland_err = get_parseland_result(uuid)
            if parseland_err:
                results["parseland_error"] += 1
                result["parseland"] = f"ERROR: {parseland_err}"
                result["parseland_affiliations"] = False
            else:
                has_aff = has_affiliations_parseland(parseland_data)
                total_auth, with_aff = count_affiliations_parseland(parseland_data)
                if has_aff:
                    results["parseland_has_affiliations"] += 1
                    result["parseland"] = f"YES - {with_aff}/{total_auth} authors have affiliations"
                    result["parseland_affiliations"] = True
                    # Show sample
                    if parseland_data and parseland_data.get("authors"):
                        sample_author = parseland_data["authors"][0]
                        sample_aff = sample_author.get("affiliations", [])[:1]
                        if sample_aff:
                            aff_text = sample_aff[0].get("name", sample_aff[0]) if isinstance(sample_aff[0], dict) else sample_aff[0]
                            result["parseland_sample"] = f"  → {sample_author.get('name', 'Unknown')}: {aff_text[:60]}..."
                else:
                    results["parseland_no_affiliations"] += 1
                    result["parseland"] = f"NO - 0/{total_auth} authors have affiliations"
                    result["parseland_affiliations"] = False
        else:
            results["taxicab_no_html"] += 1
            result["taxicab"] = f"NO HTML: {taxicab_err}"
            result["parseland"] = "N/A (no HTML)"
            result["parseland_affiliations"] = False

        # Check OpenAlex
        openalex_data, openalex_err = get_openalex_affiliations(doi)
        if openalex_err:
            results["openalex_error"] += 1
            result["openalex"] = f"ERROR: {openalex_err}"
            result["openalex_affiliations"] = False
        else:
            if openalex_data["authors_with_affiliations"] > 0:
                results["openalex_has_affiliations"] += 1
                result["openalex"] = f"YES - {openalex_data['authors_with_affiliations']}/{openalex_data['total_authors']} authors"
                result["openalex_affiliations"] = True
            else:
                results["openalex_no_affiliations"] += 1
                result["openalex"] = f"NO - 0/{openalex_data['total_authors']} authors"
                result["openalex_affiliations"] = False

        # Key metric: Parseland has it but OpenAlex doesn't
        if result.get("parseland_affiliations") and not result.get("openalex_affiliations"):
            results["parseland_yes_openalex_no"] += 1
            result["gap"] = "★ PARSELAND HAS, OPENALEX MISSING"

        detailed_results.append(result)

        # Print result
        print(f"  Taxicab: {result['taxicab']}")
        print(f"  Parseland: {result['parseland']}")
        if "parseland_sample" in result:
            print(f"  {result['parseland_sample']}")
        print(f"  OpenAlex: {result['openalex']}")
        if "gap" in result:
            print(f"  {result['gap']}")

        time.sleep(0.2)  # Be polite

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    print(f"\n### Taxicab (HTML Availability)")
    print(f"  Has HTML: {results['taxicab_has_html']} ({results['taxicab_has_html']/sample_size*100:.1f}%)")
    print(f"  No HTML: {results['taxicab_no_html']} ({results['taxicab_no_html']/sample_size*100:.1f}%)")

    print(f"\n### Parseland (Parser Extraction)")
    with_html = results['taxicab_has_html']
    if with_html > 0:
        print(f"  Has affiliations: {results['parseland_has_affiliations']} ({results['parseland_has_affiliations']/with_html*100:.1f}% of those with HTML)")
        print(f"  No affiliations: {results['parseland_no_affiliations']} ({results['parseland_no_affiliations']/with_html*100:.1f}% of those with HTML)")
        print(f"  Errors: {results['parseland_error']}")

    print(f"\n### OpenAlex Live API")
    print(f"  Has affiliations: {results['openalex_has_affiliations']} ({results['openalex_has_affiliations']/sample_size*100:.1f}%)")
    print(f"  No affiliations: {results['openalex_no_affiliations']} ({results['openalex_no_affiliations']/sample_size*100:.1f}%)")
    print(f"  Errors: {results['openalex_error']}")

    print(f"\n### THE KEY METRIC")
    print(f"  ★ Parseland has affiliations BUT OpenAlex doesn't: {results['parseland_yes_openalex_no']}")
    print(f"    → These are works where the parser IS working but data isn't in the API")

    # List the gap DOIs
    gap_dois = [r["doi"] for r in detailed_results if r.get("gap")]
    if gap_dois:
        print(f"\n### DOIs with Parseland data missing from OpenAlex:")
        for doi in gap_dois:
            print(f"    - {doi}")


if __name__ == "__main__":
    main()
