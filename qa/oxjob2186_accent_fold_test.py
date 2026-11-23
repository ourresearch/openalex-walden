#!/usr/bin/env python3
"""Guard test for CreateWorkAuthorships step 5b rule B with diacritics folded (oxjob #2186, accent-fold rule).

Rule B (oxjob #1861) folds a later seat into an earlier one with the same name and author id. Since oxjob #2186 the
name is compared with Latin diacritics folded ('Léon Tabah' = 'Leon Tabah'), and on a registrant's deposit a full name
on a byline of at most 100 is taken when its two printed forms differ only by diacritics. Identical names on a deposit
still need the same ORCID (possible namesakes), and B's other guards stand. Reuses oxjob1861_one_seat_test's fixture
builder and checks (same SQL straight from the notebook, literal fixture works, a SQL warehouse).

    python3 qa/oxjob2186_accent_fold_test.py         # warehouse: $DATABRICKS_SQL_WAREHOUSE_ID or the serverless default
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import oxjob1861_one_seat_test as base  # noqa: E402

seat, A = base.seat, base.A
O1, O2 = "https://orcid.org/0000-0001-0000-0001", "https://orcid.org/0000-0001-0000-0002"
# (label, deposit_byline, seats, expected surviving names, extra checks)
CASES = [
    ("accent-only pair on a deposit, a book review's byline (JSTOR W2324334183)", True,
     [seat("Colin G. Clarke", A + "1"), seat("Léon Tabah", A + "2"), seat("Leon Tabah", A + "2")],
     ["Colin G. Clarke", "Léon Tabah"], {}),
    ("accent-only pair on a deposit, unaccented form first (W7212436554)", True,
     [seat("Marco Larranaga", A + "1", ras=["LEGOS"]), seat("Julien Jouanno", A + "2"),
      seat("Marco Larrañaga", A + "1", corr=True)],
     ["Marco Larranaga", "Julien Jouanno"], {"merged_corresponding": "Marco Larranaga"}),
    ("accent-only pair, letters TRANSLATE folds (Å, Ö, Ğ, Ł)", True,
     [seat("Ahmet Özgür DOĞRU", A + "1"), seat("Ahmet Ozgur DOGRU", A + "1"),
      seat("Anders Ångström", A + "2"), seat("Anders Angstrom", A + "2"),
      seat("Łukasz Nowak", A + "3"), seat("Lukasz Nowak", A + "3")],
     ["Ahmet Özgür DOĞRU", "Anders Ångström", "Łukasz Nowak"], {}),
    ("accent-only pair, repository byline, name order and case differ (W7219626794)", False,
     [seat("Lâm, Tô", A + "1", ras=["talehoapccc@gmail.com"]), seat("LAM TO", A + "1", ras=["talehoapccc@gmail.com"])],
     ["Lâm, Tô"], {"sole_author_corresponding": True}),
    ("accent-only initials-only pair on a deposit with one ORCID on both", True,
     [seat("J. Pérez", A + "1", O1), seat("K. Lee", A + "2"), seat("J. Perez", A + "1", O1)],
     ["J. Pérez", "K. Lee"], {}),
    ("accent-only initials-only pair on a deposit, no ORCID: left alone", True,
     [seat("E. Borónina", A + "1"), seat("K. Lee", A + "2"), seat("E. Boronina", A + "1")],
     ["E. Borónina", "K. Lee", "E. Boronina"], {"unchanged": True}),
    ("identical full name on a deposit, no ORCID: still left alone (possible namesakes)", True,
     [seat("Yu Chen", A + "1"), seat("Li Wang", A + "2"), seat("Yu Chen", A + "1")],
     ["Yu Chen", "Li Wang", "Yu Chen"], {"unchanged": True}),
    ("accent-only pair, different ORCIDs: two people, left alone", True,
     [seat("José García", A + "1", O1), seat("Jose Garcia", A + "1", O2)],
     ["José García", "Jose Garcia"], {"unchanged": True}),
    ("accent-only pair, different affiliations: left alone", False,
     [seat("José García", A + "1", ras=["Univ A"]), seat("Jose Garcia", A + "1", ras=["Univ B"])],
     ["José García", "Jose Garcia"], {"unchanged": True}),
    ("names differ by more than accents, one id: matcher error, left alone", True,
     [seat("José García", A + "1"), seat("Josefa Garcia", A + "1")],
     ["José García", "Josefa Garcia"], {"unchanged": True}),
    ("accent-only pair on a deposit of 101 seats: left alone", True,
     [seat("René Dupont", A + "1")] + [seat(f"Author Number{k}", A + f"{100 + k}") for k in range(99)]
     + [seat("Rene Dupont", A + "1")],
     ["René Dupont"] + [f"Author Number{k}" for k in range(99)] + ["Rene Dupont"], {"unchanged": True}),
    ("whole list repeated, romanized then with diacritics (rule A, oxjob #1155): unchanged behaviour", True,
     [seat("Quach, Huu Trung", A + "1"), seat("Mac, Thi Luu", A + "2"),
      seat("Quách, Hữu Trung", A + "1"), seat("Mạc, Thị Lựu", A + "2")],
     ["Quach, Huu Trung", "Mac, Thi Luu"], {}),
]


run = base.run  # a runner (e.g. a capped one) may replace this module's run before main()


def main():
    base.CASES = CASES
    base.run = globals()["run"]
    base.main()


if __name__ == "__main__":
    main()
