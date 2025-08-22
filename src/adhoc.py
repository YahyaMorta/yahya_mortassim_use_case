import json
import sys
from collections import defaultdict
from typing import Dict, Set


def journal_with_most_distinct_drugs(jsonl_path: str) -> str:
    """
    Parcourt le JSONL du pipeline et renvoie le NOM DU JOURNAL
    qui mentionne le plus de médicaments.
    Règle d'égalité : on choisit le nom alphabétiquement premier.
    """
    journal_to_drugs: Dict[str, Set[str]] = defaultdict(set)

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            drug = (obj.get("drug") or "").strip()
            if not drug:
                continue

            for j in obj.get("journals") or []:
                name = j.get("name")
                if not name:
                    continue
                # on compte le médicament pour ce journal s'il existe au moins
                # une sous-liste de mentions non vide (pubmed ou trials)
                mentions = j.get("mentions") or []
                if any(isinstance(lst, list) and len(lst) > 0 for lst in mentions):
                    journal_to_drugs[name].add(drug)

    if not journal_to_drugs:
        return ""

    # max par nb de médicaments DISTINCTS, puis ordre alphabétique
    return min(
        journal_to_drugs.items(),
        key=lambda kv: (-len(kv[1]), kv[0])
    )[0]


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.adhoc <chemin_vers_output_jsonl>", file=sys.stderr)
        sys.exit(2)
    name = journal_with_most_distinct_drugs(sys.argv[1])
    # On imprime UNIQUEMENT le nom, comme demandé
    print(name)
    # Si vide, le shell verra une ligne vide (pas d’exception)
