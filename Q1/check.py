import csv

seen_nodes = set()
with open("nodes.csv", encoding="utf-8") as f:
    r = csv.DictReader(f)
    nodes = list(r)
    for n in nodes:
        if "," in n["name"]:
            print("Comma in name:", n)
        seen_nodes.add(n["id"])

# Known crew smoke test (not exhaustive)
known_crew_like = {"123"}  # Barrie M. Osborne
print("Crew-like IDs present:", known_crew_like & seen_nodes)

dupes = set()
bad = []
with open("edges.csv", encoding="utf-8") as f:
    r = csv.DictReader(f)
    for e in r:
        a, b = e["source"], e["target"]
        if a == b:
            bad.append(("self-loop", a, b))
        key = tuple(sorted((a, b)))
        if key in dupes:
            bad.append(("duplicate", a, b))
        dupes.add(key)

print("Problems:", bad[:10], "… total:", len(bad))
