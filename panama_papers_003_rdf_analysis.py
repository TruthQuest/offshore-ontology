import json
from collections import defaultdict, Counter
from typing import Dict, List
import networkx as nx
from rdflib import Graph
from tqdm import tqdm
import gc
import re


def discover_predicates_regex(filepath: str, sample_lines: int = 100000) -> Counter:
    """Regex-based predicate discovery - doesn't require valid TTL"""
    predicates = Counter()

    # Regex patterns for Turtle predicates
    patterns = [
        r'ns\d+:(\w+)\s+[<"]',  # ns1:predicate <uri> or "literal"
        r'vocab:(\w+)\s+[<"]',  # vocab:predicate
        r'<https?://[^>]+/vocab/(\w+)>',  # Full URI
    ]

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f):
            if i >= sample_lines:
                break

            for pattern in patterns:
                matches = re.findall(pattern, line)
                for match in matches:
                    predicates[match.lower()] += 1

    return predicates


def discover_predicates_robust(filepath: str, sample_lines: int = 200000) -> Counter:
    """Robust predicate discovery - tries multiple methods"""
    print("\nDiscovering predicates in data...")

    predicates = Counter()
    prefixes = []

    # Get prefixes
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if line.startswith('@prefix') or line.startswith('@base'):
                prefixes.append(line)
            elif not line.startswith('#') and line.strip():
                break

    prefix_block = ''.join(prefixes)

    # Get file size to sample strategically
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        total_lines = sum(1 for _ in f)

    print(f"   Total file lines: {total_lines:,}")

    # Try parsing multiple sample locations throughout the file
    num_samples = 8
    sample_locations = [int(total_lines * i / num_samples) for i in range(num_samples)]

    print(f"   Sampling from {num_samples} locations...")

    for idx, start_line in enumerate(sample_locations):
        chunk = []
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                # Skip to start location
                for i, line in enumerate(f):
                    if i < start_line:
                        continue
                    if i >= start_line + sample_lines:
                        break
                    if not (line.startswith('@') or line.startswith('#') or not line.strip()):
                        chunk.append(line)

            if not chunk:
                continue

            # Try to parse
            temp_g = Graph()
            chunk_data = prefix_block + '\n' + ''.join(chunk)
            temp_g.parse(data=chunk_data, format='turtle')

            sample_preds = 0
            for s, p, o in temp_g:
                pred = str(p).split('/')[-1].split('#')[-1]
                predicates[pred] += 1
                sample_preds += 1

            if sample_preds > 0:
                print(f"   ✓ Sample {idx + 1}/{num_samples} at line {start_line:,}: found {sample_preds:,} triples")

        except Exception as e:
            print(f"   ✗ Sample {idx + 1}/{num_samples} at line {start_line:,}: parse failed")
            continue

    # If parsing failed everywhere, fall back to regex
    if len(predicates) == 0:
        print("   Falling back to regex-based predicate discovery...")
        predicates = discover_predicates_regex(filepath, sample_lines * 2)

    print(f"\nTotal unique predicates discovered: {len(predicates)}")
    print("\nTop predicates found:")
    for pred, count in predicates.most_common(20):
        print(f"   {pred}: {count:,}")

    return predicates


def true_stream_parse_ttl(filepath: str, chunk_size: int = 100000):
    """TRUE streaming: parse TTL in chunks, never load full graph"""
    print(f"\n[TRUE STREAMING ANALYSIS WITH SEMANTIC TYPE FILTERING]")
    print("=" * 70)
    print(f"File: {filepath}")
    print(f"Chunk size: {chunk_size:,} lines")

    # STEP 1: Discover predicates with robust method
    predicates = discover_predicates_robust(filepath, sample_lines=200000)

    # CRITICAL: Define OWNERSHIP-ONLY predicates for ownership graph
    OWNERSHIP_PREDICATES = {
        'owns',
        'shareholderof',
        'beneficialowner',
        'ultimateowner',
        'underlying',
    }

    print(f"\n[CRITICAL: OWNERSHIP-ONLY FILTERING]")
    print(f"Ownership predicates (for cycle detection): {len(OWNERSHIP_PREDICATES)}")
    print(f"   {sorted(OWNERSHIP_PREDICATES)}")

    # ALL relationship predicates (for general network analysis)
    ALL_RELATIONSHIP_PREDICATES = {
        'connected_to',
        'owns',
        'hasregisteredaddress',
        'intermediaryfor',
        'similarto',
        'sameas',
        'role',
        'agent',
        'inorganization',
        'directorrole',
        'shareholderrole',
        'beneficialownerrole',
        'secretaryrole',
        'officerrole',
    }

    print(f"\nAll relationship predicates (for hub detection): {len(ALL_RELATIONSHIP_PREDICATES)}")
    print(f"   {sorted(ALL_RELATIONSHIP_PREDICATES)}")

    entities = set()
    officers = set()
    names = {}
    jurisdictions = {}

    # TWO SEPARATE GRAPHS!
    ownership_graph = nx.DiGraph()  # Only ownership relationships
    full_graph = nx.DiGraph()  # All relationships

    print("\nReading file and extracting data in chunks...")

    # First pass: collect @prefix declarations
    prefixes = []
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if line.startswith('@prefix') or line.startswith('@base'):
                prefixes.append(line)
            elif not line.startswith('#') and line.strip():
                break

    prefix_block = ''.join(prefixes)

    # Count total lines
    print("Counting lines...")
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        total_lines = sum(1 for _ in f)

    print(f"Total lines: {total_lines:,}")

    # Second pass: parse in chunks
    print("\nProcessing chunks...")

    chunk_buffer = []
    lines_processed = 0
    triples_extracted = 0
    parse_errors = 0

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        pbar = tqdm(total=total_lines, desc="   Reading")

        for line in f:
            lines_processed += 1
            pbar.update(1)

            # Skip prefixes and comments
            if line.startswith('@prefix') or line.startswith('@base') or line.startswith('#'):
                continue

            if not line.strip():
                continue

            chunk_buffer.append(line)

            # Process chunk when full
            if len(chunk_buffer) >= chunk_size:
                extracted, errors = process_chunk(
                    chunk_buffer, prefix_block, entities, officers,
                    names, jurisdictions, ownership_graph, full_graph,
                    OWNERSHIP_PREDICATES, ALL_RELATIONSHIP_PREDICATES
                )
                triples_extracted += extracted
                parse_errors += errors
                chunk_buffer = []
                gc.collect()

        pbar.close()

        # Process remaining lines
        if chunk_buffer:
            extracted, errors = process_chunk(
                chunk_buffer, prefix_block, entities, officers,
                names, jurisdictions, ownership_graph, full_graph,
                OWNERSHIP_PREDICATES, ALL_RELATIONSHIP_PREDICATES
            )
            triples_extracted += extracted
            parse_errors += errors

    print(f"\nExtracted:")
    print(f"   Lines processed: {lines_processed:,}")
    print(f"   Triples extracted: {triples_extracted:,}")
    print(f"   Parse errors: {parse_errors}")
    print(f"   Entities: {len(entities):,}")
    print(f"   Officers: {len(officers):,}")
    print(f"   Names: {len(names):,}")
    print(f"   Jurisdictions: {len(jurisdictions):,}")
    print(f"   OWNERSHIP edges (filtered): {ownership_graph.number_of_edges():,}")
    print(f"   ALL relationship edges: {full_graph.number_of_edges():,}")

    return entities, officers, names, jurisdictions, ownership_graph, full_graph


def process_chunk(chunk_lines: List[str], prefix_block: str, entities: set,
                  officers: set, names: dict, jurisdictions: dict,
                  ownership_graph: nx.DiGraph, full_graph: nx.DiGraph,
                  ownership_predicates: set, all_predicates: set) -> tuple:
    """Process a chunk of TTL lines - returns (triples_count, error_count)"""

    # Create mini graph for this chunk
    temp_g = Graph()
    error_count = 0

    try:
        # Combine prefixes with chunk
        chunk_data = prefix_block + '\n' + ''.join(chunk_lines)
        temp_g.parse(data=chunk_data, format='turtle')

        # Extract from this chunk
        for s, p, o in temp_g:
            s_str = str(s)
            p_str = str(p)
            o_str = str(o)

            # Get predicate name (last part of URI)
            pred_name = p_str.split('/')[-1].split('#')[-1].lower()

            # Extract entity types
            if 'type' in pred_name:
                if 'organization' in o_str.lower() or 'entity' in o_str.lower():
                    entities.add(s_str)
                elif 'person' in o_str.lower() or 'officer' in o_str.lower():
                    officers.add(s_str)

            # Extract names
            elif 'label' in pred_name or 'name' in pred_name:
                if s_str not in names:
                    names[s_str] = o_str[:200]

            # Extract jurisdictions
            elif 'jurisdiction' in pred_name:
                jurisdictions[s_str] = o_str.lower()

            # CRITICAL FIX: Filter by semantic type!
            elif o_str.startswith('http'):  # Only process URIs (not literals)

                # Add to ownership graph ONLY if it's ownership
                if pred_name in ownership_predicates:
                    ownership_graph.add_edge(s_str, o_str)

                # Add to full graph if it's any relationship
                if pred_name in all_predicates:
                    full_graph.add_edge(s_str, o_str)
                # Catch-all for relationship patterns not in hardcoded list
                elif any(rel in pred_name for rel in
                         ['own', 'connect', 'role', 'agent', 'organization', 'intermediary', 'address', 'similar']):
                    full_graph.add_edge(s_str, o_str)

        triple_count = len(temp_g)

    except Exception as e:
        # Count errors but continue
        error_count = 1
        triple_count = 0

    finally:
        # Clear chunk graph
        del temp_g

    return triple_count, error_count


def get_name(uri: str, names: dict) -> str:
    """Get readable name"""
    if uri in names:
        return names[uri][:100]
    parts = uri.split('/')
    if len(parts) > 0:
        return parts[-1].replace('_', ' ')[:50]
    return uri[:50]


def find_ownership_chains(graph: nx.DiGraph, names: dict, max_chains: int = 30) -> List[Dict]:
    """Find multi-layer ownership chains (using OWNERSHIP graph only)"""
    print("\n" + "=" * 70)
    print("ANALYSIS 1: OWNERSHIP CHAINS (ownership predicates only)")
    print("=" * 70)

    if graph.number_of_nodes() == 0:
        print("SKIP: No ownership relationships found")
        return []

    chains = []
    sample_size = min(300, graph.number_of_nodes())
    nodes = list(graph.nodes())[:sample_size]

    print(f"Checking {sample_size} nodes in OWNERSHIP graph...")

    for source in tqdm(nodes, desc="   Scanning"):
        try:
            lengths = nx.single_source_shortest_path_length(graph, source, cutoff=6)

            for target, length in lengths.items():
                if length >= 3:
                    try:
                        path = nx.shortest_path(graph, source, target)
                        chains.append({
                            'layers': len(path),
                            'owner': get_name(path[0], names),
                            'final': get_name(path[-1], names),
                            'chain': ' -> '.join([get_name(n, names) for n in path[:5]])
                        })

                        if len(chains) >= max_chains:
                            break
                    except:
                        pass
        except:
            continue

        if len(chains) >= max_chains:
            break

    chains.sort(key=lambda x: x['layers'], reverse=True)

    print(f"ALERT: Found {len(chains)} ownership chains")
    if chains:
        print(f"   Longest: {chains[0]['layers']} layers")

    return chains[:20]


def find_circular_ownership(graph: nx.DiGraph, names: dict) -> List[Dict]:
    """Find circular ownership (using OWNERSHIP graph only)"""
    print("\n" + "=" * 70)
    print("ANALYSIS 2: CIRCULAR OWNERSHIP (ownership predicates only)")
    print("=" * 70)

    if graph.number_of_nodes() == 0:
        print("SKIP: No ownership relationships")
        return []

    cycles = []

    try:
        print("Detecting cycles in OWNERSHIP graph...")
        for cycle in nx.simple_cycles(graph):
            if len(cycle) >= 2:  # Even 2-node cycles are suspicious (A owns B, B owns A)
                cycles.append({
                    'length': len(cycle),
                    'entities': [get_name(uri, names) for uri in cycle[:5]]
                })
            if len(cycles) >= 20:
                break

        print(f"ALERT: Found {len(cycles)} circular OWNERSHIP patterns")
        if cycles:
            print(f"   Largest: {max(c['length'] for c in cycles)} entities")
    except Exception as e:
        print(f"WARNING: {e}")

    return cycles


def find_hub_nodes(graph: nx.DiGraph, names: dict, min_degree: int = 5) -> List[Dict]:
    """Find hub intermediaries (using FULL graph for connectivity)"""
    print("\n" + "=" * 70)
    print("ANALYSIS 3: HUB INTERMEDIARIES (all relationships)")
    print("=" * 70)

    if graph.number_of_nodes() == 0:
        print("SKIP: No relationships")
        return []

    hubs = []

    for node in graph.nodes():
        in_deg = graph.in_degree(node)
        if in_deg >= min_degree:
            hubs.append({
                'intermediary': get_name(node, names),
                'entity_count': in_deg,
                'outgoing': graph.out_degree(node)
            })

    hubs.sort(key=lambda x: x['entity_count'], reverse=True)

    print(f"ALERT: Found {len(hubs)} hubs")
    if hubs:
        print(f"   Largest: {hubs[0]['entity_count']} entities")

    return hubs[:20]


def find_bridges(graph: nx.DiGraph, names: dict) -> List[Dict]:
    """Find bridge entities (using FULL graph)"""
    print("\n" + "=" * 70)
    print("ANALYSIS 4: BRIDGE ENTITIES (all relationships)")
    print("=" * 70)

    if graph.number_of_nodes() < 3:
        print("SKIP: Insufficient nodes")
        return []

    bridges = []

    try:
        print("Calculating centrality...")
        centrality = nx.degree_centrality(graph)

        for uri, score in sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:30]:
            if score > 0.001:
                bridges.append({
                    'entity': get_name(uri, names),
                    'centrality': score,
                    'connections': graph.degree(uri)
                })

        print(f"ALERT: Found {len(bridges)} bridges")
        if bridges:
            print(f"   Top: {bridges[0]['entity']}")
    except Exception as e:
        print(f"WARNING: {e}")

    return bridges[:20]


def analyze_jurisdictions(jurisdictions: dict) -> List[Dict]:
    """Analyze jurisdictions"""
    print("\n" + "=" * 70)
    print("ANALYSIS 5: JURISDICTIONS")
    print("=" * 70)

    if not jurisdictions:
        print("SKIP: No jurisdiction data")
        return []

    risk_zones = {'panama', 'virgin', 'cayman', 'cyprus', 'seychelles',
                  'bahamas', 'bermuda', 'jersey', 'belize', 'vanuatu'}

    counter = Counter(jurisdictions.values())
    total = sum(counter.values())

    anomalies = []
    for jurisdiction, count in counter.most_common(30):
        is_risk = any(zone in jurisdiction for zone in risk_zones)
        if is_risk:
            anomalies.append({
                'jurisdiction': jurisdiction,
                'count': count,
                'percentage': (count / total * 100) if total > 0 else 0
            })

    print(f"ALERT: Found {len(anomalies)} high-risk jurisdictions")
    if anomalies:
        print(f"   Total: {sum(a['count'] for a in anomalies):,} entities")

    return anomalies[:20]


def rank_targets(graph: nx.DiGraph, names: dict) -> List[Dict]:
    """Rank investigation targets (using FULL graph)"""
    print("\n" + "=" * 70)
    print("ANALYSIS 6: TARGETS (all relationships)")
    print("=" * 70)

    if graph.number_of_nodes() < 2:
        print("SKIP: Insufficient data")
        return []

    targets = []

    try:
        centrality = nx.degree_centrality(graph)

        for uri, score in sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:30]:
            if score > 0:
                priority = 'IMMEDIATE' if score > 0.05 else 'HIGH' if score > 0.02 else 'MEDIUM'
                targets.append({
                    'entity': get_name(uri, names),
                    'score': score,
                    'priority': priority
                })

        print(f"ALERT: {len(targets)} targets identified")
        immediate = sum(1 for t in targets if t['priority'] == 'IMMEDIATE')
        print(f"   IMMEDIATE: {immediate}")
    except Exception as e:
        print(f"WARNING: {e}")

    return targets[:30]


def main():
    """Main pipeline"""
    import os

    filepath = r"/Users/weisheit/PycharmProjects/EcuadorNarcos/panama_papers.ttl"

    if not os.path.exists(filepath):
        print(f"ERROR: File not found")
        return

    # TRUE streaming parse with SEMANTIC TYPE FILTERING
    entities, officers, names, jurisdictions, ownership_graph, full_graph = true_stream_parse_ttl(
        filepath,
        chunk_size=50000  # Process 50k lines at a time
    )

    # Run analyses with CORRECT graphs
    findings = {
        'ownership_chains': find_ownership_chains(ownership_graph, names),  # Uses ownership graph
        'circular_ownership': find_circular_ownership(ownership_graph, names),  # Uses ownership graph
        'hub_intermediaries': find_hub_nodes(full_graph, names),  # Uses full graph
        'bridge_entities': find_bridges(full_graph, names),  # Uses full graph
        'jurisdictions': analyze_jurisdictions(jurisdictions),
        'targets': rank_targets(full_graph, names)  # Uses full graph
    }

    # Save
    with open('fbi_report_FIXED.json', 'w') as f:
        json.dump(findings, f, indent=2, default=str)

    print("\n" + "=" * 70)
    print("COMPLETE - SEMANTIC TYPES PRESERVED!")
    print("=" * 70)
    print("\nReport saved: fbi_report_FIXED.json")
    print(f"\nGraph Statistics:")
    print(f"  Ownership edges (filtered): {ownership_graph.number_of_edges():,}")
    print(f"  All relationship edges: {full_graph.number_of_edges():,}")
    print(f"  Difference: {full_graph.number_of_edges() - ownership_graph.number_of_edges():,} non-ownership edges")
    print("\nKey Fix:")
    print("  ✓ Ownership cycles detected using ONLY ownership predicates")
    print("  ✓ Hub detection uses ALL relationships (correct for connectivity)")
    print("  ✓ Semantic types preserved through entire analysis")


if __name__ == "__main__":
    main()