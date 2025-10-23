#!/usr/bin/env python3
"""
NETWORK CONNECTION FINDER - Streaming + Parallel Version
=========================================================
Handles 9M+ triples efficiently with chunked parsing
"""

import gzip
import re
from collections import defaultdict, deque
import networkx as nx
from tqdm import tqdm
import pandas as pd
import os
from fuzzywuzzy import fuzz
from multiprocessing import Pool, cpu_count, Manager
import gc


def parse_nt_chunk(chunk_data):
    """Parse a chunk of N-Triples lines - worker function"""
    lines, chunk_id = chunk_data

    entities_data = {}
    relationships = []

    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        # N-Triples format: <subject> <predicate> <object> .
        match = re.match(r'<([^>]+)>\s+<([^>]+)>\s+(.+?)\s*\.$', line)
        if not match:
            continue

        subject, predicate, obj = match.groups()

        # Extract predicate name
        pred_name = predicate.split('/')[-1].split('#')[-1].lower()

        # Initialize entity if needed
        if subject not in entities_data:
            entities_data[subject] = {
                'uri': subject,
                'names': [],
                'countries': [],
                'jurisdictions': [],
                'datasets': [],
                'types': [],
                'addresses': []
            }

        # Clean object
        obj_cleaned = obj.strip()

        # Remove quotes and language tags for literals
        if obj_cleaned.startswith('"'):
            obj_cleaned = re.sub(r'^"(.+?)"(@[a-z]{2}|\^\^<[^>]+>)?$', r'\1', obj_cleaned)
        else:
            # It's a URI
            obj_cleaned = obj_cleaned.strip('<>')

        # Categorize predicate
        if pred_name in ['name', 'label']:
            entities_data[subject]['names'].append(obj_cleaned)

        elif pred_name in ['country', 'countries']:
            entities_data[subject]['countries'].append(obj_cleaned)

        elif pred_name == 'jurisdiction':
            entities_data[subject]['jurisdictions'].append(obj_cleaned)

        elif pred_name == 'dataset':
            entities_data[subject]['datasets'].append(obj_cleaned)

        elif pred_name == 'type':
            entities_data[subject]['types'].append(obj_cleaned)

        elif pred_name in ['address', 'hasregisteredaddress']:
            entities_data[subject]['addresses'].append(obj_cleaned)

        # Check if it's a relationship (object is URI and not a metadata predicate)
        if obj_cleaned.startswith('http') and pred_name not in [
            'type', 'subclassof', 'subpropertyof', 'domain', 'range',
            'dataset', 'sourcefile'
        ]:
            relationships.append((subject, obj_cleaned, pred_name))

    return entities_data, relationships


def stream_parse_file(filepath, chunk_size=50000, max_workers=None):
    """Stream parse large RDF file in chunks with multiprocessing"""

    if max_workers is None:
        max_workers = cpu_count()

    print(f"  Using {max_workers} worker processes")
    print(f"  Chunk size: {chunk_size:,} lines")

    # Determine file format
    opener = gzip.open if filepath.endswith('.gz') else open
    mode = 'rt'

    # Count lines first
    print("  Counting lines...")
    with opener(filepath, mode, encoding='utf-8', errors='ignore') as f:
        total_lines = sum(1 for _ in f)

    print(f"  Total lines: {total_lines:,}")

    # Read file in chunks
    all_entities = {}
    all_relationships = []

    chunks = []
    current_chunk = []
    chunk_id = 0

    print("  Reading file into chunks...")
    with opener(filepath, mode, encoding='utf-8', errors='ignore') as f:
        for line in tqdm(f, total=total_lines, desc="  Loading"):
            current_chunk.append(line)

            if len(current_chunk) >= chunk_size:
                chunks.append((current_chunk, chunk_id))
                current_chunk = []
                chunk_id += 1

        # Add remaining lines
        if current_chunk:
            chunks.append((current_chunk, chunk_id))

    print(f"  Created {len(chunks)} chunks")
    print(f"  Processing chunks in parallel...")

    # Process chunks in parallel
    with Pool(max_workers) as pool:
        results = list(tqdm(
            pool.imap(parse_nt_chunk, chunks),
            total=len(chunks),
            desc="  Parsing"
        ))

    # Merge results
    print("  Merging results...")
    for entities_data, relationships in tqdm(results, desc="  Merging"):
        # Merge entities
        for uri, data in entities_data.items():
            if uri not in all_entities:
                all_entities[uri] = data
            else:
                # Merge lists
                all_entities[uri]['names'].extend(data['names'])
                all_entities[uri]['countries'].extend(data['countries'])
                all_entities[uri]['jurisdictions'].extend(data['jurisdictions'])
                all_entities[uri]['datasets'].extend(data['datasets'])
                all_entities[uri]['types'].extend(data['types'])
                all_entities[uri]['addresses'].extend(data['addresses'])

        # Merge relationships
        all_relationships.extend(relationships)

        # Clean up
        del entities_data
        del relationships

    gc.collect()

    return all_entities, all_relationships


def load_sanctions_streaming(sanctions_file):
    """Load sanctioned entity names from file using streaming parser"""
    print(f"\n🚨 Loading sanctions from {os.path.basename(sanctions_file)}...")

    # Parse file
    entities_data, _ = stream_parse_file(sanctions_file, chunk_size=50000)

    print(f"  ✅ Loaded {len(entities_data):,} entities")

    # Build name index
    print("  Building name index...")
    sanctions_names = {}

    for uri, data in tqdm(entities_data.items(), desc="  Indexing"):
        # Get primary name
        if data['names']:
            for name in data['names']:
                name_key = name.lower().strip()
                if name_key and name_key not in sanctions_names:
                    sanctions_names[name_key] = {
                        'uri': uri,
                        'name': name,
                        'country': ', '.join(data['countries'][:3]) if data['countries'] else '',
                        'dataset': ', '.join(data['datasets'][:3]) if data['datasets'] else '',
                        'topics': '',
                        'sanctioned': 'Yes'
                    }

    print(f"  ✅ Found {len(sanctions_names):,} named entities")

    # Clean up
    del entities_data
    gc.collect()

    return sanctions_names


def load_panama_streaming(panama_file):
    """Load Panama Papers nodes and relationships using streaming parser"""
    print(f"\n📊 Loading Panama Papers from {os.path.basename(panama_file)}...")

    # Parse file
    entities_data, relationships = stream_parse_file(panama_file, chunk_size=50000)

    print(f"  ✅ Loaded {len(entities_data):,} entities")
    print(f"  ✅ Found {len(relationships):,} relationships")

    # Build name index and node data
    print("  Building indices...")
    panama_names = {}
    panama_nodes = {}

    for uri, data in tqdm(entities_data.items(), desc="  Processing"):
        # Get primary name
        primary_name = data['names'][0] if data['names'] else 'Unknown'

        # Build node data
        panama_nodes[uri] = {
            'uri': uri,
            'name': primary_name,
            'jurisdiction': data['jurisdictions'][0] if data['jurisdictions'] else '',
            'country': data['countries'][0] if data['countries'] else '',
            'entity_type': data['types'][-1].split('/')[-1] if data['types'] else '',
            'address': data['addresses'][0] if data['addresses'] else ''
        }

        # Index all names
        for name in data['names']:
            name_key = name.lower().strip()
            if name_key:
                panama_names[name_key] = uri

    print(f"  ✅ Indexed {len(panama_names):,} named entities")

    # Clean up
    del entities_data
    gc.collect()

    return panama_names, panama_nodes, relationships


def find_sanctioned_in_panama(sanctions_names, panama_names, fuzzy_threshold=85):
    """Find sanctioned entities that appear in Panama Papers"""
    print(f"\n🎯 Finding sanctioned entities in Panama Papers...")

    matches = []

    # Exact matches
    print("  Searching for exact name matches...")
    exact_count = 0

    # Use batched processing for large datasets
    sanctions_items = list(sanctions_names.items())
    batch_size = 10000

    for i in tqdm(range(0, len(sanctions_items), batch_size), desc="  Exact"):
        batch = sanctions_items[i:i + batch_size]

        for sanction_name, sanction_data in batch:
            if sanction_name in panama_names:
                matches.append({
                    'match_type': 'EXACT',
                    'similarity': 100,
                    'name': sanction_data.get('name', sanction_name),
                    'panama_uri': panama_names[sanction_name],
                    'sanction_uri': sanction_data['uri'],
                    'sanction_country': sanction_data.get('country', 'Unknown'),
                    'sanction_dataset': sanction_data.get('dataset', 'Unknown')
                })
                exact_count += 1

    print(f"    Found {exact_count:,} exact matches")

    # Fuzzy matches (only if we have few exact matches)
    if exact_count < 100:
        print(f"  Searching for fuzzy matches (>={fuzzy_threshold}% similarity)...")
        print(f"    Sampling for performance...")

        import random

        # Sample for fuzzy matching
        sample_size = min(1000, len(sanctions_names))
        sanctions_sample = random.sample(list(sanctions_names.items()), sample_size)

        panama_sample_size = min(10000, len(panama_names))
        panama_sample = random.sample(list(panama_names.items()), panama_sample_size)

        fuzzy_count = 0
        for sanction_name, sanction_data in tqdm(sanctions_sample, desc="  Fuzzy"):
            for panama_name, panama_uri in panama_sample:
                similarity = fuzz.ratio(sanction_name, panama_name)

                if fuzzy_threshold <= similarity < 100:
                    matches.append({
                        'match_type': 'FUZZY',
                        'similarity': similarity,
                        'name': f"{sanction_data.get('name')} ≈ {panama_name}",
                        'panama_uri': panama_uri,
                        'sanction_uri': sanction_data['uri'],
                        'sanction_country': sanction_data.get('country', 'Unknown'),
                        'sanction_dataset': sanction_data.get('dataset', 'Unknown')
                    })
                    fuzzy_count += 1
                    break

        print(f"    Found {fuzzy_count:,} fuzzy matches (from sample)")

    print(f"\n  ✅ Total matches: {len(matches):,}")

    return matches


def build_network_graph(panama_nodes, relationships):
    """Build NetworkX graph from Panama Papers data"""
    print(f"\n🕸️  Building network graph...")

    G = nx.DiGraph()

    # Add nodes in batches
    print("  Adding nodes...")
    batch_size = 10000
    node_items = list(panama_nodes.items())

    for i in tqdm(range(0, len(node_items), batch_size), desc="  Nodes"):
        batch = node_items[i:i + batch_size]
        for uri, data in batch:
            G.add_node(uri, **data)

    # Add edges in batches
    print("  Adding edges...")
    added = 0
    batch_size = 10000

    for i in tqdm(range(0, len(relationships), batch_size), desc="  Edges"):
        batch = relationships[i:i + batch_size]

        for source, target, rel_type in batch:
            # Only add if both nodes exist
            if source in G.nodes and target in G.nodes:
                G.add_edge(source, target, rel_type=rel_type)
                added += 1

    print(f"  ✅ Graph: {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")

    return G


def find_connected_entities(graph, sanctioned_uris, max_hops=2):
    """Find all entities within N hops of sanctioned entities"""
    print(f"\n🔍 Finding entities within {max_hops} hops of sanctioned entities...")

    # Filter to only sanctioned URIs that exist in graph
    valid_sanctioned = [uri for uri in sanctioned_uris if uri in graph]

    if not valid_sanctioned:
        print("  ⚠️  None of the sanctioned entities exist in the network graph")
        return {}

    print(f"  Starting from {len(valid_sanctioned):,} sanctioned nodes in network...")

    connected = {}

    # BFS from each sanctioned node
    for sanction_uri in tqdm(valid_sanctioned, desc="  Traversing"):
        queue = deque([(sanction_uri, 0, [sanction_uri])])
        visited = {sanction_uri}

        while queue:
            current, distance, path = queue.popleft()

            # Record this node
            if current not in connected or distance < connected[current]['distance']:
                connected[current] = {
                    'distance': distance,
                    'path': path,
                    'closest_sanction': sanction_uri,
                    'risk_score': 100 / (distance + 1)
                }

            # Continue if within hop limit
            if distance < max_hops:
                # Outgoing edges
                for neighbor in graph.neighbors(current):
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append((neighbor, distance + 1, path + [neighbor]))

                # Incoming edges
                for predecessor in graph.predecessors(current):
                    if predecessor not in visited:
                        visited.add(predecessor)
                        queue.append((predecessor, distance + 1, path + [predecessor]))

    # Remove sanctioned entities themselves
    connected = {k: v for k, v in connected.items() if v['distance'] > 0}

    # Count by distance
    distance_counts = defaultdict(int)
    for data in connected.values():
        distance_counts[data['distance']] += 1

    print(f"\n  ✅ Found {len(connected):,} connected entities:")
    for dist in sorted(distance_counts.keys()):
        print(f"    {dist}-hop: {distance_counts[dist]:,} entities")

    return connected


def save_results(matches, connected, panama_nodes, output_dir='network_analysis'):
    """Save analysis results to CSV files"""
    print(f"\n💾 Saving results to {output_dir}/...")

    os.makedirs(output_dir, exist_ok=True)

    # 1. Direct matches
    if matches:
        matches_df = pd.DataFrame(matches)
        matches_df.to_csv(f'{output_dir}/1_sanctioned_matches.csv', index=False)
        print(f"  ✅ {len(matches):,} direct matches → 1_sanctioned_matches.csv")
    else:
        print("  ⚠️  No matches to save")

    # 2. Connected entities
    if connected:
        connected_data = []
        for uri, data in connected.items():
            node_info = panama_nodes.get(uri, {})
            connected_data.append({
                'uri': uri,
                'name': node_info.get('name', 'Unknown'),
                'entity_type': node_info.get('entity_type', 'Unknown'),
                'jurisdiction': node_info.get('jurisdiction', 'Unknown'),
                'country': node_info.get('country', ''),
                'distance': data['distance'],
                'risk_score': round(data['risk_score'], 2),
                'closest_sanction': data['closest_sanction'],
                'path_length': len(data['path'])
            })

        connected_df = pd.DataFrame(connected_data)
        connected_df = connected_df.sort_values('risk_score', ascending=False)
        connected_df.to_csv(f'{output_dir}/2_connected_entities.csv', index=False)
        print(f"  ✅ {len(connected_data):,} connected entities → 2_connected_entities.csv")

        # High-risk subset
        high_risk = connected_df[connected_df['distance'] == 1]
        if len(high_risk) > 0:
            high_risk.to_csv(f'{output_dir}/3_high_risk_1hop.csv', index=False)
            print(f"  ✅ {len(high_risk):,} high-risk (1-hop) → 3_high_risk_1hop.csv")
    else:
        print("  ⚠️  No connected entities to save")

    print(f"\n📊 Results saved to {output_dir}/")


def print_summary(matches, connected, panama_nodes):
    """Print summary of findings"""
    print("\n" + "=" * 80)
    print("NETWORK ANALYSIS SUMMARY")
    print("=" * 80)

    print(f"\n🚨 Sanctioned entities in Panama Papers: {len(matches):,}")

    if matches:
        print("\n  Top 5 matches:")
        for i, match in enumerate(matches[:5], 1):
            print(f"    {i}. {match['name']}")
            print(f"       Type: {match['match_type']} ({match['similarity']}%)")
            print(f"       Country: {match['sanction_country']}")
            print(f"       Dataset: {match.get('sanction_dataset', 'Unknown')}")

    if connected:
        print(f"\n🔗 Connected entities: {len(connected):,}")

        distance_counts = defaultdict(int)
        for data in connected.values():
            distance_counts[data['distance']] += 1

        print("\n  By distance:")
        for dist in sorted(distance_counts.keys()):
            print(f"    {dist}-hop: {distance_counts[dist]:,} entities")

        # Top risk entities
        top_risk = sorted(connected.items(), key=lambda x: x[1]['risk_score'], reverse=True)[:5]

        print("\n  Top 5 highest-risk connected entities:")
        for i, (uri, data) in enumerate(top_risk, 1):
            node = panama_nodes.get(uri, {})
            print(f"    {i}. {node.get('name', 'Unknown')}")
            print(f"       Risk score: {data['risk_score']:.1f}")
            print(f"       Distance: {data['distance']}-hop")
            print(f"       Jurisdiction: {node.get('jurisdiction', 'Unknown')}")


def main():
    """Main execution"""
    print("=" * 80)
    print("NETWORK CONNECTION FINDER - Streaming + Parallel Version")
    print("=" * 80)
    print(f"\nCPU cores: {cpu_count()}")
    print("Handles 9M+ triples efficiently with chunked parsing")

    # File paths
    BASE_PATH = os.path.expanduser("~/PycharmProjects/EcuadorNarcos")

    SANCTIONS_FILE = os.path.join(BASE_PATH, "Open Sanctions Data/combined_sanctions.ttl")
    PANAMA_FILE = os.path.join(BASE_PATH, "panama_papers.ttl")

    # Try alternative file formats
    if not os.path.exists(PANAMA_FILE):
        for ext in ['.nt.gz', '.nt']:
            alt_file = os.path.join(BASE_PATH, f"panama_papers{ext}")
            if os.path.exists(alt_file):
                print(f"\n📝 Using alternative format: {os.path.basename(alt_file)}")
                PANAMA_FILE = alt_file
                break

    # Check files exist
    if not os.path.exists(SANCTIONS_FILE):
        print(f"\n❌ Sanctions file not found: {SANCTIONS_FILE}")
        return

    if not os.path.exists(PANAMA_FILE):
        print(f"\n❌ Panama Papers file not found")
        return

    print(f"\n📁 Files:")
    print(f"  Sanctions: {os.path.getsize(SANCTIONS_FILE) / 1024 / 1024:.1f} MB")
    print(f"  Panama:    {os.path.getsize(PANAMA_FILE) / 1024 / 1024:.1f} MB")

    # Step 1: Load sanctions
    sanctions_names = load_sanctions_streaming(SANCTIONS_FILE)

    if not sanctions_names:
        print("\n❌ No sanctioned entities found")
        return

    # Step 2: Load Panama Papers
    panama_names, panama_nodes, relationships = load_panama_streaming(PANAMA_FILE)

    if not panama_names:
        print("\n❌ No named entities found in Panama Papers")
        return

    # Step 3: Find matches
    matches = find_sanctioned_in_panama(sanctions_names, panama_names, fuzzy_threshold=85)

    if not matches:
        print("\n⚠️  No sanctioned entities found in Panama Papers")
        print("   Try lowering fuzzy_threshold to 75")
        save_results(matches, {}, panama_nodes)
        return

    # Step 4: Build network graph
    graph = build_network_graph(panama_nodes, relationships)

    # Step 5: Find connected entities
    sanctioned_uris = [m['panama_uri'] for m in matches]
    connected = find_connected_entities(graph, sanctioned_uris, max_hops=2)

    # Step 6: Save results
    save_results(matches, connected, panama_nodes)

    # Step 7: Print summary
    print_summary(matches, connected, panama_nodes)

    print("\n✅ Analysis complete!")
    print("   Check network_analysis/ directory for results")


if __name__ == "__main__":
    main()