#!/usr/bin/env python3
"""
Panama Papers CSV to RDF - Complete Pipeline
Features: Direct disk writing, Advanced SHACL, Provenance tracking,
          Motif detection, Forensic queries
"""

import os
import pandas as pd
from uuid import uuid4
from rdflib import Graph, Namespace, Literal, RDF, RDFS, OWL, XSD
from rdflib.namespace import FOAF, SH
from tqdm import tqdm
import random
import re
import numpy as np
import json
from datetime import datetime
import gzip
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import glob
import shutil
import networkx as nx

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_PATH = os.path.expanduser("~/PycharmProjects/EcuadorNarcos")
DATA_DIR = os.path.join(BASE_PATH, "Panama Papers Data")
TEMP_DIR = os.path.join(BASE_PATH, "temp_chunks")

ENTITIES_FILE = os.path.join(DATA_DIR, "nodes-entities.csv")
OFFICERS_FILE = os.path.join(DATA_DIR, "nodes-officers.csv")
ADDRESSES_FILE = os.path.join(DATA_DIR, "nodes-addresses.csv")
RELATIONSHIPS_FILE = os.path.join(DATA_DIR, "relationships.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "panama_papers.nt.gz")
COMPLIANCE_REPORT_FILE = os.path.join(BASE_PATH, "compliance_report.json")
MOTIF_REPORT_FILE = os.path.join(BASE_PATH, "motif_analysis.json")
FORENSIC_QUERIES_FILE = os.path.join(BASE_PATH, "forensic_queries.sparql")

VALIDATE_SAMPLE_PERCENT = 1
USE_EMBEDDINGS = True
SIMILARITY_THRESHOLD = 0.65
USE_GZIP = True
ENABLE_PROVENANCE = True  # Track data lineage

NUM_WORKERS = cpu_count()
CHUNK_SIZE = 50000

# ============================================================================
# GLOBAL STATE
# ============================================================================

node_id_to_uri = {}

embedding_model = None
canonical_embeddings = None
embedding_cache = {}
cosine_similarity_func = None

vocab = Namespace("https://panamapapers.org/vocab/")
agent = Namespace("https://panamapapers.org/agent/")
addresses = Namespace("https://panamapapers.org/address/")
prov = Namespace("http://www.w3.org/ns/prov#")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def escape_literal(value):
    """Escape string for N-Triples literal"""
    return str(value).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')


def triple_to_nt(s, p, o, o_type='uri'):
    """Convert triple to N-Triples string"""
    if o_type == 'literal':
        return f'<{s}> <{p}> "{escape_literal(o)}" .\n'
    else:
        return f'<{s}> <{p}> <{o}> .\n'


def triple_to_nt_with_provenance(s, p, o, o_type='uri', source_file='', row_num=0):
    """Convert triple to N-Triples with provenance metadata"""
    # Main triple
    lines = [triple_to_nt(s, p, o, o_type)]

    if ENABLE_PROVENANCE:
        # Create provenance statement
        statement_uri = f"https://panamapapers.org/statement/{uuid4()}"
        lines.append(triple_to_nt(statement_uri, str(RDF.type), str(vocab.ProvenanceStatement)))
        lines.append(triple_to_nt(statement_uri, str(prov.wasDerivedFrom), source_file, 'literal'))
        lines.append(triple_to_nt(statement_uri, str(vocab.sourceRow), str(row_num), 'literal'))
        lines.append(triple_to_nt(statement_uri, str(prov.generatedAtTime), datetime.now().isoformat(), 'literal'))
        lines.append(triple_to_nt(statement_uri, str(vocab.about), s))

    return ''.join(lines)


# ============================================================================
# RELATIONSHIP MATCHING
# ============================================================================

REGEX_RULES = [
    (r'\bbeneficial\s+owner\b', 'beneficial_owner', 'beneficial owner'),
    (r'\bUBO\b', 'beneficial_owner', 'UBO'),
    (r'\bultimate\s+owner\b', 'beneficial_owner', 'ultimate owner'),
    (r'\bowner\b', 'owns', 'owner'),
    (r'\bown(?:s|ed|er)?\b', 'owns', 'owns/owned'),
    (r'\bunderlying\b', 'owns', 'underlying ownership'),
    (r'\bshareholder\b', 'shareholder', 'shareholder'),
    (r'\bshare\s*holder\b', 'shareholder', 'share holder'),
    (r'\bstockholder\b', 'shareholder', 'stockholder'),
    (r'\bdirector\b', 'director', 'director'),
    (r'\bboard\s+member\b', 'director', 'board member'),
    (r'\bofficer(?:_of)?\b', 'officer', 'officer'),
    (r'\bsecretary\b', 'secretary', 'secretary'),
    (r'\btrustee\b', 'officer', 'trustee'),
    (r'\bprotector\b', 'officer', 'protector'),
    (r'\bauthori[zs]ed\s+signatory\b', 'officer', 'authorized signatory'),
    (r'\bregistered[_\s]address\b', 'address', 'registered address'),
    (r'\bregistered[_\s]office\b', 'address', 'registered office'),
    (r'\b(?:of\s+)?address\b', 'address', 'address'),
    (r'\bintermediar', 'intermediary', 'intermediary'),
    (r'\bnominee\b', 'intermediary', 'nominee'),
    (r'\bsame[_\s](?:as|address[_\s]as|company[_\s]as|id[_\s]as|intermediary[_\s]as|name[_\s]as)\b', 'same_as',
     'entity equivalence'),
    (r'\bprobably[_\s]same[_\s]officer[_\s]as\b', 'same_as', 'probable officer match'),
    (r'\bsimilar(?:[_\s]company[_\s]as)?\b', 'similar', 'similarity link'),
    (r'\bconnected[_\s]to\b', 'generic', 'connected'),
]


def match_with_regex(rel_type_str):
    if not rel_type_str:
        return None, None
    rel_lower = rel_type_str.lower().strip()
    for pattern, rel_category, description in REGEX_RULES:
        if re.search(pattern, rel_lower):
            return rel_category, f"regex: {description}"
    return None, None


CANONICAL_TYPES = {
    'director of': 'director',
    'officer of': 'officer',
    'shareholder of': 'shareholder',
    'beneficial owner': 'beneficial_owner',
    'UBO': 'beneficial_owner',
    'owner of': 'owns',
    'registered address': 'address',
    'intermediary': 'intermediary',
    'secretary': 'secretary',
}


def initialize_embeddings():
    global embedding_model, canonical_embeddings, cosine_similarity_func
    if embedding_model is not None:
        return
    try:
        from sentence_transformers import SentenceTransformer
        from sklearn.metrics.pairwise import cosine_similarity
        print("   [INFO] Loading sentence transformer model...")
        embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        cosine_similarity_func = cosine_similarity
        canonical_texts = list(CANONICAL_TYPES.keys())
        canonical_embeddings = embedding_model.encode(canonical_texts)
        print("   [SUCCESS] Embedding model ready")
    except ImportError:
        print("   [WARNING] sentence-transformers not installed, using regex only")
        embedding_model = "disabled"


def match_with_embeddings(rel_type_str, threshold=SIMILARITY_THRESHOLD):
    global embedding_cache
    if embedding_model == "disabled" or embedding_model is None:
        return None, None
    if not rel_type_str:
        return None, None
    rel_lower = rel_type_str.lower().strip()
    if rel_lower in embedding_cache:
        return embedding_cache[rel_lower]
    csv_embedding = embedding_model.encode([rel_lower])
    canonical_texts = list(CANONICAL_TYPES.keys())
    similarities = cosine_similarity_func(csv_embedding, canonical_embeddings)[0]
    best_idx = np.argmax(similarities)
    best_score = similarities[best_idx]
    best_canonical = canonical_texts[best_idx]
    if best_score >= threshold:
        result = (CANONICAL_TYPES[best_canonical], f"embedding: {best_canonical} ({best_score:.2f})")
    else:
        result = (None, None)
    embedding_cache[rel_lower] = result
    return result


def match_relationship_hybrid(rel_type_str):
    if not rel_type_str:
        return 'generic', "no_type"
    rel_category, method = match_with_regex(rel_type_str)
    if rel_category:
        return rel_category, method
    if USE_EMBEDDINGS and embedding_model is not None:
        rel_category, method = match_with_embeddings(rel_type_str)
        if rel_category:
            return rel_category, method
    return 'generic', f"unknown: {rel_type_str[:50]}"


# ============================================================================
# DIRECT FILE WRITING - PARALLEL WORKERS
# ============================================================================

def process_entity_chunk_to_file(chunk_data):
    """Process entity chunk - write directly to file with provenance"""
    chunk_df, namespace, chunk_id = chunk_data

    opener = gzip.open if USE_GZIP else open
    ext = '.nt.gz' if USE_GZIP else '.nt'
    output_file = os.path.join(TEMP_DIR, f"entities_{chunk_id}{ext}")
    local_mapping = {}

    source_file = f"nodes-entities.csv#chunk{chunk_id}"

    with opener(output_file, 'wt', encoding='utf-8') as f:
        for idx, row in enumerate(chunk_df.itertuples()):
            entity_uri = str(namespace[str(uuid4())])
            local_mapping[str(row.node_id)] = entity_uri

            f.write(triple_to_nt_with_provenance(
                entity_uri, str(RDF.type), str(vocab.Organization),
                source_file=source_file, row_num=idx
            ))
            f.write(triple_to_nt(entity_uri, str(RDF.type), str(vocab.Agent)))

            if pd.notna(row.name):
                f.write(triple_to_nt_with_provenance(
                    entity_uri, str(FOAF.name), str(row.name), 'literal',
                    source_file=source_file, row_num=idx
                ))
            if pd.notna(row.jurisdiction):
                f.write(triple_to_nt_with_provenance(
                    entity_uri, str(vocab.jurisdiction), str(row.jurisdiction), 'literal',
                    source_file=source_file, row_num=idx
                ))

    return output_file, local_mapping


def load_entities_to_disk(file_path):
    """Load entities - write directly to disk files"""
    print("\n1. Loading entities (parallel write to disk with provenance)...")
    df = pd.read_csv(file_path, dtype=str, low_memory=False)

    os.makedirs(TEMP_DIR, exist_ok=True)

    chunks = [df.iloc[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]
    chunk_data = [(chunk, agent, i) for i, chunk in enumerate(chunks)]

    output_files = []

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(process_entity_chunk_to_file, data): i for i, data in enumerate(chunk_data)}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Writing"):
            output_file, local_mapping = future.result()
            output_files.append(output_file)
            node_id_to_uri.update(local_mapping)

    print(f"   Done! Wrote {len(df)} organizations to {len(output_files)} files")
    return len(df), output_files


def process_officer_chunk_to_file(chunk_data):
    """Process officer chunk - write directly to file with provenance"""
    chunk_df, namespace, chunk_id = chunk_data

    opener = gzip.open if USE_GZIP else open
    ext = '.nt.gz' if USE_GZIP else '.nt'
    output_file = os.path.join(TEMP_DIR, f"officers_{chunk_id}{ext}")
    local_mapping = {}

    source_file = f"nodes-officers.csv#chunk{chunk_id}"

    with opener(output_file, 'wt', encoding='utf-8') as f:
        for idx, row in enumerate(chunk_df.itertuples()):
            officer_uri = str(namespace[str(uuid4())])
            local_mapping[str(row.node_id)] = officer_uri

            f.write(triple_to_nt_with_provenance(
                officer_uri, str(RDF.type), str(vocab.Person),
                source_file=source_file, row_num=idx
            ))
            f.write(triple_to_nt(officer_uri, str(RDF.type), str(vocab.Agent)))

            if pd.notna(row.name):
                f.write(triple_to_nt_with_provenance(
                    officer_uri, str(FOAF.name), str(row.name), 'literal',
                    source_file=source_file, row_num=idx
                ))

    return output_file, local_mapping


def load_officers_to_disk(file_path):
    """Load officers - write directly to disk files"""
    print("\n2. Loading officers (parallel write to disk with provenance)...")
    df = pd.read_csv(file_path, dtype=str, low_memory=False)

    chunks = [df.iloc[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]
    chunk_data = [(chunk, agent, i) for i, chunk in enumerate(chunks)]

    output_files = []

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(process_officer_chunk_to_file, data): i for i, data in enumerate(chunk_data)}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Writing"):
            output_file, local_mapping = future.result()
            output_files.append(output_file)
            node_id_to_uri.update(local_mapping)

    print(f"   Done! Wrote {len(df)} persons to {len(output_files)} files")
    return len(df), output_files


def process_address_chunk_to_file(chunk_data):
    """Process address chunk - write directly to file with provenance"""
    chunk_df, namespace, chunk_id = chunk_data

    opener = gzip.open if USE_GZIP else open
    ext = '.nt.gz' if USE_GZIP else '.nt'
    output_file = os.path.join(TEMP_DIR, f"addresses_{chunk_id}{ext}")
    local_mapping = {}

    source_file = f"nodes-addresses.csv#chunk{chunk_id}"

    with opener(output_file, 'wt', encoding='utf-8') as f:
        for idx, row in enumerate(chunk_df.itertuples()):
            address_uri = str(namespace[str(uuid4())])
            local_mapping[str(row.node_id)] = address_uri

            f.write(triple_to_nt_with_provenance(
                address_uri, str(RDF.type), str(vocab.Address),
                source_file=source_file, row_num=idx
            ))

            if pd.notna(row.address):
                f.write(triple_to_nt_with_provenance(
                    address_uri, str(RDFS.label), str(row.address), 'literal',
                    source_file=source_file, row_num=idx
                ))

    return output_file, local_mapping


def load_addresses_to_disk(file_path):
    """Load addresses - write directly to disk files"""
    print("\n3. Loading addresses (parallel write to disk with provenance)...")
    df = pd.read_csv(file_path, dtype=str, low_memory=False)

    chunks = [df.iloc[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]
    chunk_data = [(chunk, addresses, i) for i, chunk in enumerate(chunks)]

    output_files = []

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(process_address_chunk_to_file, data): i for i, data in enumerate(chunk_data)}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Writing"):
            output_file, local_mapping = future.result()
            output_files.append(output_file)
            node_id_to_uri.update(local_mapping)

    print(f"   Done! Wrote {len(df)} addresses to {len(output_files)} files")
    return len(df), output_files


def process_relationship_chunk_to_file(chunk_data):
    """Process relationship chunk - write directly to file"""
    chunk_df, node_mapping, rel_type_column, chunk_id = chunk_data

    opener = gzip.open if USE_GZIP else open
    ext = '.nt.gz' if USE_GZIP else '.nt'
    output_file = os.path.join(TEMP_DIR, f"relationships_{chunk_id}{ext}")

    role_counts = {}
    skipped = 0
    source_file = f"relationships.csv#chunk{chunk_id}"

    with opener(output_file, 'wt', encoding='utf-8') as f:
        for idx, row in enumerate(chunk_df.itertuples()):
            start_id = str(row.node_id_start)
            end_id = str(row.node_id_end)
            start_uri = node_mapping.get(start_id)
            end_uri = node_mapping.get(end_id)

            if not start_uri or not end_uri:
                skipped += 1
                continue

            rel_type_value = None
            if rel_type_column and hasattr(row, rel_type_column):
                val = getattr(row, rel_type_column)
                if pd.notna(val):
                    rel_type_value = str(val)

            rel_category, match_method = match_relationship_hybrid(rel_type_value)

            if rel_category in ['director', 'shareholder', 'beneficial_owner', 'officer', 'secretary']:
                assignment_uri = f"https://panamapapers.org/agent/{uuid4()}"
                f.write(triple_to_nt_with_provenance(
                    assignment_uri, str(RDF.type), str(vocab.RoleAssignment),
                    source_file=source_file, row_num=idx
                ))
                f.write(triple_to_nt(assignment_uri, str(vocab.agent), start_uri))
                f.write(triple_to_nt(assignment_uri, str(vocab.inOrganization), end_uri))

                if rel_category == 'director':
                    f.write(triple_to_nt(assignment_uri, str(vocab.role), str(vocab.DirectorRole)))
                    role_counts['DirectorRole'] = role_counts.get('DirectorRole', 0) + 1
                elif rel_category == 'shareholder':
                    f.write(triple_to_nt(assignment_uri, str(vocab.role), str(vocab.ShareholderRole)))
                    role_counts['ShareholderRole'] = role_counts.get('ShareholderRole', 0) + 1
                elif rel_category == 'beneficial_owner':
                    f.write(triple_to_nt(assignment_uri, str(vocab.role), str(vocab.BeneficialOwnerRole)))
                    role_counts['BeneficialOwnerRole'] = role_counts.get('BeneficialOwnerRole', 0) + 1
                elif rel_category == 'secretary':
                    f.write(triple_to_nt(assignment_uri, str(vocab.role), str(vocab.SecretaryRole)))
                    role_counts['SecretaryRole'] = role_counts.get('SecretaryRole', 0) + 1
                else:
                    f.write(triple_to_nt(assignment_uri, str(vocab.role), str(vocab.OfficerRole)))
                    role_counts['OfficerRole'] = role_counts.get('OfficerRole', 0) + 1
            elif rel_category == 'address':
                f.write(triple_to_nt_with_provenance(
                    start_uri, str(vocab.hasRegisteredAddress), end_uri,
                    source_file=source_file, row_num=idx
                ))
                role_counts['address'] = role_counts.get('address', 0) + 1
            elif rel_category == 'owns':
                f.write(triple_to_nt_with_provenance(
                    start_uri, str(vocab.owns), end_uri,
                    source_file=source_file, row_num=idx
                ))
                role_counts['owns'] = role_counts.get('owns', 0) + 1
            elif rel_category == 'intermediary':
                f.write(triple_to_nt(start_uri, str(vocab.intermediaryFor), end_uri))
                role_counts['intermediary'] = role_counts.get('intermediary', 0) + 1
            elif rel_category == 'same_as':
                f.write(triple_to_nt(start_uri, str(OWL.sameAs), end_uri))
                role_counts['same_as'] = role_counts.get('same_as', 0) + 1
            elif rel_category == 'similar':
                f.write(triple_to_nt(start_uri, str(vocab.similarTo), end_uri))
                role_counts['similar'] = role_counts.get('similar', 0) + 1
            else:
                f.write(triple_to_nt(start_uri, str(vocab.connected_to), end_uri))
                role_counts['generic'] = role_counts.get('generic', 0) + 1

    return output_file, role_counts, skipped


def load_relationships_to_disk(file_path):
    """Load relationships - write directly to disk files"""
    print("\n4. Loading relationships (parallel write to disk)...")
    df = pd.read_csv(file_path, dtype=str, low_memory=False)

    rel_type_column = None
    for col in df.columns:
        if col.lower() in ['rel_type', 'type', 'relationship_type', 'link']:
            rel_type_column = col
            break

    if rel_type_column:
        print(f"   [SUCCESS] Found relationship type column: {rel_type_column}")
        unique_types = set(df[rel_type_column].dropna().unique())
        print(f"   Found {len(unique_types)} unique relationship types")

        unknowns = []
        for rel_type in unique_types:
            cat, _ = match_with_regex(str(rel_type).lower().strip())
            if not cat:
                unknowns.append(str(rel_type).lower().strip())

        if unknowns:
            print(f"   Regex unmatched: {len(unknowns)} types")
            if USE_EMBEDDINGS:
                initialize_embeddings()

    chunks = [df.iloc[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]
    chunk_data = [(chunk, node_id_to_uri, rel_type_column, i) for i, chunk in enumerate(chunks)]

    output_files = []
    total_role_counts = {}
    total_skipped = 0

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(process_relationship_chunk_to_file, data): i for i, data in enumerate(chunk_data)}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Writing"):
            output_file, role_counts, skipped = future.result()
            output_files.append(output_file)
            total_skipped += skipped

            for role, count in role_counts.items():
                total_role_counts[role] = total_role_counts.get(role, 0) + count

    print(f"\n   Relationship distribution:")
    for role, count in sorted(total_role_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"     {role}: {count:,}")

    if total_skipped:
        print(f"   Skipped {total_skipped} (missing nodes)")

    return sum(total_role_counts.values()), output_files


# ============================================================================
# CONCATENATE FILES
# ============================================================================

def concatenate_files(output_path):
    """Concatenate all chunk files into final output"""
    print("\n5. Concatenating all chunk files...")

    pattern = os.path.join(TEMP_DIR, "*.nt*")
    chunk_files = sorted(glob.glob(pattern))

    print(f"   Found {len(chunk_files)} chunk files")

    opener_out = gzip.open if USE_GZIP else open
    mode_out = 'wb' if USE_GZIP else 'w'

    total_lines = 0

    with opener_out(output_path, mode_out) as outfile:
        for chunk_file in tqdm(chunk_files, desc="Merging"):
            opener_in = gzip.open if chunk_file.endswith('.gz') else open
            mode_in = 'rb' if chunk_file.endswith('.gz') else 'r'

            with opener_in(chunk_file, mode_in) as infile:
                if USE_GZIP:
                    outfile.write(infile.read())
                else:
                    shutil.copyfileobj(infile, outfile)

            opener_count = gzip.open if chunk_file.endswith('.gz') else open
            with opener_count(chunk_file, 'rt') as f:
                total_lines += sum(1 for _ in f)

    print(f"   Cleaning up temp files...")
    shutil.rmtree(TEMP_DIR)

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"\n   [SUCCESS] Created {output_path}")
    print(f"   Total triples: {total_lines:,}")
    print(f"   File size: {size_mb:.1f} MB")

    return total_lines


# ============================================================================
# NETWORK MOTIF DETECTION
# ============================================================================

def load_sample_into_networkx(output_path, sample_size=10000):
    """Load a sample of triples into NetworkX for motif detection"""
    print("\n   Loading sample into NetworkX...")

    G = nx.DiGraph()
    node_types = {}
    node_names = {}

    opener = gzip.open if output_path.endswith('.gz') else open

    with opener(output_path, 'rt') as f:
        total_lines = sum(1 for _ in f)

    step = max(1, total_lines // sample_size)

    with opener(output_path, 'rt') as f:
        for i, line in enumerate(f):
            if i % step == 0:
                parts = line.strip().split()
                if len(parts) >= 3:
                    s = parts[0].strip('<>')
                    p = parts[1].strip('<>')
                    o = parts[2].strip('<>')

                    # Track node types
                    if 'type' in p:
                        if 'Organization' in o:
                            node_types[s] = 'Organization'
                        elif 'Person' in o:
                            node_types[s] = 'Person'

                    # Track names
                    if 'name' in p and len(parts) >= 4:
                        name = ' '.join(parts[2:]).strip('"')
                        node_names[s] = name[:50]

                    # Add edges for ownership/control relationships
                    if any(rel in p for rel in ['owns', 'intermediaryFor', 'connected_to']):
                        G.add_edge(s, o, relation=p)

            if len(G.nodes()) >= sample_size:
                break

    print(f"   Loaded {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")

    return G, node_types, node_names


def get_node_name(node_uri, node_names):
    """Get readable name for node"""
    if node_uri in node_names:
        return node_names[node_uri]
    return node_uri.split('/')[-1][:30]


def is_tax_haven_jurisdiction(node_uri, G):
    """Check if node is in a tax haven (simplified check)"""
    # In real implementation, would check jurisdiction property
    return 'cayman' in node_uri.lower() or 'virgin' in node_uri.lower() or 'panama' in node_uri.lower()


def detect_money_laundering_motifs(output_path, sample_size=10000):
    """Detect money laundering patterns in network"""
    print("\n" + "=" * 70)
    print("NETWORK MOTIF DETECTION: Money Laundering Patterns")
    print("=" * 70)

    G, node_types, node_names = load_sample_into_networkx(output_path, sample_size)

    motifs_found = {
        'layering': [],
        'smurfing': [],
        'round_tripping': [],
        'nominee_cluster': [],
        'daisy_chain': []
    }

    print("\n   Detecting layering patterns (multi-hop chains)...")
    nodes_to_check = list(G.nodes())[:min(500, len(G.nodes()))]
    for source in nodes_to_check:
        for target in G.nodes():
            if source != target:
                try:
                    paths = list(nx.all_simple_paths(G, source, target, cutoff=6))
                    for path in paths[:3]:
                        if len(path) >= 5:
                            motifs_found['layering'].append({
                                'pattern': 'Layering',
                                'length': len(path),
                                'chain': [get_node_name(n, node_names) for n in path],
                                'risk': 'HIGH' if len(path) >= 7 else 'MEDIUM'
                            })
                except:
                    continue

            if len(motifs_found['layering']) >= 20:
                break
        if len(motifs_found['layering']) >= 20:
            break

    print("   Detecting smurfing patterns (high in-degree)...")
    for node in G.nodes():
        in_degree = G.in_degree(node)
        if in_degree >= 20:
            sources = list(G.predecessors(node))
            motifs_found['smurfing'].append({
                'pattern': 'Smurfing',
                'target': get_node_name(node, node_names),
                'source_count': in_degree,
                'sample_sources': [get_node_name(s, node_names) for s in sources[:5]],
                'risk': 'CRITICAL' if in_degree >= 50 else 'HIGH'
            })

    print("   Detecting nominee clusters (high out-degree from persons)...")
    for node in G.nodes():
        if node_types.get(node) == 'Person':
            out_degree = G.out_degree(node)
            if out_degree >= 30:
                motifs_found['nominee_cluster'].append({
                    'pattern': 'Nominee Service Cluster',
                    'nominee': get_node_name(node, node_names),
                    'entity_count': out_degree,
                    'risk': 'HIGH'
                })

    print("   Detecting round-tripping (bidirectional flows)...")
    for node in G.nodes():
        for neighbor in G.neighbors(node):
            if G.has_edge(neighbor, node):
                if is_tax_haven_jurisdiction(neighbor, G):
                    motifs_found['round_tripping'].append({
                        'pattern': 'Round-Tripping',
                        'domestic': get_node_name(node, node_names),
                        'offshore': get_node_name(neighbor, node_names),
                        'risk': 'CRITICAL'
                    })

    print("   Detecting daisy chains (circular transactions)...")
    try:
        cycles = list(nx.simple_cycles(G))
        for cycle in cycles[:50]:
            if 3 <= len(cycle) <= 5:
                motifs_found['daisy_chain'].append({
                    'pattern': 'Daisy Chain',
                    'participants': [get_node_name(n, node_names) for n in cycle],
                    'length': len(cycle),
                    'risk': 'HIGH'
                })
    except:
        print("   WARNING: Cycle detection skipped (graph too large)")

    # Print summary
    print("\n   Summary:")
    total_motifs = 0
    for motif_type, instances in motifs_found.items():
        if instances:
            print(f"\n   {motif_type.upper()}: {len(instances)} instances")
            critical = sum(1 for i in instances if i.get('risk') == 'CRITICAL')
            high = sum(1 for i in instances if i.get('risk') == 'HIGH')
            print(f"      Critical: {critical}, High: {high}")
            total_motifs += len(instances)

    # Export report
    with open(MOTIF_REPORT_FILE, 'w') as f:
        json.dump({
            'metadata': {
                'analysis_type': 'Network Motif Detection',
                'generated': datetime.now().isoformat(),
                'sample_size': sample_size,
                'nodes_analyzed': G.number_of_nodes(),
                'edges_analyzed': G.number_of_edges()
            },
            'motifs': motifs_found,
            'summary': {
                'total_motifs_found': total_motifs,
                'critical_count': sum(
                    sum(1 for i in instances if i.get('risk') == 'CRITICAL') for instances in motifs_found.values()),
                'high_count': sum(
                    sum(1 for i in instances if i.get('risk') == 'HIGH') for instances in motifs_found.values())
            }
        }, f, indent=2)

    print(f"\n   [SUCCESS] Motif analysis saved to: {MOTIF_REPORT_FILE}")

    return motifs_found


# ============================================================================
# FORENSIC QUERY LIBRARY
# ============================================================================

FORENSIC_QUERIES = {
    'shell_companies': """
# Find shell companies with no officers in tax havens
PREFIX vocab: <https://panamapapers.org/vocab/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?company ?name ?jurisdiction
WHERE {
    ?company a vocab:Organization ;
             foaf:name ?name ;
             vocab:jurisdiction ?jurisdiction .
    FILTER NOT EXISTS { 
        ?assignment vocab:inOrganization ?company 
    }
    FILTER(
        CONTAINS(LCASE(?jurisdiction), 'cayman') || 
        CONTAINS(LCASE(?jurisdiction), 'virgin') ||
        CONTAINS(LCASE(?jurisdiction), 'panama') ||
        CONTAINS(LCASE(?jurisdiction), 'seychelles')
    )
}
LIMIT 100
""",

    'hidden_beneficial_owners': """
# Find ultimate beneficial owners through ownership chains
PREFIX vocab: <https://panamapapers.org/vocab/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?personName ?company ?companyName
WHERE {
    ?person a vocab:Person ;
            foaf:name ?personName .
    ?person vocab:owns+ ?company .
    ?company a vocab:Organization ;
             foaf:name ?companyName ;
             vocab:jurisdiction ?jurisdiction .
    FILTER(
        CONTAINS(LCASE(?jurisdiction), 'cayman') || 
        CONTAINS(LCASE(?jurisdiction), 'virgin')
    )
}
LIMIT 100
""",

    'nominee_services': """
# Find professional nominee services (one person serving many entities)
PREFIX vocab: <https://panamapapers.org/vocab/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name (COUNT(DISTINCT ?company) as ?entityCount)
WHERE {
    ?assignment vocab:agent ?person ;
               vocab:inOrganization ?company .
    ?person a vocab:Person ;
            foaf:name ?name .
}
GROUP BY ?person ?name
HAVING(COUNT(DISTINCT ?company) > 50)
ORDER BY DESC(?entityCount)
LIMIT 50
""",

    'round_tripping': """
# Find round-tripping patterns (domestic -> offshore -> domestic)
PREFIX vocab: <https://panamapapers.org/vocab/>

SELECT ?domestic ?offshore
WHERE {
    ?domestic vocab:owns ?offshore .
    ?offshore vocab:owns ?domestic2 .
    ?offshore vocab:jurisdiction ?juris .
    FILTER(
        CONTAINS(LCASE(?juris), 'cayman') ||
        CONTAINS(LCASE(?juris), 'virgin')
    )
    FILTER(?domestic = ?domestic2)
}
LIMIT 100
""",

    'multi_jurisdiction_owners': """
# Find individuals with entities in multiple tax havens
PREFIX vocab: <https://panamapapers.org/vocab/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name (COUNT(DISTINCT ?jurisdiction) as ?jurisdictionCount)
WHERE {
    ?assignment vocab:agent ?person ;
               vocab:inOrganization ?company .
    ?person a vocab:Person ;
            foaf:name ?name .
    ?company vocab:jurisdiction ?jurisdiction .
    FILTER(
        CONTAINS(LCASE(?jurisdiction), 'cayman') || 
        CONTAINS(LCASE(?jurisdiction), 'virgin') ||
        CONTAINS(LCASE(?jurisdiction), 'panama') ||
        CONTAINS(LCASE(?jurisdiction), 'bahamas') ||
        CONTAINS(LCASE(?jurisdiction), 'bermuda')
    )
}
GROUP BY ?person ?name
HAVING(COUNT(DISTINCT ?jurisdiction) >= 3)
ORDER BY DESC(?jurisdictionCount)
LIMIT 50
""",

    'missing_beneficial_owners': """
# Find tax haven entities lacking beneficial owner disclosure
PREFIX vocab: <https://panamapapers.org/vocab/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?company ?name ?jurisdiction
WHERE {
    ?company a vocab:Organization ;
             foaf:name ?name ;
             vocab:jurisdiction ?jurisdiction .
    FILTER NOT EXISTS {
        ?assignment vocab:inOrganization ?company ;
                   vocab:role vocab:BeneficialOwnerRole .
    }
    FILTER(
        CONTAINS(LCASE(?jurisdiction), 'cayman') || 
        CONTAINS(LCASE(?jurisdiction), 'virgin') ||
        CONTAINS(LCASE(?jurisdiction), 'panama')
    )
}
LIMIT 100
""",

    'complex_ownership_chains': """
# Find entities with complex multi-layer ownership structures
PREFIX vocab: <https://panamapapers.org/vocab/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?entity ?name (COUNT(?intermediate) as ?layers)
WHERE {
    ?entity a vocab:Organization ;
            foaf:name ?name .
    ?entity vocab:owns+ ?intermediate .
    ?intermediate vocab:owns+ ?final .
}
GROUP BY ?entity ?name
HAVING(COUNT(?intermediate) >= 3)
ORDER BY DESC(?layers)
LIMIT 50
""",

    'intermediary_concentration': """
# Find addresses serving as hubs for multiple entities (registered office concentration)
PREFIX vocab: <https://panamapapers.org/vocab/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?address ?label (COUNT(?company) as ?companyCount)
WHERE {
    ?company vocab:hasRegisteredAddress ?address .
    ?address rdfs:label ?label .
}
GROUP BY ?address ?label
HAVING(COUNT(?company) >= 20)
ORDER BY DESC(?companyCount)
LIMIT 50
"""
}


def export_forensic_queries():
    """Export forensic query library to file"""
    print("\n" + "=" * 70)
    print("FORENSIC QUERY LIBRARY")
    print("=" * 70)

    with open(FORENSIC_QUERIES_FILE, 'w') as f:
        f.write("# Panama Papers Forensic Query Library\n")
        f.write("# Generated: " + datetime.now().isoformat() + "\n")
        f.write("# Usage: Load panama_papers.nt.gz into a SPARQL endpoint\n")
        f.write("#        Then execute these queries for investigations\n\n")
        f.write("=" * 70 + "\n\n")

        for query_name, query in FORENSIC_QUERIES.items():
            f.write(f"\n{'=' * 70}\n")
            f.write(f"QUERY: {query_name}\n")
            f.write(f"{'=' * 70}\n")
            f.write(query)
            f.write("\n\n")

    print(f"   [SUCCESS] Exported {len(FORENSIC_QUERIES)} forensic queries")
    print(f"   File: {FORENSIC_QUERIES_FILE}")
    print("\n   Available queries:")
    for i, query_name in enumerate(FORENSIC_QUERIES.keys(), 1):
        print(f"      {i}. {query_name}")

    print("\n   Usage instructions:")
    print("      1. Load panama_papers.nt.gz into Apache Jena Fuseki or Blazegraph")
    print("      2. Open the SPARQL query interface")
    print("      3. Copy and execute queries from forensic_queries.sparql")
    print("      4. Queries are optimized for financial crime investigations")


# ============================================================================
# ADVANCED SHACL VALIDATION
# ============================================================================

def add_advanced_shacl(g):
    """Add comprehensive SHACL validation rules"""
    print("   [INFO] Adding advanced SHACL validation rules...")

    # Basic property validation
    org_shape = vocab.OrganizationShape
    g.add((org_shape, RDF.type, SH.NodeShape))
    g.add((org_shape, SH.targetClass, vocab.Organization))

    name_prop = vocab.OrgNameProperty
    g.add((org_shape, SH.property, name_prop))
    g.add((name_prop, SH.path, FOAF.name))
    g.add((name_prop, SH.minCount, Literal(1)))
    g.add((name_prop, SH.datatype, XSD.string))
    g.add((name_prop, SH.message, Literal("Organization must have a name")))
    g.add((name_prop, SH.severity, SH.Violation))

    person_shape = vocab.PersonShape
    g.add((person_shape, RDF.type, SH.NodeShape))
    g.add((person_shape, SH.targetClass, vocab.Person))

    person_name_prop = vocab.PersonNameProperty
    g.add((person_shape, SH.property, person_name_prop))
    g.add((person_name_prop, SH.path, FOAF.name))
    g.add((person_name_prop, SH.minCount, Literal(1)))
    g.add((person_name_prop, SH.datatype, XSD.string))
    g.add((person_name_prop, SH.message, Literal("Person must have a name")))

    address_shape = vocab.AddressShape
    g.add((address_shape, RDF.type, SH.NodeShape))
    g.add((address_shape, SH.targetClass, vocab.Address))

    address_label_prop = vocab.AddressLabelProperty
    g.add((address_shape, SH.property, address_label_prop))
    g.add((address_label_prop, SH.path, RDFS.label))
    g.add((address_label_prop, SH.minCount, Literal(1)))
    g.add((address_label_prop, SH.message, Literal("Address must have a label")))

    # Advanced SPARQL-based constraints (correct syntax)

    # Rule 1: Tax Haven Without Beneficial Owner
    tax_haven_rule = vocab.TaxHavenBeneficialOwnerRule
    g.add((tax_haven_rule, RDF.type, SH.NodeShape))
    g.add((tax_haven_rule, SH.targetClass, vocab.Organization))

    # Create blank node for SPARQL constraint
    sparql_constraint_1 = vocab.TaxHavenSPARQLConstraint
    g.add((tax_haven_rule, SH.sparql, sparql_constraint_1))
    g.add((sparql_constraint_1, RDF.type, SH.SPARQLConstraint))

    tax_haven_sparql = """
        PREFIX vocab: <https://panamapapers.org/vocab/>
        SELECT $this
        WHERE {
            $this vocab:jurisdiction ?jurisdiction .
            FILTER(
                CONTAINS(LCASE(?jurisdiction), 'panama') ||
                CONTAINS(LCASE(?jurisdiction), 'cayman') ||
                CONTAINS(LCASE(?jurisdiction), 'virgin') ||
                CONTAINS(LCASE(?jurisdiction), 'bermuda') ||
                CONTAINS(LCASE(?jurisdiction), 'bahamas')
            )
            FILTER NOT EXISTS {
                ?assignment vocab:inOrganization $this ;
                           vocab:role vocab:BeneficialOwnerRole .
            }
        }
    """
    g.add((sparql_constraint_1, SH.select, Literal(tax_haven_sparql)))
    g.add((sparql_constraint_1, SH.message,
           Literal("[HIGH RISK] Tax haven entity lacks beneficial owner (AML violation)")))
    g.add((tax_haven_rule, vocab.riskScore, Literal(95)))
    g.add((tax_haven_rule, vocab.complianceRule, Literal("FATF Recommendation 24")))

    # Rule 2: Circular Ownership
    circular_rule = vocab.CircularOwnershipRule
    g.add((circular_rule, RDF.type, SH.NodeShape))
    g.add((circular_rule, SH.targetClass, vocab.Organization))

    sparql_constraint_2 = vocab.CircularSPARQLConstraint
    g.add((circular_rule, SH.sparql, sparql_constraint_2))
    g.add((sparql_constraint_2, RDF.type, SH.SPARQLConstraint))

    circular_sparql = """
        PREFIX vocab: <https://panamapapers.org/vocab/>
        SELECT $this
        WHERE {
            $this vocab:owns+ ?intermediate .
            ?intermediate vocab:owns+ $this .
            FILTER($this != ?intermediate)
        }
    """
    g.add((sparql_constraint_2, SH.select, Literal(circular_sparql)))
    g.add((sparql_constraint_2, SH.message, Literal("[CRITICAL] Circular ownership detected")))
    g.add((circular_rule, vocab.riskScore, Literal(100)))

    # Rule 3: Shell Company
    shell_rule = vocab.ShellCompanyRule
    g.add((shell_rule, RDF.type, SH.NodeShape))
    g.add((shell_rule, SH.targetClass, vocab.Organization))

    sparql_constraint_3 = vocab.ShellSPARQLConstraint
    g.add((shell_rule, SH.sparql, sparql_constraint_3))
    g.add((sparql_constraint_3, RDF.type, SH.SPARQLConstraint))

    shell_sparql = """
        PREFIX vocab: <https://panamapapers.org/vocab/>
        SELECT $this
        WHERE {
            FILTER NOT EXISTS { 
                ?assignment vocab:inOrganization $this ;
                           vocab:role ?role .
            }
            $this vocab:jurisdiction ?jurisdiction .
            FILTER(
                CONTAINS(LCASE(?jurisdiction), 'virgin') || 
                CONTAINS(LCASE(?jurisdiction), 'panama') ||
                CONTAINS(LCASE(?jurisdiction), 'cayman')
            )
        }
    """
    g.add((sparql_constraint_3, SH.select, Literal(shell_sparql)))
    g.add((sparql_constraint_3, SH.message, Literal("[SUSPICIOUS] Dormant shell company")))
    g.add((shell_rule, vocab.riskScore, Literal(85)))

    # Rule 4: Nominee Service Concentration
    intermediary_rule = vocab.IntermediaryConcentrationRule
    g.add((intermediary_rule, RDF.type, SH.NodeShape))
    g.add((intermediary_rule, SH.targetClass, vocab.Person))

    sparql_constraint_4 = vocab.IntermediarySPARQLConstraint
    g.add((intermediary_rule, SH.sparql, sparql_constraint_4))
    g.add((sparql_constraint_4, RDF.type, SH.SPARQLConstraint))

    concentration_sparql = """
        PREFIX vocab: <https://panamapapers.org/vocab/>
        SELECT $this
        WHERE {
            {
                SELECT $this (COUNT(DISTINCT ?org) as ?count)
                WHERE {
                    ?assignment vocab:agent $this ;
                               vocab:inOrganization ?org .
                }
                GROUP BY $this
                HAVING(COUNT(DISTINCT ?org) > 50)
            }
        }
    """
    g.add((sparql_constraint_4, SH.select, Literal(concentration_sparql)))
    g.add((sparql_constraint_4, SH.message, Literal("[RISK] Nominee service (serves 50+ entities)")))
    g.add((intermediary_rule, vocab.riskScore, Literal(60)))

    print("   [SUCCESS] Advanced SHACL rules added:")
    print("      - Tax haven beneficial owner compliance")
    print("      - Circular ownership detection")
    print("      - Shell company identification")
    print("      - Nominee service concentration")


def add_ontology(g):
    """Add OWL ontology"""
    g.add((vocab.Agent, RDF.type, OWL.Class))
    g.add((vocab.Person, RDFS.subClassOf, vocab.Agent))
    g.add((vocab.Organization, RDFS.subClassOf, vocab.Agent))
    g.add((vocab.Role, RDF.type, OWL.Class))
    g.add((vocab.DirectorRole, RDFS.subClassOf, vocab.Role))
    g.add((vocab.ShareholderRole, RDFS.subClassOf, vocab.Role))
    g.add((vocab.BeneficialOwnerRole, RDFS.subClassOf, vocab.Role))
    g.add((vocab.OfficerRole, RDFS.subClassOf, vocab.Role))
    g.add((vocab.SecretaryRole, RDFS.subClassOf, vocab.Role))
    g.add((vocab.RoleAssignment, RDF.type, OWL.Class))
    g.add((vocab.agent, RDF.type, OWL.ObjectProperty))
    g.add((vocab.role, RDF.type, OWL.ObjectProperty))
    g.add((vocab.inOrganization, RDF.type, OWL.ObjectProperty))
    g.add((vocab.owns, RDF.type, OWL.ObjectProperty))
    g.add((vocab.owns, RDF.type, OWL.TransitiveProperty))
    g.add((vocab.hasRegisteredAddress, RDF.type, OWL.ObjectProperty))
    g.add((vocab.intermediaryFor, RDF.type, OWL.ObjectProperty))
    g.add((vocab.connected_to, RDF.type, OWL.ObjectProperty))
    g.add((vocab.similarTo, RDF.type, OWL.ObjectProperty))
    g.add((vocab.jurisdiction, RDF.type, OWL.DatatypeProperty))
    g.add((vocab.riskScore, RDF.type, OWL.DatatypeProperty))

    # Provenance properties
    g.add((vocab.ProvenanceStatement, RDF.type, OWL.Class))
    g.add((vocab.sourceRow, RDF.type, OWL.DatatypeProperty))
    g.add((vocab.about, RDF.type, OWL.ObjectProperty))


def calculate_risk_scores(validation_results, sample_graph):
    """Calculate risk scores from validation results"""
    print("\n   [INFO] Calculating risk scores...")

    entity_risks = {}

    for result in validation_results.subjects(RDF.type, SH.ValidationResult):
        focus_node = validation_results.value(result, SH.focusNode)
        source_shape = validation_results.value(result, SH.sourceShape)

        if focus_node and source_shape:
            risk_score = sample_graph.value(source_shape, vocab.riskScore)
            risk_score = int(risk_score) if risk_score else 10

            entity_uri = str(focus_node)
            if entity_uri not in entity_risks:
                entity_risks[entity_uri] = {'score': 0, 'violations': []}

            entity_risks[entity_uri]['score'] += risk_score
            message = validation_results.value(result, SH.resultMessage)
            compliance_rule = sample_graph.value(source_shape, vocab.complianceRule)

            entity_risks[entity_uri]['violations'].append({
                'message': str(message),
                'risk': risk_score,
                'compliance_rule': str(compliance_rule) if compliance_rule else 'N/A',
                'shape': str(source_shape).split('/')[-1]
            })

    ranked = sorted(entity_risks.items(), key=lambda x: x[1]['score'], reverse=True)

    print(f"\n   Top 10 highest-risk entities:")
    for i, (uri, data) in enumerate(ranked[:10], 1):
        entity_name = sample_graph.value(uri, FOAF.name)
        entity_name = entity_name if entity_name else uri.split('/')[-1][:30]
        print(f"   {i}. {entity_name}")
        print(f"      Total risk: {data['score']}/100")
        print(f"      Violations: {len(data['violations'])}")

    return entity_risks


def export_compliance_report(entity_risks, sample_graph):
    """Export compliance report"""
    print(f"\n   [INFO] Exporting compliance report...")

    report = {
        'metadata': {
            'report_type': 'AML/KYC Compliance Risk Assessment',
            'generated': datetime.now().isoformat(),
            'sample_based': True,
            'total_entities_assessed': len(entity_risks),
            'high_risk_count': sum(1 for e in entity_risks.values() if e['score'] >= 80),
            'medium_risk_count': sum(1 for e in entity_risks.values() if 50 <= e['score'] < 80),
            'low_risk_count': sum(1 for e in entity_risks.values() if e['score'] < 50),
        },
        'high_risk_entities': []
    }

    for uri, data in sorted(entity_risks.items(), key=lambda x: x[1]['score'], reverse=True)[:100]:
        entity_name = str(sample_graph.value(uri, FOAF.name) or uri.split('/')[-1])
        jurisdiction = str(sample_graph.value(uri, vocab.jurisdiction) or 'UNKNOWN')

        report['high_risk_entities'].append({
            'entity_uri': uri,
            'entity_name': entity_name,
            'jurisdiction': jurisdiction,
            'risk_score': data['score'],
            'violations': data['violations']
        })

    with open(COMPLIANCE_REPORT_FILE, 'w') as f:
        json.dump(report, f, indent=2)

    print(f"   [SUCCESS] Report exported with {len(report['high_risk_entities'])} entities")


def validate_sample_with_advanced_shacl(output_path, sample_percent=1):
    """Load sample and run advanced SHACL validation"""
    print("\n" + "=" * 70)
    print(f"ADVANCED SHACL VALIDATION (Loading {sample_percent}% sample)")
    print("=" * 70)

    try:
        from pyshacl import validate

        sample_graph = Graph()
        sample_graph.bind("vocab", vocab)
        sample_graph.bind("foaf", FOAF)
        sample_graph.bind("sh", SH)
        sample_graph.bind("prov", prov)

        print("   Adding ontology and SHACL shapes...")
        add_ontology(sample_graph)
        add_advanced_shacl(sample_graph)

        print(f"   Sampling {sample_percent}% of triples...")
        opener = gzip.open if output_path.endswith('.gz') else open

        with opener(output_path, 'rt') as f:
            total_lines = sum(1 for _ in f)

        sample_size = max(1000, int(total_lines * sample_percent / 100))
        sampled_lines = []

        with opener(output_path, 'rt') as f:
            step = max(1, total_lines // sample_size)
            for i, line in enumerate(f):
                if i % step == 0:
                    sampled_lines.append(line)
                if len(sampled_lines) >= sample_size:
                    break

        print(f"   Parsing {len(sampled_lines):,} sampled triples...")
        sample_graph.parse(data=''.join(sampled_lines), format='nt')

        print(f"   Running SHACL validation...")
        conforms, results_graph, results_text = validate(
            sample_graph,
            shacl_graph=sample_graph,
            inference='none',
            abort_on_first=False,
            advanced=True
        )

        if conforms:
            print(f"\n[SUCCESS] VALIDATION PASSED")
        else:
            violation_count = len(list(results_graph.subjects(RDF.type, SH.ValidationResult)))
            print(f"\n[WARNING] Found {violation_count} violations")

            entity_risks = calculate_risk_scores(results_graph, sample_graph)
            export_compliance_report(entity_risks, sample_graph)

        return conforms

    except ImportError:
        print("[WARNING] pyshacl not installed")
        return None


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 70)
    print("Panama Papers - Complete Analysis Pipeline")
    print("Features: Provenance, Motif Detection, Forensic Queries")
    print(f"CPU Cores: {NUM_WORKERS} | Compression: {'gzip' if USE_GZIP else 'none'}")
    print("=" * 70)

    csv_counts = {}

    count, files = load_entities_to_disk(ENTITIES_FILE)
    csv_counts['entities'] = count

    count, files = load_officers_to_disk(OFFICERS_FILE)
    csv_counts['officers'] = count

    count, files = load_addresses_to_disk(ADDRESSES_FILE)
    csv_counts['addresses'] = count

    count, files = load_relationships_to_disk(RELATIONSHIPS_FILE)
    csv_counts['relationships'] = count

    # Concatenate
    total_triples = concatenate_files(OUTPUT_FILE)

    # SHACL validation
    validate_sample_with_advanced_shacl(OUTPUT_FILE, sample_percent=VALIDATE_SAMPLE_PERCENT)

    # Network motif detection
    detect_money_laundering_motifs(OUTPUT_FILE, sample_size=10000)

    # Export forensic queries
    export_forensic_queries()

    print("\n" + "=" * 70)
    print("[SUCCESS] PIPELINE COMPLETE")
    print("=" * 70)
    print(f"\nGenerated files:")
    print(f"  1. {OUTPUT_FILE} - RDF data with provenance")
    print(f"  2. {COMPLIANCE_REPORT_FILE} - Compliance risk assessment")
    print(f"  3. {MOTIF_REPORT_FILE} - Money laundering motif analysis")
    print(f"  4. {FORENSIC_QUERIES_FILE} - Investigation query library")
    print(f"\nTotal triples: {total_triples:,}")
    print(f"\nCSV counts:")
    print(f"  Entities: {csv_counts['entities']:,}")
    print(f"  Officers: {csv_counts['officers']:,}")
    print(f"  Addresses: {csv_counts['addresses']:,}")
    print(f"  Relationships: {csv_counts['relationships']:,}")
    print(f"\n[INFO] Load into triplestore for querying:")
    print(f"  Apache Jena: tdb2.tdbloader --loc DB {OUTPUT_FILE}")
    print(f"  Blazegraph: dataLoader.sh {OUTPUT_FILE}")


if __name__ == "__main__":
    main()