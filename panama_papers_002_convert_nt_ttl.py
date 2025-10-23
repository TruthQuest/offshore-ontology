#!/usr/bin/env python3
"""
Fast NT to TTL Converter
Handles large files efficiently
"""

import os
import sys
from rdflib import Graph
from tqdm import tqdm


def convert_nt_to_ttl(nt_file, ttl_file=None):
    """
    Convert N-Triples to Turtle format
    Fast and simple for huge files
    """

    # Auto-generate output filename if not provided
    if ttl_file is None:
        ttl_file = nt_file.replace('.nt', '.ttl')

    print("=" * 60)
    print("NT to TTL Converter")
    print("=" * 60)
    print(f"\nInput:  {nt_file}")
    print(f"Output: {ttl_file}")

    # Check file exists
    if not os.path.exists(nt_file):
        print(f"\n❌ ERROR: File not found: {nt_file}")
        sys.exit(1)

    # Get file size
    file_size_mb = os.path.getsize(nt_file) / (1024 * 1024)
    print(f"Size:   {file_size_mb:.1f} MB")

    # Count lines for progress bar
    print("\n📊 Counting triples...")
    with open(nt_file, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)

    print(f"   Found {total_lines:,} triples")

    # Load graph with progress
    print("\n📥 Loading N-Triples...")
    graph = Graph()

    # Parse with progress bar
    with open(nt_file, 'r', encoding='utf-8') as f:
        with tqdm(total=total_lines, desc="Loading", unit=" triples") as pbar:
            batch = []
            batch_size = 10000

            for line in f:
                batch.append(line)

                if len(batch) >= batch_size:
                    # Parse batch
                    batch_data = ''.join(batch)
                    graph.parse(data=batch_data, format='ntriples')
                    pbar.update(len(batch))
                    batch = []

            # Parse remaining
            if batch:
                batch_data = ''.join(batch)
                graph.parse(data=batch_data, format='ntriples')
                pbar.update(len(batch))

    print(f"   ✅ Loaded {len(graph):,} triples")

    # Serialize to Turtle
    print("\n💾 Converting to Turtle format...")
    print("   (This may take a while for large graphs...)")

    graph.serialize(destination=ttl_file, format='turtle')

    # Get output size
    output_size_mb = os.path.getsize(ttl_file) / (1024 * 1024)

    print(f"   ✅ Saved to {ttl_file}")
    print(f"   Output size: {output_size_mb:.1f} MB")

    print("\n" + "=" * 60)
    print("✅ CONVERSION COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    # Get filename from command line or use default
    if len(sys.argv) > 1:
        nt_file = sys.argv[1]
        ttl_file = sys.argv[2] if len(sys.argv) > 2 else None
    else:
        # Default to panama papers file
        nt_file = os.path.expanduser("~/PycharmProjects/EcuadorNarcos/panama_papers.nt")
        ttl_file = None

    convert_nt_to_ttl(nt_file, ttl_file)