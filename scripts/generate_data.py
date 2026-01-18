#!/usr/bin/env python3
"""
Script to generate synthetic fintech data.

Usage:
    python scripts/generate_data.py
    python scripts/generate_data.py --output-dir data/raw --seed 42
"""

import argparse
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_generator import DataGenerator
from src.data_generator.generator import GeneratorConfig


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic fintech data")
    parser.add_argument(
        "--output-dir",
        default="data/raw",
        help="Output directory for CSV files (default: data/raw)",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)"
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=10000,
        help="Number of customers to generate (default: 10000)",
    )
    parser.add_argument(
        "--merchants", type=int, default=500, help="Number of merchants to generate (default: 500)"
    )
    parser.add_argument(
        "--transactions",
        type=int,
        default=100000,
        help="Number of transactions to generate (default: 100000)",
    )

    args = parser.parse_args()

    # Create generator
    config = GeneratorConfig(seed=args.seed)
    generator = DataGenerator(config=config)

    # Generate all data
    print(f"\n🚀 Starting data generation with seed={args.seed}")
    print(f"   Output directory: {args.output_dir}\n")

    paths = generator.generate_all(output_dir=args.output_dir)

    print(f"\n✅ Done! Data files are ready in {args.output_dir}/")
    return paths


if __name__ == "__main__":
    main()
