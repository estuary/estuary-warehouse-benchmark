"""Generate TPC-H partsupp data in CSV format.

This module generates synthetic part supplier data following the TPC-H benchmark
specification, saving the data in batches to CSV files.
"""

import argparse
import logging
import random
from typing import List, Dict, Any

from faker import Faker

from config import (
    PARTSUPP_TOTAL_RECORDS,
    DEFAULT_RECORDS_PER_BATCH,
    DEFAULT_OUTPUT_DIR,
    SUPPLY_COST_RANGE,
    AVAILABILITY_QTY_RANGE
)
from utils import save_batch_to_csv, calculate_batches, ensure_output_directory, setup_logging


def generate_partsupp_batch(fake: Faker, batch_size: int) -> List[Dict[str, Any]]:
    """Generate a batch of part supplier data.
    
    Args:
        fake: Faker instance for generating synthetic data
        batch_size: Number of part supplier records to generate
        
    Returns:
        List of dictionaries containing part supplier data
    """
    data = []
    for _ in range(batch_size):
        partkey = fake.unique.random_number(digits=8)
        suppkey = fake.unique.random_number(digits=8)
        availqty = random.randint(*AVAILABILITY_QTY_RANGE)
        supplycost = round(random.uniform(*SUPPLY_COST_RANGE), 2)
        comment = fake.text()

        data.append({
            'partkey': partkey,
            'suppkey': suppkey,
            'availqty': availqty,
            'supplycost': supplycost,
            'comment': comment
        })
    return data


def generate_partsupp_data(
    total_records: int = PARTSUPP_TOTAL_RECORDS,
    records_per_batch: int = DEFAULT_RECORDS_PER_BATCH,
    output_dir: str = DEFAULT_OUTPUT_DIR
) -> None:
    """Generate part supplier data and save to CSV files.
    
    Args:
        total_records: Total number of part supplier records to generate
        records_per_batch: Number of records per batch
        output_dir: Directory to save output files
    """
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting partsupp data generation: {total_records} records in batches of {records_per_batch}")
    
    ensure_output_directory(output_dir)
    fake = Faker()
    num_batches = calculate_batches(total_records, records_per_batch)
    
    for i in range(num_batches):
        logger.info(f"Generating partsupp batch {i + 1}/{num_batches}")
        batch_data = generate_partsupp_batch(fake, records_per_batch)
        filename = f'partsupp_batch_{i + 1}.csv'
        save_batch_to_csv(batch_data, filename, output_dir)
    
    logger.info(f"Partsupp data generation completed. Generated {num_batches} batches.")


def main() -> None:
    """Main entry point for the part supplier data generator."""
    parser = argparse.ArgumentParser(description='Generate TPC-H part supplier data')
    parser.add_argument(
        '--total-records',
        type=int,
        default=PARTSUPP_TOTAL_RECORDS,
        help=f'Total number of records to generate (default: {PARTSUPP_TOTAL_RECORDS})'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=DEFAULT_RECORDS_PER_BATCH,
        help=f'Number of records per batch (default: {DEFAULT_RECORDS_PER_BATCH})'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory for CSV files (default: {DEFAULT_OUTPUT_DIR})'
    )
    
    args = parser.parse_args()
    
    generate_partsupp_data(
        total_records=args.total_records,
        records_per_batch=args.batch_size,
        output_dir=args.output_dir
    )


if __name__ == '__main__':
    main()
