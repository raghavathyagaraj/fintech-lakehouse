"""
Synthetic Data Generator for Fintech Lakehouse
===============================================

This module generates realistic synthetic data for testing.

Why synthetic data?
- No privacy concerns (not real customer data)
- Can generate any volume needed
- Can include edge cases and fraud patterns
- Reproducible (same seed = same data)

Usage:
    from src.data_generator import DataGenerator

    gen = DataGenerator(seed=42)
    customers_df = gen.generate_customers(n=10000)
    merchants_df = gen.generate_merchants(n=500)
    transactions_df = gen.generate_transactions(n=100000)
"""

import random
import uuid
import csv
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class GeneratorConfig:
    """Configuration for data generation."""

    seed: int = 42
    start_date: datetime = None
    end_date: datetime = None
    fraud_rate: float = 0.025  # 2.5% fraud rate

    def __post_init__(self):
        if self.start_date is None:
            self.start_date = datetime.now() - timedelta(days=90)
        if self.end_date is None:
            self.end_date = datetime.now()


class DataGenerator:
    """
    Generate synthetic fintech data.

    This class creates realistic-looking data for:
    - Customers
    - Merchants
    - Transactions
    - Exchange rates
    """

    # ========== REFERENCE DATA ==========

    FIRST_NAMES = [
        "James",
        "Mary",
        "John",
        "Patricia",
        "Robert",
        "Jennifer",
        "Michael",
        "Linda",
        "William",
        "Elizabeth",
        "David",
        "Barbara",
        "Richard",
        "Susan",
        "Joseph",
        "Jessica",
        "Thomas",
        "Sarah",
        "Charles",
        "Karen",
        "Emma",
        "Olivia",
        "Ava",
        "Isabella",
        "Sophia",
        "Mia",
        "Charlotte",
        "Amelia",
        "Harper",
        "Evelyn",
        "Liam",
        "Noah",
        "Oliver",
        "Elijah",
        "Lucas",
        "Mason",
        "Logan",
        "Alexander",
        "Ethan",
        "Jacob",
        "Aiden",
        "Daniel",
        "Wei",
        "Fang",
        "Ming",
        "Hiroshi",
        "Yuki",
        "Raj",
        "Priya",
        "Ahmed",
        "Fatima",
        "Carlos",
        "Maria",
        "Pedro",
        "Ana",
        "Giovanni",
        "Sofia",
    ]

    LAST_NAMES = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
        "Hernandez",
        "Lopez",
        "Gonzalez",
        "Wilson",
        "Anderson",
        "Thomas",
        "Taylor",
        "Moore",
        "Jackson",
        "Martin",
        "Lee",
        "Perez",
        "Thompson",
        "White",
        "Harris",
        "Sanchez",
        "Clark",
        "Chen",
        "Wang",
        "Li",
        "Zhang",
        "Liu",
        "Yang",
        "Huang",
        "Wu",
        "Kim",
        "Park",
        "Choi",
        "Patel",
        "Singh",
        "Kumar",
        "Shah",
        "Gupta",
        "Mueller",
        "Schmidt",
        "Schneider",
        "Fischer",
        "Weber",
        "Meyer",
    ]

    COUNTRIES = [
        ("US", "United States", ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
        ("UK", "United Kingdom", ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"]),
        ("CA", "Canada", ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"]),
        ("DE", "Germany", ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"]),
        ("FR", "France", ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"]),
        ("JP", "Japan", ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"]),
        ("AU", "Australia", ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]),
        ("SG", "Singapore", ["Singapore"]),
        ("IN", "India", ["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"]),
        ("BR", "Brazil", ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza"]),
    ]

    CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "BRL"]

    # Merchant Category Codes (MCC)
    MCC_CATEGORIES = [
        ("5411", "Grocery Stores", "LOW"),
        ("5812", "Restaurants", "LOW"),
        ("5912", "Drug Stores", "LOW"),
        ("5541", "Gas Stations", "LOW"),
        ("5311", "Department Stores", "LOW"),
        ("5732", "Electronics Stores", "MEDIUM"),
        ("5945", "Hobby & Toy Stores", "LOW"),
        ("7011", "Hotels & Lodging", "MEDIUM"),
        ("4511", "Airlines", "MEDIUM"),
        ("7512", "Car Rentals", "MEDIUM"),
        ("5999", "Miscellaneous Retail", "MEDIUM"),
        ("7995", "Gambling", "HIGH"),
        ("5962", "Direct Marketing", "HIGH"),
        ("6051", "Crypto Exchanges", "HIGH"),
        ("4829", "Wire Transfers", "HIGH"),
    ]

    PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "DIGITAL_WALLET", "BANK_TRANSFER", "CRYPTO"]
    CHANNELS = ["WEB", "MOBILE_APP", "POS", "ATM"]
    TRANSACTION_TYPES = ["PURCHASE", "REFUND", "TRANSFER", "WITHDRAWAL"]
    STATUSES = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
    KYC_STATUSES = ["VERIFIED", "PENDING", "REJECTED", "EXPIRED"]
    SEGMENTS = ["HIGH_VALUE", "REGULAR", "OCCASIONAL", "NEW", "CHURNING"]

    FRAUD_INDICATORS = [
        "VELOCITY_SPIKE",
        "UNUSUAL_AMOUNT",
        "NEW_DEVICE",
        "GEOGRAPHIC_ANOMALY",
        "AFTER_HOURS",
        "MULTIPLE_CARDS",
        "HIGH_RISK_MERCHANT",
        "SUSPICIOUS_IP",
    ]

    def __init__(self, config: Optional[GeneratorConfig] = None):
        """Initialize the generator with optional config."""
        self.config = config or GeneratorConfig()
        random.seed(self.config.seed)

        # Pre-generate customer and merchant IDs for relationships
        self._customer_ids: List[str] = []
        self._merchant_ids: List[str] = []
        self._merchant_countries: Dict[str, str] = {}

    def _generate_id(self, prefix: str) -> str:
        """Generate a unique ID with prefix."""
        return f"{prefix}_{uuid.uuid4().hex[:12].upper()}"

    def _generate_email(self, first_name: str, last_name: str) -> str:
        """Generate a realistic email address."""
        domains = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "hotmail.com"]
        separator = random.choice([".", "_", ""])
        number = random.randint(1, 999) if random.random() > 0.5 else ""
        return (
            f"{first_name.lower()}{separator}{last_name.lower()}{number}@{random.choice(domains)}"
        )

    def _generate_phone(self, country_code: str) -> str:
        """Generate a phone number for a country."""
        country_prefixes = {
            "US": "+1",
            "UK": "+44",
            "CA": "+1",
            "DE": "+49",
            "FR": "+33",
            "JP": "+81",
            "AU": "+61",
            "SG": "+65",
            "IN": "+91",
            "BR": "+55",
        }
        prefix = country_prefixes.get(country_code, "+1")
        number = "".join([str(random.randint(0, 9)) for _ in range(10)])
        return f"{prefix}{number}"

    def _random_date(self, start: datetime, end: datetime) -> datetime:
        """Generate a random datetime between start and end."""
        delta = end - start
        random_seconds = random.randint(0, int(delta.total_seconds()))
        return start + timedelta(seconds=random_seconds)

    # ========== CUSTOMER GENERATION ==========

    def generate_customers(self, n: int = 10000) -> List[Dict[str, Any]]:
        """
        Generate n customer records.

        Args:
            n: Number of customers to generate

        Returns:
            List of customer dictionaries
        """
        print(f"Generating {n:,} customers...")
        customers = []

        for i in range(n):
            customer_id = self._generate_id("CUST")
            self._customer_ids.append(customer_id)

            first_name = random.choice(self.FIRST_NAMES)
            last_name = random.choice(self.LAST_NAMES)
            country_code, country_name, cities = random.choice(self.COUNTRIES)

            # Generate dates
            created_at = self._random_date(
                self.config.start_date - timedelta(days=365 * 2),  # Up to 2 years ago
                self.config.end_date - timedelta(days=30),  # At least 30 days ago
            )

            # KYC verification (most are verified)
            kyc_status = random.choices(
                self.KYC_STATUSES, weights=[0.85, 0.08, 0.04, 0.03]  # 85% verified
            )[0]

            kyc_verified_date = None
            if kyc_status == "VERIFIED":
                kyc_verified_date = created_at + timedelta(days=random.randint(1, 14))

            # Risk score (0-100, most are low risk)
            risk_score = int(random.betavariate(2, 8) * 100)  # Skewed toward lower values

            # Segment based on some logic
            segment = random.choices(self.SEGMENTS, weights=[0.10, 0.50, 0.25, 0.10, 0.05])[0]

            # PEP (Politically Exposed Person) - rare
            is_pep = random.random() < 0.005  # 0.5%

            customer = {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": self._generate_email(first_name, last_name),
                "phone": self._generate_phone(country_code),
                "country": country_code,
                "city": random.choice(cities),
                "postal_code": "".join([str(random.randint(0, 9)) for _ in range(5)]),
                "date_of_birth": (
                    datetime.now() - timedelta(days=random.randint(18 * 365, 80 * 365))
                ).strftime("%Y-%m-%d"),
                "segment": segment,
                "kyc_status": kyc_status,
                "kyc_verified_date": kyc_verified_date.strftime("%Y-%m-%d")
                if kyc_verified_date
                else None,
                "risk_score": risk_score,
                "is_pep": is_pep,
                "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            customers.append(customer)

            if (i + 1) % 2500 == 0:
                print(f"  Generated {i + 1:,} customers...")

        print(f"✅ Generated {len(customers):,} customers")
        return customers

    # ========== MERCHANT GENERATION ==========

    def generate_merchants(self, n: int = 500) -> List[Dict[str, Any]]:
        """
        Generate n merchant records.

        Args:
            n: Number of merchants to generate

        Returns:
            List of merchant dictionaries
        """
        print(f"Generating {n:,} merchants...")
        merchants = []

        business_suffixes = ["Inc", "LLC", "Corp", "Ltd", "Co", "Group", "Holdings"]

        for i in range(n):
            merchant_id = self._generate_id("MERCH")
            self._merchant_ids.append(merchant_id)

            mcc_code, mcc_category, risk_tier = random.choice(self.MCC_CATEGORIES)
            country_code, country_name, cities = random.choice(self.COUNTRIES)

            self._merchant_countries[merchant_id] = country_code

            # Generate business name
            name_patterns = [
                f"{random.choice(self.LAST_NAMES)}'s {mcc_category}",
                f"The {mcc_category.split()[0]} Shop",
                f"{random.choice(cities)} {mcc_category}",
                f"{mcc_category} {random.choice(business_suffixes)}",
                f"Quick {mcc_category.split()[0]}",
                f"Prime {mcc_category}",
            ]
            merchant_name = random.choice(name_patterns)

            # Onboarding date
            onboarding_date = self._random_date(
                self.config.start_date - timedelta(days=365 * 3),
                self.config.end_date - timedelta(days=60),
            )

            # Fee rate based on risk tier
            base_fee = {"LOW": 0.015, "MEDIUM": 0.025, "HIGH": 0.035}[risk_tier]
            fee_rate = base_fee + random.uniform(-0.005, 0.005)

            # Monthly volume (varies widely)
            monthly_volume = random.uniform(10000, 5000000)
            avg_ticket = monthly_volume / random.randint(100, 10000)

            merchant = {
                "merchant_id": merchant_id,
                "merchant_name": merchant_name,
                "mcc_code": mcc_code,
                "mcc_category": mcc_category,
                "country": country_code,
                "currency": "USD" if country_code == "US" else random.choice(self.CURRENCIES[:5]),
                "risk_tier": risk_tier,
                "avg_ticket_size": round(avg_ticket, 2),
                "monthly_volume": round(monthly_volume, 2),
                "onboarding_date": onboarding_date.strftime("%Y-%m-%d"),
                "status": random.choices(
                    ["ACTIVE", "SUSPENDED", "TERMINATED"], weights=[0.95, 0.03, 0.02]
                )[0],
                "fee_rate": round(fee_rate, 4),
                "created_at": onboarding_date.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            merchants.append(merchant)

        print(f"✅ Generated {len(merchants):,} merchants")
        return merchants

    # ========== TRANSACTION GENERATION ==========

    def generate_transactions(self, n: int = 100000) -> List[Dict[str, Any]]:
        """
        Generate n transaction records.

        Args:
            n: Number of transactions to generate

        Returns:
            List of transaction dictionaries
        """
        if not self._customer_ids:
            raise ValueError("Generate customers first!")
        if not self._merchant_ids:
            raise ValueError("Generate merchants first!")

        print(f"Generating {n:,} transactions...")
        transactions = []

        for i in range(n):
            transaction_id = self._generate_id("TXN")
            customer_id = random.choice(self._customer_ids)
            merchant_id = random.choice(self._merchant_ids)

            # Transaction timestamp
            txn_timestamp = self._random_date(self.config.start_date, self.config.end_date)

            # Amount (log-normal distribution - most small, some large)
            amount = round(random.lognormvariate(3.5, 1.5), 2)
            amount = min(amount, 50000)  # Cap at 50k
            amount = max(amount, 0.01)  # Min 1 cent

            # Currency and conversion
            currency = random.choices(
                self.CURRENCIES,
                weights=[0.60, 0.15, 0.10, 0.05, 0.03, 0.02, 0.02, 0.01, 0.01, 0.01],
            )[0]

            # Simple exchange rate simulation
            exchange_rates = {
                "USD": 1.0,
                "EUR": 1.08,
                "GBP": 1.27,
                "JPY": 0.0067,
                "CAD": 0.74,
                "AUD": 0.65,
                "CHF": 1.13,
                "CNY": 0.14,
                "INR": 0.012,
                "BRL": 0.20,
            }
            amount_usd = round(amount * exchange_rates.get(currency, 1.0), 2)

            # Fee calculation
            fee_amount = round(amount_usd * random.uniform(0.01, 0.03), 2)
            net_amount = round(amount_usd - fee_amount, 2)

            # Transaction type
            txn_type = random.choices(self.TRANSACTION_TYPES, weights=[0.85, 0.05, 0.07, 0.03])[0]

            # Payment method
            payment_method = random.choices(
                self.PAYMENT_METHODS, weights=[0.40, 0.30, 0.20, 0.08, 0.02]
            )[0]

            # Channel
            channel = random.choices(self.CHANNELS, weights=[0.35, 0.40, 0.20, 0.05])[0]

            # Status
            status = random.choices(self.STATUSES, weights=[0.92, 0.04, 0.03, 0.01])[0]

            # Cross-border detection
            merchant_country = self._merchant_countries.get(merchant_id, "US")
            customer_country = random.choice([c[0] for c in self.COUNTRIES])
            is_cross_border = merchant_country != customer_country

            # Fraud flagging
            is_flagged = False
            fraud_indicators = []

            # Determine if this should be flagged (based on fraud rate)
            if random.random() < self.config.fraud_rate:
                is_flagged = True
                # Add 1-3 fraud indicators
                num_indicators = random.randint(1, 3)
                fraud_indicators = random.sample(self.FRAUD_INDICATORS, num_indicators)

            # Additional fraud signals
            if amount_usd > 9000 and amount_usd < 10000:  # Structuring pattern
                if random.random() < 0.3:
                    is_flagged = True
                    if "UNUSUAL_AMOUNT" not in fraud_indicators:
                        fraud_indicators.append("UNUSUAL_AMOUNT")

            if txn_timestamp.hour < 6 or txn_timestamp.hour > 23:  # Late night
                if random.random() < 0.1:
                    is_flagged = True
                    if "AFTER_HOURS" not in fraud_indicators:
                        fraud_indicators.append("AFTER_HOURS")

            transaction = {
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "merchant_id": merchant_id,
                "amount": amount,
                "currency": currency,
                "amount_usd": amount_usd,
                "fee_amount": fee_amount,
                "net_amount": net_amount,
                "transaction_type": txn_type,
                "payment_method": payment_method,
                "channel": channel,
                "status": status,
                "merchant_country": merchant_country,
                "customer_country": customer_country,
                "is_cross_border": is_cross_border,
                "is_flagged": is_flagged,
                "fraud_indicators": "|".join(fraud_indicators) if fraud_indicators else None,
                "ip_address": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
                "device_id": f"DEV_{uuid.uuid4().hex[:8].upper()}",
                "transaction_timestamp": txn_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at": txn_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            }
            transactions.append(transaction)

            if (i + 1) % 25000 == 0:
                print(f"  Generated {i + 1:,} transactions...")

        # Print summary
        flagged_count = sum(1 for t in transactions if t["is_flagged"])
        print(
            f"✅ Generated {len(transactions):,} transactions ({flagged_count:,} flagged for review)"
        )
        return transactions

    # ========== EXCHANGE RATE GENERATION ==========

    def generate_exchange_rates(self, days: int = 90) -> List[Dict[str, Any]]:
        """
        Generate exchange rate data.

        Args:
            days: Number of days of history

        Returns:
            List of exchange rate dictionaries
        """
        print(f"Generating {days} days of exchange rates...")
        rates = []

        # Base rates (to USD)
        base_rates = {
            "EUR": 1.08,
            "GBP": 1.27,
            "JPY": 0.0067,
            "CAD": 0.74,
            "AUD": 0.65,
            "CHF": 1.13,
            "CNY": 0.14,
            "INR": 0.012,
            "BRL": 0.20,
        }

        for day_offset in range(days):
            rate_date = (self.config.end_date - timedelta(days=day_offset)).strftime("%Y-%m-%d")

            for currency, base_rate in base_rates.items():
                # Add some daily fluctuation (±2%)
                fluctuation = random.uniform(-0.02, 0.02)
                daily_rate = round(base_rate * (1 + fluctuation), 6)

                rate_record = {
                    "rate_date": rate_date,
                    "source_currency": currency,
                    "target_currency": "USD",
                    "exchange_rate": daily_rate,
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                rates.append(rate_record)

        print(f"✅ Generated {len(rates):,} exchange rate records")
        return rates

    # ========== FILE EXPORT ==========

    def save_to_csv(self, data: List[Dict], filepath: str) -> None:
        """Save data to CSV file."""
        if not data:
            print(f"Warning: No data to save to {filepath}")
            return

        # Create directory if needed
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        print(f"💾 Saved {len(data):,} records to {filepath}")

    def generate_all(self, output_dir: str = "data/raw") -> Dict[str, str]:
        """
        Generate all datasets and save to CSV files.

        Args:
            output_dir: Directory to save CSV files

        Returns:
            Dictionary of dataset names to file paths
        """
        print("\n" + "=" * 60)
        print("GENERATING SYNTHETIC FINTECH DATA")
        print("=" * 60 + "\n")

        # Generate data
        customers = self.generate_customers(n=10000)
        merchants = self.generate_merchants(n=500)
        transactions = self.generate_transactions(n=100000)
        exchange_rates = self.generate_exchange_rates(days=90)

        # Save to CSV
        paths = {
            "customers": f"{output_dir}/customers.csv",
            "merchants": f"{output_dir}/merchants.csv",
            "transactions": f"{output_dir}/transactions.csv",
            "exchange_rates": f"{output_dir}/exchange_rates.csv",
        }

        self.save_to_csv(customers, paths["customers"])
        self.save_to_csv(merchants, paths["merchants"])
        self.save_to_csv(transactions, paths["transactions"])
        self.save_to_csv(exchange_rates, paths["exchange_rates"])

        print("\n" + "=" * 60)
        print("DATA GENERATION COMPLETE!")
        print("=" * 60)
        print(f"\nFiles saved to: {output_dir}/")
        print(f"  • customers.csv     ({len(customers):,} records)")
        print(f"  • merchants.csv     ({len(merchants):,} records)")
        print(f"  • transactions.csv  ({len(transactions):,} records)")
        print(f"  • exchange_rates.csv ({len(exchange_rates):,} records)")

        return paths
