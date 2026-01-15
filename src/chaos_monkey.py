import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

class ChaosMonkey:
    def __init__(self, error_rate=0.05):
        """
        Args:
            error_rate (float): Probability of a record being corrupted (0.0 to 1.0)
        """
        self.error_rate = error_rate

    def apply_chaos(self, orders_df):
        """
        Applies various data quality issues to the orders dataframe.
        """
        df = orders_df.copy()
        n_rows = len(df)
        if n_rows == 0:
            return df
        
        # Randomize "Chaos Intensity" per execution
        # Max errors per category ~ error_rate
        # But we want variability: some days have 0 errors, others have many.
        max_errors_per_cat = int(n_rows * self.error_rate)
        
        print(f"  [ChaosMonkey 2.0] Randomized Intensity (Max {max_errors_per_cat} per cat)...")

        def get_random_count():
            return random.randint(0, max_errors_per_cat)

        # 1. Referential Integrity: Invalid customer_id (Ghost Customers)
        n = get_random_count()
        if n > 0:
            idxs_ref = np.random.choice(df.index, size=n, replace=False)
            df.loc[idxs_ref, 'customer_id'] = [fake.uuid4() for _ in range(n)]

        # 2. Completeness: NULL customer_id
        n_null = get_random_count()
        if n_null > 0:
            idxs_null = np.random.choice(df.index, size=n_null, replace=False)
            df.loc[idxs_null, 'customer_id'] = None

        # 3. Domain Error: Negative total_amount
        n_neg = get_random_count()
        if n_neg > 0:
            idxs_negative = np.random.choice(df.index, size=n_neg, replace=False)
            df.loc[idxs_negative, 'total_amount'] = df.loc[idxs_negative, 'total_amount'] * -1

        # 4. Calculation Integrity: Mismatch between Price*Qty and Total
        n_math = get_random_count()
        if n_math > 0:
            idxs_math = np.random.choice(df.index, size=n_math, replace=False)
            df.loc[idxs_math, 'total_amount'] = df.loc[idxs_math, 'total_amount'] * 1.5

        # 5. Temporal Validity: Future Dates (Time Travelers)
        n_future = get_random_count()
        if n_future > 0:
            idxs_future = np.random.choice(df.index, size=n_future, replace=False)
            future_date = (datetime.now() + timedelta(days=10000)).strftime('%Y-%m-%d')
            df.loc[idxs_future, 'order_date'] = future_date

        # 6. Consistency/String: Status Typos (The "Dirty" Data)
        n_typo = get_random_count()
        if n_typo > 0:
            idxs_typo = np.random.choice(df.index, size=n_typo, replace=False)
            typos = ['Completado', 'completed', 'Shipped!', 'Pending...', 'Cancelled (User)']
            df.loc[idxs_typo, 'status'] = [random.choice(typos) for _ in range(n_typo)]

        # 7. Uniqueness: Duplicate Orders
        # Duplicates are special, let's keep them rare but random (0 to 5)
        n_dups = random.randint(0, 5)
        if n_dups > 0:
            duplicate_rows = df.sample(n=min(n_dups, n_rows))
            df = pd.concat([df, duplicate_rows], ignore_index=True)

        print(f"    -> Injected: {n} FK, {n_null} Nulls, {n_neg} Neg, {n_math} Math, {n_future} Future, {n_typo} Typos, {n_dups} Dups.")
        
        return df
