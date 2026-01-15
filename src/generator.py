import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class CustomerGenerator:
    def __init__(self, num_customers=50):
        # Deterministic Seed for Customers!
        # This ensures that if we run the script multiple times (for different days),
        # the Customer IDs remain constant, preventing "Orphan Orders" for past data.
        Faker.seed(42) 
        self.fake = Faker()
        self.num_customers = num_customers

    def generate(self):
        customers = []
        for _ in range(self.num_customers):
            customers.append({
                'customer_id': fake.uuid4(),
                'name': fake.name(),
                'email': fake.email(),
                'signup_date': fake.date_between(start_date='-2y', end_date='today'),
                'region': fake.country()
            })
        return pd.DataFrame(customers)

class ProductGenerator:
    def __init__(self, num_products=20):
        # Deterministic Seed for Products too!
        Faker.seed(42)
        random.seed(42)  # <--- CRITICAL: Fixes random.uniform(price) fluctuations!
        self.fake = Faker()
        self.num_products = num_products

    def generate(self):
        categories = ['Electronics', 'Books', 'Home', 'Clothing', 'Sports']
        products = []
        for _ in range(self.num_products):
            products.append({
                'product_id': fake.uuid4(),
                'category': random.choice(categories),
                'price': round(random.uniform(10.0, 500.0), 2),
                'stock_level': random.randint(0, 100),
                'product_name': fake.word().capitalize()
            })
        return pd.DataFrame(products)

class OrderGenerator:
    def __init__(self, customers_df, products_df, num_orders=200):
        self.customers_df = customers_df
        self.products_df = products_df
        self.num_orders = num_orders

    def generate(self, date=None):
        if date is None:
            date = datetime.now().date()
        
        orders = []
        customer_ids = self.customers_df['customer_id'].tolist()
        product_ids = self.products_df['product_id'].tolist()
        
        for _ in range(self.num_orders):
            product_id = random.choice(product_ids)
            # Find price for the chosen product to calculate total
            price = self.products_df.loc[self.products_df['product_id'] == product_id, 'price'].values[0]
            quantity = random.randint(1, 5)
            
            orders.append({
                'order_id': fake.uuid4(),
                'customer_id': random.choice(customer_ids),
                'product_id': product_id,
                'quantity': quantity,
                'total_amount': round(price * quantity, 2),
                'order_date': date,
                'status': random.choice(['COMPLETED', 'PENDING', 'SHIPPED', 'CANCELLED'])
            })
        return pd.DataFrame(orders)
