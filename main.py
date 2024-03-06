import faker
import psycopg2
from datetime import datetime
import random

fake = faker.Faker()

def generate_transaction():
    user = fake.simple_profile()

    return {
        "transactionId" : fake.uuid4(),
        "userId" : user['username'],
        "timestamp" : datetime.utcnow().timestamp(),
        "amount" : round(random.uniform(10, 1000),2),
        "currency" : random.choice(["USD","AED","EGP"]),
        "city" : fake.city(),
        "country" : fake.country(),
        "brand" : fake.company(),
        "paymentMethod":random.choice(["credit_card","PayPal","online_transfer"]),
        "ipAddress" : fake.ipv4(),
        "voucherCode" :  random.choice(["","DISCOUNT20",""]),
        "affilitaeId" : fake.uuid4()
    }

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions(
            transactionId VARCHAR(255) PRIMARY KEY,
            userId VARCHAR(255),
            timestamp TIMESTAMP,
            amount DECIMAL,
            currency VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            brand VARCHAR(255),
            paymentMethod VARCHAR(255),
            ipAddress VARCHAR(255),
            voucherCode VARCHAR(255),
            affilitaeId VARCHAR(255)
        );
        """
    )
    cursor.close()
    conn.commit()

if __name__ == "__main__":
    conn = psycopg2.connect(
        host='localhost',
        database='financial_db',
        user='postgres',
        password='postgres',
        port=5432
    )

    create_table(conn)

    transaction = generate_transaction()
    cursor = conn.cursor()
    print(transaction)

    cursor.execute(
        """
        INSERT INTO transactions(transactionId, userId, timestamp, amount, currency, city, country, brand, paymentMethod, 
        ipAddress, affilitaeId, voucherCode)
        VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (transaction["transactionId"], transaction["userId"], datetime.fromtimestamp(transaction["timestamp"]).strftime('%Y-%m-%d %H:%M:%S'),
              transaction["amount"], transaction["currency"], transaction["city"], transaction["country"],
              transaction["brand"], transaction["paymentMethod"], transaction["ipAddress"],
              transaction["affilitaeId"], transaction["voucherCode"])
    )
    cursor.close()
    conn.commit()